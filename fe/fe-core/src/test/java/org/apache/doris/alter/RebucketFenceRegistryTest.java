// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.alter;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.common.LoadException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class RebucketFenceRegistryTest {

    private final OlapTable table = Mockito.mock(OlapTable.class);
    private final Partition partition = Mockito.mock(Partition.class);

    @Test
    public void testFenceRefusesOnlyTransactionsThatBeganAfterIt() throws Exception {
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(partition.getName()).thenReturn("p");
        Mockito.when(partition.getId()).thenReturn(10L);
        RebucketFenceRegistry registry = new RebucketFenceRegistry();

        registry.checkCommit(table, partition, 100L); // no fence: nothing to refuse
        registry.fence(table, partition, 7L, 100L);
        Assertions.assertTrue(registry.isFenced(10L));
        Mockito.verify(partition).setState(PartitionState.REBUCKET_FENCED);

        // began before or at the watershed: the in-flight set the job drains
        registry.checkCommit(table, partition, 99L);
        registry.checkCommit(table, partition, 100L);
        // began after the fence: refused, retriable, marked
        LoadException e = Assertions.assertThrows(LoadException.class,
                () -> registry.checkCommit(table, partition, 101L));
        Assertions.assertTrue(e.getMessage().contains(RebucketFenceRegistry.FENCED_MARKER), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("job 7"), e.getMessage());

        Mockito.when(partition.getState()).thenReturn(PartitionState.REBUCKET_FENCED);
        registry.unfence(table, partition);
        Assertions.assertFalse(registry.isFenced(10L));
        Mockito.verify(partition).setState(PartitionState.NORMAL);
        registry.checkCommit(table, partition, 101L);
    }

    @Test
    public void testUnfenceLeavesAForeignStateAlone() {
        Mockito.when(partition.getId()).thenReturn(11L);
        Mockito.when(partition.getState()).thenReturn(PartitionState.RESTORE);
        RebucketFenceRegistry registry = new RebucketFenceRegistry();
        registry.unfence(table, partition);
        Mockito.verify(partition, Mockito.never()).setState(Mockito.any());
        Assertions.assertEquals(0, registry.size());
    }
}
