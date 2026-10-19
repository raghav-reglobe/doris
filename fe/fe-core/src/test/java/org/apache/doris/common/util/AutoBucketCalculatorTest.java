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

package org.apache.doris.common.util;

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketContext;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketResult;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class AutoBucketCalculatorTest {

    OlapTable table = Mockito.mock(OlapTable.class);

    @Test
    public void testCalculateAutoBucketsNotAutoBucket() {
        Mockito.when(table.isAutoBucket()).thenReturn(false);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 10);
        AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(10, result.getBuckets());
        Assertions.assertEquals("not auto bucket table", result.getErrorMessage());
    }

    @Test
    public void testCalculateAutoBucketsExecuteFirstTime() {
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", true, 10);
        AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(10, result.getBuckets());
        Assertions.assertEquals("executeFirstTime", result.getErrorMessage());
    }

    @Test
    public void testCalculateAutoBucketsWithBoundsCheck() {
        Mockito.when(table.isAutoBucket()).thenReturn(false);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 10);
        int buckets = AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context);

        Assertions.assertEquals(10, buckets);
    }

    @Test
    public void testAutoBucketContextConstructor() {
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        // 5-param constructor
        AutoBucketContext context = new AutoBucketContext(
                table, "p3", "p4", true, 5);
        Assertions.assertEquals("p3", context.getPartitionName());
        Assertions.assertEquals("p4", context.getNowPartitionName());
        Assertions.assertTrue(context.isExecuteFirstTime());
        Assertions.assertEquals(5, context.getDefaultBuckets());
    }

    @Test
    public void testCalculateForExistingPartitionsSizesAnUnpartitionedTableFromItsData() throws Exception {
        int savedPerBucket = Config.autobucket_partition_size_per_bucket_gb;
        int savedMin = Config.autobucket_min_buckets;
        Config.autobucket_partition_size_per_bucket_gb = 1;
        Config.autobucket_min_buckets = 1;
        // one alive backend with one online 1 TB disk: the non-cloud formula caps the count by backend disks
        DiskInfo disk = Mockito.mock(DiskInfo.class);
        Mockito.when(disk.getState()).thenReturn(DiskState.ONLINE);
        Mockito.when(disk.hasPathHash()).thenReturn(true);
        Mockito.when(disk.getAvailableCapacityB()).thenReturn(1024L * 1024 * 1024 * 1024);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isAlive()).thenReturn(true);
        Mockito.when(backend.isLoadAvailable()).thenReturn(true);
        Mockito.when(backend.getDisks()).thenReturn(ImmutableMap.of("/data", disk));
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.doReturn(ImmutableMap.of(1L, backend)).when(systemInfoService).getAllBackendsByAllCluster();
        DistributionInfo distributionInfo = Mockito.mock(DistributionInfo.class);
        Mockito.when(distributionInfo.getBucketNum()).thenReturn(4);
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getName()).thenReturn("test_table");
        Mockito.when(partition.getVisibleVersion()).thenReturn(5L);
        long threeGb = 3L * 1024 * 1024 * 1024;
        Mockito.when(partition.getDataSizeExcludeEmptyReplica(true)).thenReturn(threeGb);
        Mockito.when(partition.getDataSize(true)).thenReturn(threeGb);
        Mockito.when(partition.getDistributionInfo()).thenReturn(distributionInfo);
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);
        Mockito.when(table.getPartitionInfo()).thenReturn(new SinglePartitionInfo());
        Mockito.doReturn(Collections.singletonList(partition)).when(table).getPartitions();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            // 3 GB stored, 1 GB per bucket: 3 buckets, whatever the partition being replaced had (4)
            Assertions.assertEquals(3, AutoBucketCalculator.calculateForExistingPartitions(table, "tmp"));
            // a partition without data gives nothing to size from: the caller keeps its default
            Mockito.when(partition.getVisibleVersion()).thenReturn(1L);
            Assertions.assertEquals(-1, AutoBucketCalculator.calculateForExistingPartitions(table, "tmp"));
        } finally {
            Config.autobucket_partition_size_per_bucket_gb = savedPerBucket;
            Config.autobucket_min_buckets = savedMin;
        }
    }
}
