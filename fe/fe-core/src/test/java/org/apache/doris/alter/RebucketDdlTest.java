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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class RebucketDdlTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("rbd");
        createTable("CREATE TABLE rbd.t (k BIGINT NOT NULL, v VARCHAR(32)) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES (\"replication_num\" = \"1\")");
    }

    private List<RebucketJob> jobsOf(long tableId) {
        return Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values().stream()
                .filter(j -> j instanceof RebucketJob && j.getTableId() == tableId)
                .map(j -> (RebucketJob) j).collect(Collectors.toList());
    }

    @Test
    public void testRebucketStatementCreatesAJob() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("rbd");
        OlapTable table = (OlapTable) db.getTableOrDdlException("t");
        long partitionId = table.getPartition("t").getId();

        alterTableSync("ALTER TABLE rbd.t REBUCKET BUCKETS 3");
        List<RebucketJob> jobs = jobsOf(table.getId());
        Assertions.assertEquals(1, jobs.size());
        Assertions.assertEquals(3, jobs.get(0).getTargetBuckets());
        Assertions.assertEquals(List.of(partitionId), jobs.get(0).getPartitionIds());

        alterTableSync("ALTER TABLE rbd.t REBUCKET PARTITION (t) BUCKETS AUTO");
        jobs = jobsOf(table.getId());
        Assertions.assertEquals(2, jobs.size());
        Assertions.assertTrue(jobs.stream().anyMatch(j -> j.getTargetBuckets() == RebucketJob.BUCKETS_AUTO));
    }

    @Test
    public void testRebucketStatementNeedsABucketCount() {
        Assertions.assertThrows(ParseException.class,
                () -> new NereidsParser().parseSingle("ALTER TABLE rbd.t REBUCKET"));
        Assertions.assertThrows(ParseException.class,
                () -> new NereidsParser().parseSingle("ALTER TABLE rbd.t REBUCKET TEMPORARY PARTITION (p) BUCKETS 2"));
    }
}
