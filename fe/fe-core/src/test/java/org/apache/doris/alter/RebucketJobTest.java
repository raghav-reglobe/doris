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

import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RebucketJobTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("rb");
        createTable("CREATE TABLE rb.t (k BIGINT NOT NULL, v VARCHAR(32), s BIGINT NOT NULL) UNIQUE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES (\"replication_num\" = \"1\","
                + " \"function_column.sequence_col\" = \"s\")");
    }

    /** The fill is a load; the FE test cluster has no data path, so the job under test skips it. */
    private static class FillLessJob extends RebucketJob {
        FillLessJob(long dbId, OlapTable table, int buckets) {
            super("test", Env.getCurrentEnv().getNextId(), dbId, table.getId(), table.getName(), 3600_000L,
                    Lists.newArrayList(table.getPartition(table.getName()).getId()), buckets, null);
        }

        @Override
        protected void fill(Database db, OlapTable table, Partition p) {
        }
    }

    @Test
    public void testRebucketsAnUnpartitionedTableThroughATemporaryPartition() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("rb");
        OlapTable table = (OlapTable) db.getTableOrDdlException("t");
        Partition p = table.getPartition("t");
        Assertions.assertEquals(4, p.getDistributionInfo().getBucketNum());
        RebucketFenceRegistry registry = Env.getCurrentEnv().getRebucketFenceRegistry();

        FillLessJob job = new FillLessJob(db.getId(), table, 2);
        Env.getCurrentEnv().getSchemaChangeHandler().addAlterJobV2(job);

        // PENDING: the temporary partition exists at the new count
        job.run();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState(), job.errMsg);
        Partition temp = table.getPartition(job.getTempPartitionName(), true);
        Assertions.assertNotNull(temp);
        Assertions.assertEquals(2, temp.getDistributionInfo().getBucketNum());
        Assertions.assertFalse(registry.isFenced(p.getId()));

        // WAITING_TXN: fence up, nothing in flight, fill done
        job.run();
        Assertions.assertEquals(JobState.RUNNING, job.getJobState(), job.errMsg);
        Assertions.assertTrue(registry.isFenced(p.getId()));
        Assertions.assertEquals(PartitionState.REBUCKET_FENCED, p.getState());

        // RUNNING: swapped, fence down, done
        job.run();
        Assertions.assertEquals(JobState.FINISHED, job.getJobState(), job.errMsg);
        Partition after = table.getPartition("t");
        Assertions.assertEquals(2, after.getDistributionInfo().getBucketNum());
        Assertions.assertNotEquals(p.getId(), after.getId());
        Assertions.assertFalse(registry.isFenced(p.getId()));
        Assertions.assertFalse(registry.isFenced(after.getId()));
        Assertions.assertEquals(PartitionState.NORMAL, after.getState());
        Assertions.assertNull(job.getTempPartitionName());
        String result = job.getResults().values().iterator().next();
        Assertions.assertTrue(result.contains("4 -> 2"), job.getResults().toString());
    }

    @Test
    public void testCancelWhileFencedDropsTheTemporaryPartitionAndLowersTheFence() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("rb");
        OlapTable table = (OlapTable) db.getTableOrDdlException("t");
        Partition p = table.getPartition("t");
        int before = p.getDistributionInfo().getBucketNum();
        RebucketFenceRegistry registry = Env.getCurrentEnv().getRebucketFenceRegistry();

        FillLessJob job = new FillLessJob(db.getId(), table, before + 1);
        Env.getCurrentEnv().getSchemaChangeHandler().addAlterJobV2(job);
        job.run();
        String temp = job.getTempPartitionName();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState(), job.errMsg);
        Assertions.assertNotNull(table.getPartition(temp, true));

        Assertions.assertTrue(job.cancel("test cancel"));
        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        Assertions.assertNull(table.getPartition(temp, true));
        Assertions.assertFalse(registry.isFenced(p.getId()));
        Assertions.assertEquals(before, table.getPartition("t").getDistributionInfo().getBucketNum());
    }

    @Test
    public void testOneJobPerTableAndTheCancelStatementReachesIt() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("rb");
        OlapTable table = (OlapTable) db.getTableOrDdlException("t");
        Partition p = table.getPartition("t");
        int before = p.getDistributionInfo().getBucketNum();
        RebucketFenceRegistry registry = Env.getCurrentEnv().getRebucketFenceRegistry();
        FillLessJob job = new FillLessJob(db.getId(), table, before + 2);
        Env.getCurrentEnv().getSchemaChangeHandler().addAlterJobV2(job);
        job.run();
        Assertions.assertEquals(JobState.WAITING_TXN, job.getJobState(), job.errMsg);
        Assertions.assertSame(job, RebucketJob.unfinishedJobOf(table.getId()));
        // the table stays NORMAL, yet a second job and any other ALTER are refused while this one runs
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Exception second = Assertions.assertThrows(Exception.class,
                () -> RebucketJob.submit(db, table, Lists.newArrayList("t"), 3, null, "test"));
        Assertions.assertTrue(second.getMessage().contains("unfinished REBUCKET job"), second.getMessage());
        Exception other = Assertions.assertThrows(Exception.class,
                () -> alterTableSync("ALTER TABLE rb.t ADD COLUMN c INT"));
        Assertions.assertTrue(other.getMessage().contains("unfinished REBUCKET job"), other.getMessage());
        // the statement path cancels it although the table is not under SCHEMA_CHANGE
        String temp = job.getTempPartitionName();
        executeNereidsSql("CANCEL ALTER TABLE COLUMN FROM rb.t");
        Assertions.assertEquals(JobState.CANCELLED, job.getJobState());
        Assertions.assertNull(table.getPartition(temp, true));
        Assertions.assertFalse(registry.isFenced(p.getId()));
        Assertions.assertNull(RebucketJob.unfinishedJobOf(table.getId()));
        Assertions.assertEquals(before, table.getPartition("t").getDistributionInfo().getBucketNum());
    }
}
