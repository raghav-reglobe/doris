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
import org.apache.doris.common.Config;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TabletSizeDriftCheckerTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("drift");
        createTable("CREATE TABLE drift.t_in (k BIGINT NOT NULL, v VARCHAR(32)) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES (\"replication_num\" = \"1\")");
        createTable("CREATE TABLE drift.t_out (k BIGINT NOT NULL, v VARCHAR(32)) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES (\"replication_num\" = \"1\","
                + " \"auto_rebucket\" = \"false\")");
    }

    @Test
    public void testBandsAndHysteresis() {
        int savedMin = Config.autobucket_min_buckets;
        int savedPerBucket = Config.autobucket_partition_size_per_bucket_gb;
        Config.autobucket_min_buckets = 1;
        Config.autobucket_partition_size_per_bucket_gb = 1;
        try {
            long mb = 1024L * 1024;
            Assertions.assertEquals(1, TabletSizeDriftChecker.wantBuckets(50 * mb));
            Assertions.assertEquals(2, TabletSizeDriftChecker.wantBuckets(500 * mb));
            Assertions.assertEquals(3, TabletSizeDriftChecker.wantBuckets(3 * 1024 * mb));
        } finally {
            Config.autobucket_min_buckets = savedMin;
            Config.autobucket_partition_size_per_bucket_gb = savedPerBucket;
        }
        // hysteresis 1.0: off by more than 2x either way
        Assertions.assertTrue(TabletSizeDriftChecker.drifted(10, 3, 1.0));
        Assertions.assertTrue(TabletSizeDriftChecker.drifted(1, 3, 1.0));
        Assertions.assertFalse(TabletSizeDriftChecker.drifted(4, 3, 1.0));
        Assertions.assertFalse(TabletSizeDriftChecker.drifted(2, 3, 1.0));
        Assertions.assertFalse(TabletSizeDriftChecker.drifted(0, 3, 1.0));
    }

    @Test
    public void testOptOutPropertyAtCreateAndAlter() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("drift");
        OlapTable in = (OlapTable) db.getTableOrDdlException("t_in");
        OlapTable out = (OlapTable) db.getTableOrDdlException("t_out");
        Assertions.assertTrue(in.isAutoRebucket());
        Assertions.assertFalse(out.isAutoRebucket());
        alterTableSync("ALTER TABLE drift.t_out SET (\"auto_rebucket\" = \"true\")");
        Assertions.assertTrue(out.isAutoRebucket());
        alterTableSync("ALTER TABLE drift.t_in SET (\"auto_rebucket\" = \"false\")");
        Assertions.assertFalse(in.isAutoRebucket());
    }

    @Test
    public void testWalkSchedulesNothingWithoutSizes() throws Exception {
        boolean saved = Config.enable_auto_rebucket;
        Config.enable_auto_rebucket = true;
        try {
            TabletSizeDriftChecker checker = Env.getCurrentEnv().getTabletSizeDriftChecker();
            checker.walk();
            Assertions.assertTrue(checker.getLastCandidates().isEmpty(), checker.getLastCandidates().toString());
            long jobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values().stream()
                    .filter(j -> j instanceof RebucketJob).count();
            Assertions.assertEquals(0, jobs);
        } finally {
            Config.enable_auto_rebucket = saved;
        }
    }
}
