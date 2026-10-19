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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.common.util.MasterDaemon;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Finds partitions whose tablet count has drifted outside the auto-bucket size bands and schedules a
 * re-bucket job (BUCKETS AUTO) for them. Off unless enable_auto_rebucket is set. A table opts out with
 * the property auto_rebucket = false. The newest period of a dynamic or auto-partitioned table is left
 * alone while it is still being filled; every other partition is judged on its stored size alone, with
 * hysteresis and a "size moved since the last decision" rule against flapping, a per-partition size cap
 * above which a candidate is only reported, and a cap on concurrent jobs.
 */
public class TabletSizeDriftChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletSizeDriftChecker.class);

    /** one line per partition the last walk looked at and found outside the bands */
    public static class Candidate {
        public final String dbName;
        public final String tableName;
        public final String partitionName;
        public final long storedBytes;
        public final int have;
        public final int want;
        public final String decision;

        Candidate(String dbName, String tableName, String partitionName, long storedBytes, int have, int want,
                String decision) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.storedBytes = storedBytes;
            this.have = have;
            this.want = want;
            this.decision = decision;
        }

        @Override
        public String toString() {
            return dbName + "." + tableName + "." + partitionName + " stored=" + storedBytes + " have=" + have
                    + " want=" + want + " -> " + decision;
        }
    }

    private final Map<Long, Long> lastDecisionSize = Maps.newConcurrentMap();
    private volatile List<Candidate> lastCandidates = Lists.newArrayList();

    public TabletSizeDriftChecker(String name, long intervalMs) {
        super(name, intervalMs);
    }

    /** The auto-bucket count for a stored size (the bands take the raw volume: stored x 5). */
    public static int wantBuckets(long storedBytes) {
        return AutoBucketUtils.getBucketsNum(storedBytes * 5, Config.autobucket_min_buckets);
    }

    /** Outside the bands by more than the hysteresis, either way. */
    public static boolean drifted(int have, int want, double hysteresis) {
        if (have <= 0 || want <= 0) {
            return false;
        }
        return have < want / (1 + hysteresis) || have > want * (1 + hysteresis);
    }

    public List<Candidate> getLastCandidates() {
        return lastCandidates;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_auto_rebucket) {
            return;
        }
        setInterval(Config.auto_rebucket_check_interval_s * 1000L);
        try {
            walk();
        } catch (Throwable t) {
            LOG.warn("auto rebucket walk failed", t);
        }
    }

    private int runningJobs() {
        int n = 0;
        for (AlterJobV2 job : Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values()) {
            if (job instanceof RebucketJob && !job.isDone()) {
                n++;
            }
        }
        return n;
    }

    private boolean tableHasAlterJob(long tableId) {
        for (AlterJobV2 job : Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values()) {
            if (job.getTableId() == tableId && !job.isDone()) {
                return true;
            }
        }
        return false;
    }

    /** The partition with the highest range upper bound, when the table grows by period. */
    static Partition newestPeriod(OlapTable table) {
        PartitionInfo info = table.getPartitionInfo();
        if (info.getType() != PartitionType.RANGE) {
            return null;
        }
        boolean byPeriod = info.enableAutomaticPartition()
                || (table.getTableProperty() != null
                        && table.getTableProperty().getDynamicPartitionProperty() != null
                        && table.getTableProperty().getDynamicPartitionProperty().getEnable());
        if (!byPeriod) {
            return null;
        }
        Partition newest = null;
        PartitionKey newestUpper = null;
        for (Map.Entry<Long, PartitionItem> e : info.getIdToItem(false).entrySet()) {
            PartitionKey upper = ((RangePartitionItem) e.getValue()).getItems().upperEndpoint();
            if (newestUpper == null || upper.compareTo(newestUpper) > 0) {
                newestUpper = upper;
                newest = table.getPartition(e.getKey());
            }
        }
        return newest;
    }

    void walk() {
        List<Candidate> candidates = Lists.newArrayList();
        int scheduled = 0;
        int running = runningJobs();
        long now = System.currentTimeMillis();
        for (Long dbId : Env.getCurrentInternalCatalog().getDbIds()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            for (Table t : db.getTables()) {
                if (!(t instanceof OlapTable) || t.isTemporary()) {
                    continue;
                }
                OlapTable table = (OlapTable) t;
                if (table.getState() != OlapTable.OlapTableState.NORMAL || !table.isAutoRebucket()
                        || tableHasAlterJob(table.getId())) {
                    continue;
                }
                Partition newest;
                List<Partition> partitions;
                table.readLock();
                try {
                    newest = newestPeriod(table);
                    partitions = Lists.newArrayList(table.getPartitions());
                } finally {
                    table.readUnlock();
                }
                for (Partition p : partitions) {
                    if (p.getVisibleVersion() < 2) {
                        continue;
                    }
                    long stored = p.getDataSizeExcludeEmptyReplica(true);
                    if (stored <= 0) {
                        continue;
                    }
                    int have = p.getDistributionInfo().getBucketNum();
                    int want = wantBuckets(stored);
                    if (!drifted(have, want, Config.auto_rebucket_hysteresis)) {
                        lastDecisionSize.remove(p.getId());
                        continue;
                    }
                    String decision;
                    if (newest != null && newest.getId() == p.getId()
                            && now - p.getVisibleVersionTime() < Config.auto_rebucket_min_quiet_s * 1000L) {
                        decision = "wait: newest period still filling";
                    } else if (stored > Config.auto_rebucket_max_partition_bytes) {
                        decision = "report only: above auto_rebucket_max_partition_bytes";
                    } else if (lastDecisionSize.containsKey(p.getId()) && lastDecisionSize.get(p.getId()) == stored) {
                        decision = "wait: size unchanged since the last decision";
                    } else if (running + scheduled >= Config.auto_rebucket_max_concurrent_jobs) {
                        decision = "wait: job cap";
                    } else {
                        try {
                            RebucketJob.submit(db, table, Lists.newArrayList(p.getName()), RebucketJob.BUCKETS_AUTO,
                                    null, "auto_rebucket");
                            scheduled++;
                            lastDecisionSize.put(p.getId(), stored);
                            decision = "scheduled";
                        } catch (Exception e) {
                            decision = "failed: " + e.getMessage();
                        }
                    }
                    candidates.add(new Candidate(db.getFullName(), table.getName(), p.getName(), stored, have, want,
                            decision));
                }
            }
        }
        lastCandidates = candidates;
        if (!candidates.isEmpty()) {
            LOG.info("auto rebucket: {} candidates, {} scheduled, {} running: {}", candidates.size(), scheduled,
                    running, candidates);
        }
    }
}
