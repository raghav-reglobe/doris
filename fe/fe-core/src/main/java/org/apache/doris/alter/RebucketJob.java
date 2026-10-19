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

import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Re-bucket existing partitions of an OLAP table in place, one partition at a time, through a
 * temporary partition: build it at the new bucket count, fill it from the partition, hold the write
 * fence while the last in-flight loads drain, swap it in with REPLACE PARTITION, lower the fence.
 * Readers never pause; a load that arrives while the fence is up is refused with a retriable error.
 *
 * Job states map onto AlterJobV2's: PENDING = build and fill the temporary partition of the current
 * partition, WAITING_TXN = fence up and drain, RUNNING = swap and fence down, then PENDING again for
 * the next partition, FINISHED when the list is done. The fence is derived from the job state on replay.
 * The delta mode is NONE: the fill runs under the fence (fence-before-fill); a version-range delta that
 * fills first and replays what changed is a later mode.
 */
public class RebucketJob extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(RebucketJob.class);

    public static final int BUCKETS_AUTO = -1;
    private static final String TEMP_PREFIX = "rbtmp_";

    @SerializedName(value = "pids")
    private List<Long> partitionIds = Lists.newArrayList();
    @SerializedName(value = "tb")
    private int targetBuckets = BUCKETS_AUTO;
    @SerializedName(value = "dm")
    private String deltaMode = "NONE";
    @SerializedName(value = "cc")
    private String cloudCluster;
    @SerializedName(value = "cur")
    private int cursor = 0;
    @SerializedName(value = "tmp")
    private String tempPartitionName;
    @SerializedName(value = "ws")
    private long fenceWatershedTxnId = -1;
    @SerializedName(value = "res")
    private Map<Long, String> results = Maps.newHashMap();

    public RebucketJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public RebucketJob(String rawSql, long jobId, long dbId, long tableId, String tableName, long timeoutMs,
            List<Long> partitionIds, int targetBuckets, String cloudCluster) {
        super(rawSql, jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        this.partitionIds = partitionIds;
        this.targetBuckets = targetBuckets;
        this.cloudCluster = cloudCluster;
    }

    /** Build, register and log a job for the named partitions (every partition when the list is empty). */
    public static RebucketJob submit(Database db, OlapTable table, List<String> partitionNames, int targetBuckets,
            String cloudCluster, String rawSql) throws UserException {
        List<Long> ids = Lists.newArrayList();
        table.readLock();
        try {
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new UserException("Table " + table.getName() + " is " + table.getState() + ", not NORMAL");
            }
            if (partitionNames == null || partitionNames.isEmpty()) {
                for (Partition p : table.getPartitions()) {
                    ids.add(p.getId());
                }
            } else {
                for (String name : partitionNames) {
                    Partition p = table.getPartition(name);
                    if (p == null) {
                        throw new UserException("Partition " + name + " does not exist in " + table.getName());
                    }
                    ids.add(p.getId());
                }
            }
            if (targetBuckets != BUCKETS_AUTO && targetBuckets < 1) {
                throw new UserException("BUCKETS must be AUTO or a positive number");
            }
        } finally {
            table.readUnlock();
        }
        long jobId = Env.getCurrentEnv().getNextId();
        RebucketJob job = new RebucketJob(rawSql, jobId, db.getId(), table.getId(), table.getName(),
                Config.alter_table_timeout_second * 1000L * 24, ids, targetBuckets, cloudCluster);
        Env.getCurrentEnv().getSchemaChangeHandler().addAlterJobV2(job);
        Env.getCurrentEnv().getEditLog().logAlterJob(job);
        LOG.info("rebucket job {} created: table {}, {} partitions, buckets {}", jobId, table.getName(),
                ids.size(), targetBuckets == BUCKETS_AUTO ? "AUTO" : String.valueOf(targetBuckets));
        return job;
    }

    // ---- state machine -------------------------------------------------------------------------

    private Partition currentPartition(OlapTable table) {
        return cursor < partitionIds.size() ? table.getPartition(partitionIds.get(cursor)) : null;
    }

    private Database db() throws AlterCancelException {
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            throw new AlterCancelException("Database " + dbId + " does not exist");
        }
        return db;
    }

    private OlapTable table(Database db) throws AlterCancelException {
        OlapTable table = (OlapTable) db.getTableNullable(tableId);
        if (table == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }
        return table;
    }

    /** PENDING: skip partitions that need nothing, add the temporary partition of the current one, fill it. */
    @Override
    protected void runPendingJob() throws Exception {
        Database db = db();
        OlapTable table = table(db);
        while (cursor < partitionIds.size()) {
            Partition p = currentPartition(table);
            if (p == null) {
                results.put(partitionIds.get(cursor), "skipped: partition gone");
                cursor++;
                continue;
            }
            if (targetBuckets != BUCKETS_AUTO && p.getDistributionInfo().getBucketNum() == targetBuckets) {
                results.put(p.getId(), "skipped: already " + targetBuckets + " buckets");
                cursor++;
                continue;
            }
            break;
        }
        if (cursor >= partitionIds.size()) {
            finish();
            return;
        }
        Partition p = currentPartition(table);
        // a valid partition name (letter first, no leading underscore), unique per job and partition
        tempPartitionName = TEMP_PREFIX + jobId + "_" + p.getId();
        addTempPartition(db, table, p);
        int tempBuckets = table.getPartition(tempPartitionName, true).getDistributionInfo().getBucketNum();
        if (targetBuckets == BUCKETS_AUTO && tempBuckets == p.getDistributionInfo().getBucketNum()) {
            // AUTO landed on the count the partition already has: nothing to do for this one
            dropTempPartition(db, table);
            results.put(p.getId(), "skipped: AUTO = current " + tempBuckets + " buckets");
            cursor++;
            Env.getCurrentEnv().getEditLog().logAlterJob(this);
            return;
        }
        // fence-before-fill: the fill runs with the fence up, so the temporary partition IS the state
        this.jobState = JobState.WAITING_TXN;
        Env.getCurrentEnv().getEditLog().logAlterJob(this);
        LOG.info("rebucket job {}: partition {} -> {} buckets, temp {}", jobId, p.getName(), tempBuckets,
                tempPartitionName);
    }

    /** WAITING_TXN: raise the fence, wait for the loads that began before it, then fill. */
    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Database db = db();
        OlapTable table = table(db);
        Partition p = currentPartition(table);
        if (p == null) {
            throw new AlterCancelException("partition " + partitionIds.get(cursor) + " vanished");
        }
        RebucketFenceRegistry registry = Env.getCurrentEnv().getRebucketFenceRegistry();
        try {
            if (fenceWatershedTxnId < 0) {
                fenceWatershedTxnId = Env.getCurrentGlobalTransactionMgr().getNextTransactionId();
                registry.fence(table, p, jobId, fenceWatershedTxnId);
                Env.getCurrentEnv().getEditLog().logAlterJob(this);
            } else if (!registry.isFenced(p.getId())) {
                registry.fence(table, p, jobId, fenceWatershedTxnId);
            }
            if (!Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(fenceWatershedTxnId, dbId,
                    Lists.newArrayList(tableId))) {
                LOG.info("rebucket job {}: waiting for loads before txn {} on {}", jobId, fenceWatershedTxnId,
                        p.getName());
                return;
            }
            fill(db, table, p);
        } catch (AlterCancelException e) {
            throw e;
        } catch (Exception e) {
            throw new AlterCancelException(e.getMessage());
        }
        this.jobState = JobState.RUNNING;
        Env.getCurrentEnv().getEditLog().logAlterJob(this);
    }

    /** RUNNING: swap the temporary partition in, lower the fence, move on. */
    @Override
    protected void runRunningJob() throws AlterCancelException {
        Database db = db();
        OlapTable table = table(db);
        Partition p = currentPartition(table);
        if (p == null) {
            throw new AlterCancelException("partition " + partitionIds.get(cursor) + " vanished");
        }
        int before = p.getDistributionInfo().getBucketNum();
        RebucketFenceRegistry registry = Env.getCurrentEnv().getRebucketFenceRegistry();
        RebucketFenceRegistry.Fence fence = registry.getFence(p.getId());
        long fencedAtMs = fence == null ? System.currentTimeMillis() : fence.getFencedAtMs();
        try {
            Map<String, String> props = Maps.newHashMap();
            props.put("strict_range", "false");
            props.put("use_temp_partition_name", "false");
            // the swap expects the caller to hold the table's write lock, as the ALTER path does
            table.writeLockOrDdlException();
            try {
                Env.getCurrentEnv().replaceTempPartition(db, table, new ReplacePartitionOp(
                        new PartitionNamesInfo(false, Lists.newArrayList(p.getName())),
                        new PartitionNamesInfo(true, Lists.newArrayList(tempPartitionName)), false, props));
            } finally {
                table.writeUnlock();
            }
        } catch (Exception e) {
            throw new AlterCancelException("swap failed: " + e);
        } finally {
            registry.unfence(table, p);
        }
        Partition after = table.getPartition(p.getName());
        results.put(partitionIds.get(cursor), before + " -> " + after.getDistributionInfo().getBucketNum());
        LOG.info("rebucket job {}: partition {} {} -> {} buckets, fence held {} ms", jobId, p.getName(), before,
                after.getDistributionInfo().getBucketNum(), System.currentTimeMillis() - fencedAtMs);
        cursor++;
        tempPartitionName = null;
        fenceWatershedTxnId = -1;
        if (cursor >= partitionIds.size()) {
            finish();
        } else {
            this.jobState = JobState.PENDING;
            Env.getCurrentEnv().getEditLog().logAlterJob(this);
        }
    }

    private void finish() {
        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logAlterJob(this);
        LOG.info("rebucket job {} finished: {}", jobId, results);
    }

    // ---- the acts ------------------------------------------------------------------------------

    private void addTempPartition(Database db, OlapTable table, Partition p) throws Exception {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        SinglePartitionDesc desc;
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            desc = new SinglePartitionDesc(false, tempPartitionName, PartitionKeyDesc.DUMMY_KEY_DESC,
                    Maps.newHashMap());
        } else {
            PartitionDesc all = partitionInfo.toPartitionDesc(table);
            SinglePartitionDesc old = all.getSinglePartitionDescByName(p.getName());
            if (old == null) {
                throw new AlterCancelException("no partition desc for " + p.getName());
            }
            desc = new SinglePartitionDesc(false, tempPartitionName, old.getPartitionKeyDesc(), old.getProperties());
        }
        DistributionInfo dist = p.getDistributionInfo();
        boolean auto = targetBuckets == BUCKETS_AUTO;
        int buckets = auto ? dist.getBucketNum() : targetBuckets;
        DistributionDescriptor distDesc;
        if (dist.getType() == DistributionInfoType.HASH) {
            List<String> cols = ((HashDistributionInfo) dist).getDistributionColumns().stream()
                    .map(Column::getName).collect(Collectors.toList());
            distDesc = new DistributionDescriptor(true, auto, buckets, cols);
        } else {
            distDesc = new DistributionDescriptor(false, auto, buckets, null);
        }
        Env.getCurrentEnv().addPartition(db, table.getName(),
                new AddPartitionOp(desc, distDesc, desc.getProperties(), true), false, 0, true, null);
    }

    private void dropTempPartition(Database db, OlapTable table) {
        if (tempPartitionName == null) {
            return;
        }
        try {
            table.writeLockOrDdlException();
            try {
                Env.getCurrentEnv().dropPartition(db, table,
                        new DropPartitionOp(true, tempPartitionName, true, true));
            } finally {
                table.writeUnlock();
            }
        } catch (Exception e) {
            LOG.warn("rebucket job {}: dropping temp partition {} failed: {}", jobId, tempPartitionName,
                    e.toString());
        }
        tempPartitionName = null;
    }

    /** The fill: an internal INSERT of the partition into its temporary partition. Overridable for tests. */
    protected void fill(Database db, OlapTable table, Partition p) throws Exception {
        String fqn = "`" + db.getFullName() + "`.`" + table.getName() + "`";
        String sql = "INSERT INTO " + fqn + " TEMPORARY PARTITION (`" + tempPartitionName + "`) SELECT * FROM "
                + fqn + " PARTITION (`" + p.getName() + "`)";
        ConnectContext ctx = MTMVPlanUtil.createBasicMvContext(null, ImmutableList.of(), ImmutableMap.of());
        if (cloudCluster != null && !cloudCluster.isEmpty()) {
            ctx.setCloudCluster(cloudCluster);
        }
        ctx.setDatabase(db.getFullName());
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        if (!(plan instanceof Command)) {
            throw new UserException("not a command: " + sql);
        }
        StatementContext stmtCtx = new StatementContext(ctx, new OriginStatement(sql, 0));
        LOG.info("rebucket job {}: fill {}", jobId, sql);
        MTMVPlanUtil.executeCommand(ctx, (Command) plan, stmtCtx, sql, null);
    }

    // ---- cancel / replay / show ----------------------------------------------------------------

    @Override
    protected synchronized boolean cancelImpl(String errMsg) {
        if (jobState.isFinalState()) {
            return false;
        }
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        OlapTable table = db == null ? null : (OlapTable) db.getTableNullable(tableId);
        if (table != null) {
            Partition p = currentPartition(table);
            if (p != null) {
                Env.getCurrentEnv().getRebucketFenceRegistry().unfence(table, p);
            }
            dropTempPartition(db, table);
        }
        this.errMsg = errMsg;
        this.jobState = JobState.CANCELLED;
        this.finishedTimeMs = System.currentTimeMillis();
        Env.getCurrentEnv().getEditLog().logAlterJob(this);
        LOG.info("rebucket job {} cancelled: {}", jobId, errMsg);
        return true;
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        RebucketJob other = (RebucketJob) replayedJob;
        this.jobState = other.jobState;
        this.errMsg = other.errMsg;
        this.finishedTimeMs = other.finishedTimeMs;
        this.cursor = other.cursor;
        this.tempPartitionName = other.tempPartitionName;
        this.fenceWatershedTxnId = other.fenceWatershedTxnId;
        this.results = other.results;
        // the fence is a function of the job state
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        OlapTable table = db == null ? null : (OlapTable) db.getTableNullable(tableId);
        if (table == null) {
            return;
        }
        Partition p = currentPartition(table);
        if (p == null) {
            return;
        }
        RebucketFenceRegistry registry = Env.getCurrentEnv().getRebucketFenceRegistry();
        boolean fenced = fenceWatershedTxnId >= 0
                && (jobState == JobState.WAITING_TXN || jobState == JobState.RUNNING);
        if (fenced) {
            registry.fence(table, p, jobId, fenceWatershedTxnId);
        } else {
            registry.unfence(table, p);
        }
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        List<Comparable> info = Lists.newArrayList();
        info.add(jobId);
        info.add(tableName);
        info.add(TimeUtils.longToTimeStringWithms(createTimeMs));
        info.add(TimeUtils.longToTimeStringWithms(finishedTimeMs));
        info.add("REBUCKET " + (targetBuckets == BUCKETS_AUTO ? "AUTO" : String.valueOf(targetBuckets)));
        info.add(tempPartitionName == null ? "" : tempPartitionName);
        info.add(cursor < partitionIds.size() ? partitionIds.get(cursor) : -1L);
        info.add(deltaMode);
        info.add(fenceWatershedTxnId);
        info.add(jobState.name());
        info.add(errMsg);
        info.add(cursor + "/" + partitionIds.size() + " " + results);
        info.add(timeoutMs / 1000);
        infos.add(info);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public int getTargetBuckets() {
        return targetBuckets;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public int getCursor() {
        return cursor;
    }

    public String getTempPartitionName() {
        return tempPartitionName;
    }

    public Map<Long, String> getResults() {
        return results;
    }
}
