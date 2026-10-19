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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The write fence a re-bucket job holds on a partition while it replays the delta and swaps the
 * partition. A load whose transaction began after the fence was raised is refused at commit with a
 * retriable error; a transaction that began before it may still commit, and the job waits for those
 * before it reads the delta. The fence lives in memory and is derived from the job's state: the job
 * raises and lowers it, and on replay the job state decides whether it is up.
 */
public class RebucketFenceRegistry {
    private static final Logger LOG = LogManager.getLogger(RebucketFenceRegistry.class);

    /** The marker a client can match to retry the load. */
    public static final String FENCED_MARKER = "[REBUCKET_FENCED]";

    public static class Fence {
        private final long jobId;
        private final long watershedTxnId;
        private final long fencedAtMs;

        Fence(long jobId, long watershedTxnId, long fencedAtMs) {
            this.jobId = jobId;
            this.watershedTxnId = watershedTxnId;
            this.fencedAtMs = fencedAtMs;
        }

        public long getJobId() {
            return jobId;
        }

        public long getWatershedTxnId() {
            return watershedTxnId;
        }

        public long getFencedAtMs() {
            return fencedAtMs;
        }
    }

    private final Map<Long, Fence> fences = new ConcurrentHashMap<>();

    /**
     * Raise the fence on a partition. Transactions with an id above watershedTxnId began after the
     * fence and are refused at commit; the ones at or below it are the in-flight set the job drains.
     */
    public void fence(OlapTable table, Partition partition, long jobId, long watershedTxnId) {
        fences.put(partition.getId(), new Fence(jobId, watershedTxnId, System.currentTimeMillis()));
        partition.setState(PartitionState.REBUCKET_FENCED);
        LOG.info("rebucket fence up: table {}, partition {}, job {}, watershed txn {}",
                table.getName(), partition.getName(), jobId, watershedTxnId);
    }

    public void unfence(OlapTable table, Partition partition) {
        Fence fence = fences.remove(partition.getId());
        if (partition.getState() == PartitionState.REBUCKET_FENCED) {
            partition.setState(PartitionState.NORMAL);
        }
        if (fence != null) {
            LOG.info("rebucket fence down: table {}, partition {}, job {}, held {} ms",
                    table.getName(), partition.getName(), fence.getJobId(),
                    System.currentTimeMillis() - fence.getFencedAtMs());
        }
    }

    public boolean isFenced(long partitionId) {
        return fences.containsKey(partitionId);
    }

    public Fence getFence(long partitionId) {
        return fences.get(partitionId);
    }

    public int size() {
        return fences.size();
    }

    /**
     * Called at commit for every partition a transaction wrote. Refuses the commit when the partition
     * is fenced and the transaction began after the fence was raised.
     */
    public void checkCommit(OlapTable table, Partition partition, long txnId) throws LoadException {
        Fence fence = fences.get(partition.getId());
        if (fence == null || txnId <= fence.getWatershedTxnId()) {
            return;
        }
        throw new LoadException(FENCED_MARKER + " Table [" + table.getName() + "], Partition ["
                + partition.getName() + "] is being re-bucketed (job " + fence.getJobId()
                + "). Retry the load in a moment.");
    }
}
