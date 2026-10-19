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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** ALTER TABLE ... REBUCKET [PARTITION (...)] BUCKETS N | AUTO: rebuild existing partitions at a new bucket count. */
public class RebucketOp extends AlterTableOp {
    public static final int BUCKETS_AUTO = -1;

    private final List<String> partitionNames;
    private final int buckets;

    public RebucketOp(List<String> partitionNames, int buckets) {
        super(AlterOpType.REBUCKET);
        this.partitionNames = partitionNames;
        this.buckets = buckets;
    }

    /** null = every partition of the table */
    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public int getBuckets() {
        return buckets;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public boolean allowOpRowBinlog() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("REBUCKET");
        if (partitionNames != null && !partitionNames.isEmpty()) {
            sb.append(" PARTITION (").append(String.join(", ", partitionNames)).append(")");
        }
        sb.append(" BUCKETS ").append(buckets == BUCKETS_AUTO ? "AUTO" : String.valueOf(buckets));
        return sb.toString();
    }

    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
