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

package org.apache.doris.clone;

import org.apache.doris.common.Config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DynamicPartitionSchedulerTest {

    private double savedThreshold;

    @BeforeEach
    public void saveConfig() {
        savedThreshold = Config.autobucket_out_of_bounds_percent_threshold;
        Config.autobucket_out_of_bounds_percent_threshold = 0.5;
    }

    @AfterEach
    public void restoreConfig() {
        Config.autobucket_out_of_bounds_percent_threshold = savedThreshold;
    }

    @Test
    public void testAutoBucketCalcNumStepsDownToTheLowerBound() {
        // The previous partition got the default 10, the calculated count is 3: step down to the
        // lower bound (5) instead of repeating 10, which would pin the table at 10 forever.
        Assertions.assertEquals(5, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(3, 10, "t", "p1"));
        // From 5 the next calculated 3 is within bounds: -1 lets the caller keep its own value.
        Assertions.assertEquals(-1, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(3, 5, "t", "p2"));
        Assertions.assertEquals(-1, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(7, 10, "t", "p3"));
    }

    @Test
    public void testAutoBucketCalcNumGrowthIsNeverClamped() {
        Assertions.assertEquals(16, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(16, 10, "t", "p1"));
        // No previous partition to bound against.
        Assertions.assertEquals(10, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(10, 0, "t", "p2"));
    }

    @Test
    public void testAutoBucketCalcNumThresholdOfOneDisablesTheLowerBound() {
        Config.autobucket_out_of_bounds_percent_threshold = 1.0;
        Assertions.assertEquals(-1, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(3, 10, "t", "p1"));
    }
}
