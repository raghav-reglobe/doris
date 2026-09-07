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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.View;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

/**
 * Regression for CREATE ROW POLICY column resolution on a VIEW whose body reads an EXTERNAL
 * catalog. Table's constructor keys the nameToColumn map by {@link Column#getDefineName()};
 * after FE metadata replay such a view carries columns whose defineName diverges from their
 * name, so {@code getColumn(name)} (the old guard) misses a real column while DESCRIBE, query
 * planning, and the row-policy runtime all resolve it. CreatePolicyCommand#columnExistsInSchema
 * resolves against getFullSchema() instead, so it agrees with those working paths.
 */
public class CreatePolicyColumnResolutionTest {

    private Column columnWithDefineName(String name, String defineName) throws Exception {
        Column c = new Column(name, PrimitiveType.INT);
        // No public setter: the divergent defineName is a replay/deserialize artifact, so set it
        // directly to reproduce the reloaded cross-catalog-view state.
        Field f = Column.class.getDeclaredField("defineName");
        f.setAccessible(true);
        f.set(c, defineName);
        return c;
    }

    @Test
    public void testResolvesFullSchemaNotDefineNameKeyedMap() throws Exception {
        // A reloaded cross-catalog view: the column's defineName is the qualified source name,
        // so the nameToColumn map is keyed by it and getColumn("zone") misses.
        Column zone = columnWithDefineName("zone", "silver_qc.csh_qc.qc_devices.zone");
        View view = new View(1L, "gold_serving_view", Lists.newArrayList(zone));

        // Reproduce the bug: the map-based lookup the old guard used returns null...
        Assertions.assertNull(view.getColumn("zone"));
        // ...while the fix resolves against the full-schema list (case-insensitive) and finds it.
        Assertions.assertTrue(CreatePolicyCommand.columnExistsInSchema(view, "zone"));
        Assertions.assertTrue(CreatePolicyCommand.columnExistsInSchema(view, "ZONE"));
        // ...and still refuses a genuinely absent column (no silent acceptance).
        Assertions.assertFalse(CreatePolicyCommand.columnExistsInSchema(view, "no_such_column"));
    }

    @Test
    public void testNormallyKeyedColumnResolvesBothWays() {
        // A normal column (defineName == name): both the old map lookup and the fix find it.
        Column id = new Column("id", PrimitiveType.INT);
        View view = new View(2L, "plain_view", Lists.newArrayList(id));
        Assertions.assertNotNull(view.getColumn("id"));
        Assertions.assertTrue(CreatePolicyCommand.columnExistsInSchema(view, "id"));
        Assertions.assertFalse(CreatePolicyCommand.columnExistsInSchema(view, "missing"));
    }
}
