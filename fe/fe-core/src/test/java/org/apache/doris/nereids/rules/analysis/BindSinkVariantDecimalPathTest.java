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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseToVariant;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TryParseToVariant;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

class BindSinkVariantDecimalPathTest {

    private static Column templatedVariantColumn(String name) {
        ArrayList<VariantField> fields = new ArrayList<>();
        fields.add(new VariantField("money.amount", ScalarType.createDecimalV3Type(20, 6), "",
                PatternType.MATCH_NAME));
        fields.add(new VariantField("name", ScalarType.createStringType(), ""));
        return new Column(name, new org.apache.doris.catalog.VariantType(fields));
    }

    private static Map<String, NamedExpression> outputs(NamedExpression... exprs) {
        Map<String, NamedExpression> map = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (NamedExpression expr : exprs) {
            map.put(expr.getName(), expr);
        }
        return map;
    }

    @Test
    void parseToVariantIsTypedWithTheTemplatedColumnType() {
        Column column = templatedVariantColumn("v");
        SlotReference text = SlotReference.of("t", StringType.INSTANCE);
        Alias parse = new Alias(new ParseToVariant(text), "v");
        Alias tryParse = new Alias(new TryParseToVariant(text), "w");
        Column other = templatedVariantColumn("w");

        Map<String, NamedExpression> result = BindSink.retargetVariantParseOutputs(
                ImmutableList.of(column, other), outputs(parse, tryParse));

        DataType target = DataType.fromCatalogType(column.getType());
        Assertions.assertInstanceOf(VariantType.class, target);
        Assertions.assertEquals(1, ((VariantType) target).getPredefinedFields().size());

        Alias typedParse = Assertions.assertInstanceOf(Alias.class, result.get("v"));
        Assertions.assertEquals(parse.getExprId(), typedParse.getExprId());
        ParseToVariant typedCall = Assertions.assertInstanceOf(ParseToVariant.class, typedParse.child());
        Assertions.assertEquals(target, typedCall.getDataType());
        Assertions.assertEquals(target, typedCall.getReturnType());
        Assertions.assertSame(text, typedCall.child());

        Alias typedTryParse = Assertions.assertInstanceOf(Alias.class, result.get("w"));
        TryParseToVariant typedTryCall = Assertions.assertInstanceOf(TryParseToVariant.class,
                typedTryParse.child());
        Assertions.assertEquals(target, typedTryCall.getDataType());

        // A typed output needs no further cast at the sink.
        Slot slot = typedParse.toSlot();
        Assertions.assertSame(slot, BindSink.coerceSinkExpression(slot, target));
    }

    @Test
    void outputsWithoutATemplateOrWithoutAParseCallAreLeftAlone() {
        SlotReference text = SlotReference.of("t", StringType.INSTANCE);
        Alias parse = new Alias(new ParseToVariant(text), "plain");
        Column plainColumn = new Column("plain", new org.apache.doris.catalog.VariantType());

        Alias cast = new Alias(new Cast(text, VariantType.INSTANCE), "casted");
        Column castedColumn = templatedVariantColumn("casted");

        Alias string = new Alias(text, "s");
        Column stringColumn = new Column("s", Type.STRING);

        Map<String, NamedExpression> input = outputs(parse, cast, string);
        Map<String, NamedExpression> result = BindSink.retargetVariantParseOutputs(
                ImmutableList.of(plainColumn, castedColumn, stringColumn), input);
        Assertions.assertSame(input, result);
        Assertions.assertSame(parse, result.get("plain"));
        Assertions.assertSame(cast, result.get("casted"));
        Assertions.assertSame(string, result.get("s"));
    }
}
