/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.command.BackgroundQueryCommand;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.util.Version;

public class BackgroundIndexFilterWireTest {
    private static final DataSet DATA_SET = DataSet.of("test", "bg_index_filter_wire");

    @Test
    void namedFilterSendsIndexFieldsAndResidualFilterExp() {
        Filter filter = Filter.rangeByIndex("age_idx", 30, 65);

        CommandBuffer cb = encodeBackgroundCommand(filter, true);
        List<Integer> types = fieldTypes(cb);

        assertEquals("age_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
        assertTrue(types.contains(FieldType.FILTER_EXP));
        assertFalse(types.contains(FieldType.WHERE));
        assertFalse(types.contains(FieldType.INDEX_EXPRESSION));
    }

    @Test
    void filterAfterWhereStillSendsIndexFieldsAndResidualFilterExp() {
        Filter filter = Filter.rangeByIndex("age_idx", 30, 65);

        CommandBuffer cb = encodeBackgroundCommand(filter, true, true);
        List<Integer> types = fieldTypes(cb);

        assertEquals("age_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
        assertTrue(types.contains(FieldType.FILTER_EXP));
        assertFalse(types.contains(FieldType.WHERE));
    }

    @Test
    void expressionFilterSendsExpressionFieldsAndResidualFilterExp() {
        Expression ageExp = Exp.build(Exp.intBin("age"));
        Filter filter = Filter.range(ageExp, 30, 65);

        CommandBuffer cb = encodeBackgroundCommand(filter, true);
        List<Integer> types = fieldTypes(cb);

        assertArrayEquals(ageExp.getBytes(), fieldBytes(cb, FieldType.INDEX_EXPRESSION));
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
        assertTrue(types.contains(FieldType.FILTER_EXP));
        assertFalse(types.contains(FieldType.WHERE));
        assertFalse(types.contains(FieldType.INDEX_NAME));
    }

    @Test
    void collectionFilterSendsIndexType() {
        Filter filter = Filter.containsByIndex("licenses_idx", IndexCollectionType.LIST, "7XYZ789");

        CommandBuffer cb = encodeBackgroundCommand(filter, true);

        assertEquals((byte) IndexCollectionType.LIST.ordinal(), fieldBytes(cb, FieldType.INDEX_TYPE)[0]);
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
    }

    @Test
    void cdtFilterSendsIndexContext() {
        Filter filter = Filter.range("scores", IndexCollectionType.LIST, 10, 20, CTX.listIndex(0));

        CommandBuffer cb = encodeBackgroundCommand(filter, true);

        assertArrayEquals(filter.getPackedCtx(), fieldBytes(cb, FieldType.INDEX_CONTEXT));
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
    }

    @Test
    void filterOnlySendsIndexFieldsWithoutResidualFilterExp() {
        Filter filter = Filter.rangeByIndex("age_idx", 30, 65);

        CommandBuffer cb = encodeBackgroundCommand(filter, false);
        List<Integer> types = fieldTypes(cb);

        assertEquals("age_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
        assertFalse(types.contains(FieldType.FILTER_EXP));
        assertFalse(types.contains(FieldType.WHERE));
    }

    private static CommandBuffer encodeBackgroundCommand(Filter filter, boolean residualWhere) {
        return encodeBackgroundCommand(filter, residualWhere, false);
    }

    private static CommandBuffer encodeBackgroundCommand(
        Filter filter,
        boolean residualWhere,
        boolean whereBeforeFilter
    ) {
        try (Cluster cluster = TestClusters.disconnected(Version.SERVER_VERSION_8_2)) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            BackgroundOperationBuilder builder = session.backgroundTask()
                .update(DATA_SET)
                .bin("campaign").setTo("updated");

            if (residualWhere && whereBeforeFilter) {
                builder.where(Exp.gt(Exp.intBin("score"), Exp.val(10)));
            }
            builder.filter(filter);
            if (residualWhere && !whereBeforeFilter) {
                builder.where(Exp.gt(Exp.intBin("score"), Exp.val(10)));
            }

            BackgroundQueryCommand cmd = builder.buildCommand(cluster, settings(session), 12345L);
            CommandBuffer cb = new CommandBuffer();
            cb.setBackgroundQuery(cmd);
            assertNotNull(cb.getBuffer());
            return cb;
        }
    }

    private static ResolvedSettings settings(Session session) {
        return session.getBehavior().getSettings(
            Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
    }

    private static byte[] indexRangeBody(Filter filter) {
        byte[] out = new byte[1 + filter.estimateSize()];
        out[0] = 1;
        filter.write(out, 1);
        return out;
    }

    private static List<Integer> fieldTypes(CommandBuffer cb) {
        byte[] buffer = cb.getBuffer();
        int fieldCount = Buffer.bytesToShort(buffer, 26);
        int offset = Command.MSG_TOTAL_HEADER_SIZE;
        List<Integer> types = new ArrayList<>(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            int len = Buffer.bytesToInt(buffer, offset);
            offset += 4;
            int type = buffer[offset++] & 0xFF;
            types.add(type);
            offset += len - 1;
        }
        return types;
    }

    private static byte[] fieldBytes(CommandBuffer cb, int fieldType) {
        byte[] buffer = cb.getBuffer();
        int fieldCount = Buffer.bytesToShort(buffer, 26);
        int offset = Command.MSG_TOTAL_HEADER_SIZE;

        for (int i = 0; i < fieldCount; i++) {
            int len = Buffer.bytesToInt(buffer, offset);
            offset += 4;
            int type = buffer[offset++] & 0xFF;
            int size = len - 1;
            if (type == fieldType) {
                byte[] value = new byte[size];
                System.arraycopy(buffer, offset, value, 0, size);
                return value;
            }
            offset += size;
        }
        throw new AssertionError("Field not found: " + fieldType);
    }

    private static String fieldUtf8(CommandBuffer cb, int fieldType) {
        return new String(fieldBytes(cb, fieldType), StandardCharsets.UTF_8);
    }
}
