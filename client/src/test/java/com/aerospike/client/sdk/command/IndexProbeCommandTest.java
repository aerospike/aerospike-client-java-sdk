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
package com.aerospike.client.sdk.command;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ErrorDetailVerbosity;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

class IndexProbeCommandTest {

    private static final String AEL = "$.age > 30";

    @Test
    void info4CarriesErrorDetailVerbosity() {
        CommandBuffer cb = encodeExplain(null);
        assertEquals(0, cb.getBuffer()[12] & Command.INFO4_ERROR_VERBOSITY_MASK);

        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE)
            )
        );
        ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        CommandBuffer verbose = new CommandBuffer();
        verbose.setQueryExplain(explainCommand(7L, null, QueryWhereWire.FLAG_EXPLAIN, settings));

        assertEquals(
            settings.getErrorDetailBits(),
            verbose.getBuffer()[12] & Command.INFO4_ERROR_VERBOSITY_MASK
        );
    }

    @Test
    void operationCountIsZero() {
        CommandBuffer cb = encodeExplain(null);
        int opCount = Buffer.bytesToShort(cb.getBuffer(), 28);
        assertEquals(0, opCount);
    }

    @Test
    void fieldOrderWithoutHint() {
        CommandBuffer cb = encodeExplain(null);
        List<Integer> types = fieldTypes(cb);

        assertEquals(
            List.of(
                FieldType.NAMESPACE,
                FieldType.TABLE,
                FieldType.SOCKET_TIMEOUT,
                FieldType.QUERY_ID,
                FieldType.WHERE
            ),
            types
        );
    }

    @Test
    void fieldOrderWithIndexNameHint() {
        CommandBuffer cb = encodeExplain("age_idx");
        List<Integer> types = fieldTypes(cb);

        assertEquals(
            List.of(
                FieldType.NAMESPACE,
                FieldType.TABLE,
                FieldType.SOCKET_TIMEOUT,
                FieldType.QUERY_ID,
                FieldType.INDEX_NAME,
                FieldType.WHERE
            ),
            types
        );
        assertEquals("age_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
    }

    @Test
    void noPartitionOrIndexRangeFields() {
        CommandBuffer cb = encodeExplain("age_idx");
        List<Integer> types = fieldTypes(cb);

        assertFalse(types.contains(FieldType.PID_ARRAY));
        assertFalse(types.contains(FieldType.DIGEST_ARRAY));
        assertFalse(types.contains(FieldType.BVAL_ARRAY));
        assertFalse(types.contains(FieldType.INDEX_RANGE));
        assertFalse(types.contains(FieldType.FILTER_EXP));
    }

    @Test
    void wherePayloadWrittenInField44() {
        CommandBuffer cb = encodeExplain(null);
        assertArrayEquals(QueryWhereWire.forExplain(AEL), fieldBytes(cb, FieldType.WHERE));
        assertEquals(AEL, QueryWhereWire.ael(fieldBytes(cb, FieldType.WHERE)));
        assertEquals(QueryWhereWire.FLAG_EXPLAIN,
            QueryWhereWire.flags(fieldBytes(cb, FieldType.WHERE)));
    }

    @Test
    void wherePayloadIncludesRequireIndexFlag() {
        int flags = QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX;
        CommandBuffer cb = encodeExplain(null, flags);
        assertEquals(flags, QueryWhereWire.flags(fieldBytes(cb, FieldType.WHERE)));
    }

    @Test
    void wherePayloadIncludesHardHintFlag() {
        int flags = QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT;
        CommandBuffer cb = encodeExplain("age_idx", flags);
        assertEquals(flags, QueryWhereWire.flags(fieldBytes(cb, FieldType.WHERE)));
    }

    @Test
    void namespaceAndSetValues() {
        CommandBuffer cb = encodeExplain(null);
        assertEquals("test", fieldUtf8(cb, FieldType.NAMESPACE));
        assertEquals("users", fieldUtf8(cb, FieldType.TABLE));
    }

    @Test
    void socketTimeoutAndTaskId() {
        IndexProbeCommand cmd = explainCommand(42L, null);
        CommandBuffer cb = new CommandBuffer();
        cb.setQueryExplain(cmd);

        assertEquals(cmd.socketTimeout, Buffer.bytesToInt(fieldBytes(cb, FieldType.SOCKET_TIMEOUT), 0));
        assertEquals(42L, Buffer.bytesToLong(fieldBytes(cb, FieldType.QUERY_ID), 0));
    }

    @Test
    void blankIndexHintOmitsField21() {
        CommandBuffer cb = encodeExplain("  ");
        assertFalse(fieldTypes(cb).contains(FieldType.INDEX_NAME));
    }

    @Test
    void protoHeaderMarksAsMsg() {
        CommandBuffer cb = encodeExplain(null);
        long proto = Buffer.bytesToLong(cb.getBuffer(), 0);
        long type = (proto >> 48) & 0xff;
        assertEquals(Command.AS_MSG_TYPE, type);
    }

    @Test
    void rejectsNullAel() {
        assertThrows(IllegalArgumentException.class,
            () -> new IndexProbeCommand(null, "test", "users", null, null, 7L, settings()));
    }

    @Test
    void rejectsBlankAel() {
        assertThrows(IllegalArgumentException.class,
            () -> new IndexProbeCommand(null, "test", "users", "  ", null, 7L, settings()));
    }

    private static CommandBuffer encodeExplain(String indexHint) {
        return encodeExplain(indexHint, QueryWhereWire.FLAG_EXPLAIN);
    }

    private static CommandBuffer encodeExplain(String indexHint, int whereFlags) {
        CommandBuffer cb = new CommandBuffer();
        cb.setQueryExplain(explainCommand(7L, indexHint, whereFlags));
        return cb;
    }

    private static IndexProbeCommand explainCommand(long taskId, String indexHint) {
        return explainCommand(taskId, indexHint, QueryWhereWire.FLAG_EXPLAIN);
    }

    private static IndexProbeCommand explainCommand(long taskId, String indexHint, int whereFlags) {
        return explainCommand(taskId, indexHint, whereFlags, settings());
    }

    private static IndexProbeCommand explainCommand(
        long taskId, String indexHint, int whereFlags, ResolvedSettings settings
    ) {
        return new IndexProbeCommand(null, "test", "users", AEL, indexHint, whereFlags, taskId, settings);
    }

    private static ResolvedSettings settings() {
        return Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
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
