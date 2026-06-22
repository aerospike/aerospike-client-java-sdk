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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;

class IndexProbeCommandTest {

    private static final Expression PREDICATE = Exp.build(Exp.val(1));

    @Test
    void setsInfo4QuerySelectionBit() {
        CommandBuffer cb = encodeProbe(null);
        assertEquals(Command.INFO4_QUERY_SELECTION, cb.getBuffer()[12] & Command.INFO4_QUERY_SELECTION);
    }

    @Test
    void operationCountIsZero() {
        CommandBuffer cb = encodeProbe(null);
        int opCount = Buffer.bytesToShort(cb.getBuffer(), 28);
        assertEquals(0, opCount);
    }

    @Test
    void fieldOrderWithoutHint() {
        CommandBuffer cb = encodeProbe(null);
        List<Integer> types = fieldTypes(cb);

        assertEquals(
            List.of(
                FieldType.NAMESPACE,
                FieldType.TABLE,
                FieldType.SOCKET_TIMEOUT,
                FieldType.QUERY_ID,
                FieldType.FILTER_EXP
            ),
            types
        );
    }

    @Test
    void fieldOrderWithIndexNameHint() {
        CommandBuffer cb = encodeProbe("age_idx");
        List<Integer> types = fieldTypes(cb);

        assertEquals(
            List.of(
                FieldType.NAMESPACE,
                FieldType.TABLE,
                FieldType.SOCKET_TIMEOUT,
                FieldType.QUERY_ID,
                FieldType.INDEX_NAME,
                FieldType.FILTER_EXP
            ),
            types
        );
        assertEquals("age_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
    }

    @Test
    void noPartitionOrIndexRangeFields() {
        CommandBuffer cb = encodeProbe("age_idx");
        List<Integer> types = fieldTypes(cb);

        assertFalse(types.contains(FieldType.PID_ARRAY));
        assertFalse(types.contains(FieldType.DIGEST_ARRAY));
        assertFalse(types.contains(FieldType.BVAL_ARRAY));
        assertFalse(types.contains(FieldType.INDEX_RANGE));
    }

    @Test
    void predicateBytesWrittenInFilterExp() {
        CommandBuffer cb = encodeProbe(null);
        assertArrayEquals(PREDICATE.getBytes(), fieldBytes(cb, FieldType.FILTER_EXP));
    }

    @Test
    void namespaceAndSetValues() {
        CommandBuffer cb = encodeProbe(null);
        assertEquals("test", fieldUtf8(cb, FieldType.NAMESPACE));
        assertEquals("users", fieldUtf8(cb, FieldType.TABLE));
    }

    @Test
    void socketTimeoutAndTaskId() {
        IndexProbeCommand cmd = probeCommand(42L, null);
        CommandBuffer cb = new CommandBuffer();
        cb.setIndexProbe(cmd);

        assertEquals(cmd.socketTimeout, Buffer.bytesToInt(fieldBytes(cb, FieldType.SOCKET_TIMEOUT), 0));
        assertEquals(42L, Buffer.bytesToLong(fieldBytes(cb, FieldType.QUERY_ID), 0));
    }

    @Test
    void missingNamespaceThrows() {
        assertThrows(AerospikeException.class, () ->
            new IndexProbeCommand(null, "", "users", PREDICATE, null, 1L, settings()));
    }

    @Test
    void missingPredicateThrows() {
        assertThrows(AerospikeException.class, () ->
            new IndexProbeCommand(null, "test", "users", null, null, 1L, settings()));
    }

    @Test
    void blankIndexHintOmitsField21() {
        CommandBuffer cb = encodeProbe("  ");
        assertFalse(fieldTypes(cb).contains(FieldType.INDEX_NAME));
    }

    @Test
    void protoHeaderMarksAsMsg() {
        CommandBuffer cb = encodeProbe(null);
        long proto = Buffer.bytesToLong(cb.getBuffer(), 0);
        long type = (proto >> 48) & 0xff;
        assertEquals(Command.AS_MSG_TYPE, type);
    }

    private static CommandBuffer encodeProbe(String indexHint) {
        CommandBuffer cb = new CommandBuffer();
        cb.setIndexProbe(probeCommand(7L, indexHint));
        return cb;
    }

    private static IndexProbeCommand probeCommand(long taskId, String indexHint) {
        return new IndexProbeCommand(null, "test", "users", PREDICATE, indexHint, taskId, settings());
    }

    private static ResolvedSettings settings() {
        return Behavior.DEFAULT.getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
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
