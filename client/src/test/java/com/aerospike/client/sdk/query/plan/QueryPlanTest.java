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
package com.aerospike.client.sdk.query.plan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.MsgFieldParser;

class QueryPlanTest {

    private static final byte[] PREDICATE = new byte[] {0x01, 0x02, 0x03};
    private static final byte[] RANGE = new byte[] {1, 3, 'a', 'g', 'e'};

    @Test
    void primaryIndexPlanWhenNoIndexFields() {
        MsgFieldParser fields = fieldsOf();
        QueryPlan plan = QueryPlan.fromProbeResponse(
            ResultCode.OK, "test", "users", PREDICATE, fields);

        assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection());
        assertTrue(plan.isPrimaryIndex());
        assertEquals("test", plan.getNamespace());
        assertEquals("users", plan.getSet());
        assertArrayEquals(PREDICATE, plan.getPredicateBytes());
        assertNull(plan.getIndexName());
        assertNull(plan.getIndexRangeBytes());
    }

    @Test
    void secondaryIndexPlanWhenNameAndRangePresent() {
        MsgFieldParser fields = fieldsOf(
            field(FieldType.INDEX_NAME, "age_idx"),
            field(FieldType.INDEX_RANGE, RANGE)
        );
        QueryPlan plan = QueryPlan.fromProbeResponse(
            ResultCode.OK, "test", null, PREDICATE, fields);

        assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection());
        assertTrue(plan.isSecondaryIndex());
        assertEquals("age_idx", plan.getIndexName());
        assertArrayEquals(RANGE, plan.getIndexRangeBytes());
        assertArrayEquals(PREDICATE, plan.getPredicateBytes());
    }

    @Test
    void filteredOutPlan() {
        QueryPlan plan = QueryPlan.fromProbeResponse(
            ResultCode.FILTERED_OUT, "test", "users", PREDICATE, fieldsOf());

        assertEquals(QuerySelection.FILTERED_OUT, plan.getSelection());
        assertTrue(plan.isFilteredOut());
        assertNull(plan.getIndexName());
        assertNull(plan.getIndexRangeBytes());
    }

    @Test
    void inconsistentResponseThrows() {
        MsgFieldParser fields = fieldsOf(field(FieldType.INDEX_NAME, "age_idx"));

        assertThrows(AerospikeException.Parse.class, () ->
            QueryPlan.fromProbeResponse(ResultCode.OK, "test", "users", PREDICATE, fields));
    }

    @Test
    void nonOkNonFilteredResultThrows() {
        assertThrows(AerospikeException.class, () ->
            QueryPlan.fromProbeResponse(ResultCode.PARAMETER_ERROR, "test", "users", PREDICATE, fieldsOf()));
    }

    @Test
    void querySelectionConstantMatchesProto() {
        assertEquals(1 << 7, Command.INFO4_QUERY_SELECTION);
    }

    private static MsgFieldParser fieldsOf(Field... entries) {
        int fieldCount = entries.length;
        int size = 0;
        for (Field entry : entries) {
            size += 4 + 1 + entry.value.length;
        }

        byte[] buffer = new byte[size];
        int offset = 0;
        for (Field entry : entries) {
            offset = writeField(buffer, offset, entry.type, entry.value);
        }
        return new MsgFieldParser(buffer, 0, fieldCount);
    }

    private static Field field(int type, String utf8) {
        return new Field(type, utf8.getBytes(StandardCharsets.UTF_8));
    }

    private static Field field(int type, byte[] value) {
        return new Field(type, value);
    }

    private static int writeField(byte[] buffer, int offset, int type, byte[] value) {
        com.aerospike.client.sdk.command.Buffer.intToBytes(value.length + 1, buffer, offset);
        offset += 4;
        buffer[offset++] = (byte) type;
        System.arraycopy(value, 0, buffer, offset, value.length);
        return offset + value.length;
    }

    private record Field(int type, byte[] value) {}
}
