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
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.MsgFieldParser;
import com.aerospike.client.sdk.query.IndexCollectionType;

public class QueryPlanTest {

    private static final String AEL = "$.age > 30";
    private static final byte[] EXPLAIN_WHERE = QueryWhereWire.forExplain(AEL);
    private static final byte[] RANGE = new byte[] {1, 3, 'a', 'g', 'e', 3};

    @Test
    void primaryIndexPlanWhenNoIndexFields() {
        MsgFieldParser fields = fieldsOf();
        QueryPlan plan = QueryPlan.fromExplainResponse(
            ResultCode.OK, "test", "users", EXPLAIN_WHERE, fields);

        assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection());
        assertTrue(plan.isPrimaryIndex());
        assertEquals("test", plan.getNamespace());
        assertEquals("users", plan.getSet());
        assertEquals(AEL, plan.getAel());
        assertArrayEquals(EXPLAIN_WHERE, plan.getExplainWhereBytes());
        assertArrayEquals(QueryWhereWire.forExecute(AEL), plan.getExecuteWhereBytes());
        assertNull(plan.getIndexName());
        assertNull(plan.getIndexRangeBytes());
        assertEquals(IndexCollectionType.DEFAULT, plan.getIndexType());
    }

    @Test
    void secondaryIndexPlanWhenNameRangeAndTypePresent() {
        MsgFieldParser fields = fieldsOf(
            field(FieldType.INDEX_NAME, "age_idx"),
            field(FieldType.INDEX_TYPE, new byte[] {(byte) IndexCollectionType.LIST.ordinal()}),
            field(FieldType.INDEX_RANGE, RANGE)
        );
        QueryPlan plan = QueryPlan.fromExplainResponse(
            ResultCode.OK, "test", null, EXPLAIN_WHERE, fields);

        assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection());
        assertTrue(plan.isSecondaryIndex());
        assertEquals("age_idx", plan.getIndexName());
        assertArrayEquals(RANGE, plan.getIndexRangeBytes());
        assertEquals(IndexCollectionType.LIST, plan.getIndexType());
    }

    @Test
    void filteredOutPlan() {
        QueryPlan plan = QueryPlan.fromExplainResponse(
            ResultCode.FILTERED_OUT, "test", "users", EXPLAIN_WHERE, fieldsOf());

        assertEquals(QuerySelection.FILTERED_OUT, plan.getSelection());
        assertTrue(plan.isFilteredOut());
        assertNull(plan.getIndexName());
        assertNull(plan.getIndexRangeBytes());
    }

    @Test
    void inconsistentResponseThrows() {
        MsgFieldParser fields = fieldsOf(field(FieldType.INDEX_NAME, "age_idx"));

        assertThrows(AerospikeException.Parse.class, () ->
            QueryPlan.fromExplainResponse(ResultCode.OK, "test", "users", EXPLAIN_WHERE, fields));
    }

    @Test
    void emptyIndexNameOnSiExplainThrows() {
        MsgFieldParser fields = fieldsOf(
            field(FieldType.INDEX_NAME, ""),
            field(FieldType.INDEX_RANGE, RANGE)
        );

        assertThrows(AerospikeException.Parse.class, () ->
            QueryPlan.fromExplainResponse(ResultCode.OK, "test", "users", EXPLAIN_WHERE, fields));
    }

    @Test
    void emptyIndexRangeOnSiExplainThrows() {
        MsgFieldParser fields = fieldsOf(
            field(FieldType.INDEX_NAME, "age_idx"),
            field(FieldType.INDEX_RANGE, new byte[0])
        );

        assertThrows(AerospikeException.Parse.class, () ->
            QueryPlan.fromExplainResponse(ResultCode.OK, "test", "users", EXPLAIN_WHERE, fields));
    }

    @Test
    void nonOkNonFilteredResultThrows() {
        assertThrows(AerospikeException.class, () ->
            QueryPlan.fromExplainResponse(
                ResultCode.PARAMETER_ERROR, "test", "users", EXPLAIN_WHERE, fieldsOf()));
    }

    @Test
    void multipleRangesOnSiExplainThrows() {
        MsgFieldParser fields = fieldsOf(
            field(FieldType.INDEX_NAME, "age_idx"),
            field(FieldType.INDEX_RANGE, new byte[] {2, 3, 'a', 'g', 'e', 3})
        );

        assertThrows(AerospikeException.Parse.class, () ->
            QueryPlan.fromExplainResponse(ResultCode.OK, "test", "users", EXPLAIN_WHERE, fields));
    }

    @Test
    void truncatedIndexRangeOnSiExplainThrows() {
        MsgFieldParser fields = fieldsOf(
            field(FieldType.INDEX_NAME, "age_idx"),
            field(FieldType.INDEX_RANGE, new byte[] {1, 3, 'a', 'g'})
        );

        assertThrows(AerospikeException.Parse.class, () ->
            QueryPlan.fromExplainResponse(ResultCode.OK, "test", "users", EXPLAIN_WHERE, fields));
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
