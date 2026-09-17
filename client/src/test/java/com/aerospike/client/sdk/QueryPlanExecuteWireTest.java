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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.MsgFieldParser;
import com.aerospike.client.sdk.command.PartitionFilter;
import com.aerospike.client.sdk.command.PartitionTracker;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.query.QueryBuilder;
import com.aerospike.client.sdk.query.plan.IndexRangeWire;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

public class QueryPlanExecuteWireTest {

    private static final String AEL = "$.age > 30";
    private static final byte[] EXPLAIN_WHERE = QueryWhereWire.forExplain(AEL);

    @Test
    void secondaryIndexPlanSendsWhereIndexNameRangeAndType() {
        byte[] probeRangeBytes = probeIndexRangeBytes();
        QueryPlan plan = secondaryIndexPlan(probeRangeBytes, IndexCollectionType.LIST);
        byte[] executeRangeBytes = IndexRangeWire.forExecuteWithIndexName(probeRangeBytes);

        QueryCommand cmd = queryCommandForPlan(plan);
        CommandBuffer cb = encodeQuery(cmd);

        assertEquals("age_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
        assertArrayEquals(executeRangeBytes, fieldBytes(cb, FieldType.INDEX_RANGE));
        assertArrayEquals(plan.getExecuteWhereBytes(), fieldBytes(cb, FieldType.WHERE));
        assertEquals(0, QueryWhereWire.flags(fieldBytes(cb, FieldType.WHERE)));
        assertEquals((byte) IndexCollectionType.LIST.ordinal(),
            fieldBytes(cb, FieldType.INDEX_TYPE)[0]);
        assertFalse(fieldTypes(cb).contains(FieldType.FILTER_EXP));
        assertTrue(cmd.isPlanDriven());
    }

    @Test
    void primaryIndexPlanSendsWhereOnly() {
        QueryPlan plan = QueryPlan.fromExplainResponse(
            ResultCode.OK, "test", "users", EXPLAIN_WHERE, fieldsOf());

        QueryCommand cmd = queryCommandForPlan(plan);
        CommandBuffer cb = encodeQuery(cmd);
        List<Integer> types = fieldTypes(cb);

        assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection());
        assertFalse(types.contains(FieldType.INDEX_RANGE));
        assertFalse(types.contains(FieldType.INDEX_NAME));
        assertFalse(types.contains(FieldType.FILTER_EXP));
        assertArrayEquals(plan.getExecuteWhereBytes(), fieldBytes(cb, FieldType.WHERE));
    }

    private static byte[] probeIndexRangeBytes() {
        Filter structured = Filter.equal("age", 30L);
        byte[] wireBody = new byte[1 + structured.estimateSize()];
        wireBody[0] = 1;
        structured.write(wireBody, 1);
        return wireBody;
    }

    private static QueryPlan secondaryIndexPlan(byte[] rangeBytes, IndexCollectionType indexType) {
        return QueryPlan.fromExplainResponse(
            ResultCode.OK,
            "test",
            "users",
            EXPLAIN_WHERE,
            fieldsOf(
                field(FieldType.INDEX_NAME, "age_idx"),
                field(FieldType.INDEX_TYPE, new byte[] {(byte) indexType.ordinal()}),
                field(FieldType.INDEX_RANGE, rangeBytes)
            )
        );
    }

    private static QueryCommand queryCommandForPlan(QueryPlan plan) {
        Session session = new Session(null, Behavior.DEFAULT);
        DataSet dataSet = DataSet.of("test", "users");
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        ResolvedSettings settings = Behavior.DEFAULT.getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
        return QueryCommand.forPlan(null, dataSet, plan, settings, qb);
    }

    private static CommandBuffer encodeQuery(QueryCommand cmd) {
        PartitionTracker tracker = new PartitionTracker(
            cmd, new Node[1], PartitionFilter.all());
        CommandBuffer cb = new CommandBuffer();
        cb.setQuery(cmd, tracker, null, 9L);
        return cb;
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
        Buffer.intToBytes(value.length + 1, buffer, offset);
        offset += 4;
        buffer[offset++] = (byte) type;
        System.arraycopy(value, 0, buffer, offset, value.length);
        return offset + value.length;
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

    private record Field(int type, byte[] value) {}
}
