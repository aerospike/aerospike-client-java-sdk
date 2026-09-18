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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.PartitionFilter;
import com.aerospike.client.sdk.command.PartitionTracker;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.Order;
import com.aerospike.client.sdk.query.OrderByFlags;
import com.aerospike.client.sdk.query.OrderByType;
import com.aerospike.client.sdk.query.QueryBuilder;

class QueryTopKWireTest {
    @Test
    void concatenatesTwoOrderBySpecsAndBigEndianLimit() {
        QueryCommand command = command(true);
        Map<Integer, byte[]> fields = fields(command);

        assertArrayEquals(new byte[] {
            1, 0, 0, 5, 'f', 'i', 'r', 's', 't',
            3, 1, 1, 6, 's', 'e', 'c', 'o', 'n', 'd'
        }, fields.get(FieldType.ORDER_BY));
        assertArrayEquals(new byte[] {0, 0, 0, 25}, fields.get(FieldType.TOP_K));
    }

    @Test
    void singleSpecOrderByIsByteIdenticalToLegacyFormat() {
        Session session = new Session(null, Behavior.DEFAULT);
        DataSet dataSet = DataSet.of("test", "topk_wire");
        QueryBuilder builder = new QueryBuilder(session, dataSet)
            .orderBy("rank", OrderByType.INTEGER, Order.ASC)
            .topK(3);
        ResolvedSettings settings = Behavior.DEFAULT.getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
        QueryCommand command = new QueryCommand(null, dataSet, null, null, settings, builder);
        command.setSendTopK(true);

        Map<Integer, byte[]> fields = fields(command);
        assertArrayEquals(new byte[] {1, 0, 0, 4, 'r', 'a', 'n', 'k'}, fields.get(FieldType.ORDER_BY));
        assertArrayEquals(new byte[] {0, 0, 0, 3}, fields.get(FieldType.TOP_K));
    }

    @Test
    void omitsPushdownFieldsForMixedVersionFallback() {
        Map<Integer, byte[]> fields = fields(command(false));

        assertFalse(fields.containsKey(FieldType.ORDER_BY));
        assertFalse(fields.containsKey(FieldType.TOP_K));
        assertTrue(fields.containsKey(FieldType.NAMESPACE));
    }

    private static QueryCommand command(boolean sendTopK) {
        Session session = new Session(null, Behavior.DEFAULT);
        DataSet dataSet = DataSet.of("test", "topk_wire");
        QueryBuilder builder = new QueryBuilder(session, dataSet)
            .orderBy("first", OrderByType.INTEGER, Order.ASC)
            .thenOrderBy("second", OrderByType.STRING, Order.DESC, OrderByFlags.CASE_INSENSITIVE)
            .topK(25);
        ResolvedSettings settings = Behavior.DEFAULT.getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
        QueryCommand command = new QueryCommand(null, dataSet, null, null, settings, builder);
        command.setSendTopK(sendTopK);
        return command;
    }

    private static Map<Integer, byte[]> fields(QueryCommand command) {
        PartitionTracker tracker = new PartitionTracker(command, new Node[1], PartitionFilter.all());
        CommandBuffer buffer = new CommandBuffer();
        buffer.setQuery(command, tracker, null, 1L);

        byte[] bytes = buffer.getBuffer();
        int count = Buffer.bytesToShort(bytes, 26);
        int offset = Command.MSG_TOTAL_HEADER_SIZE;
        Map<Integer, byte[]> fields = new HashMap<>();
        for (int i = 0; i < count; i++) {
            int size = Buffer.bytesToInt(bytes, offset) - 1;
            offset += 4;
            int type = bytes[offset++] & 0xff;
            byte[] value = new byte[size];
            System.arraycopy(bytes, offset, value, 0, size);
            offset += size;
            fields.put(type, value);
        }
        return fields;
    }
}
