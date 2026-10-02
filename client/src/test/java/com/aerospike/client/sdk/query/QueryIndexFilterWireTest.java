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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TestClusters;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.PartitionFilter;
import com.aerospike.client.sdk.command.PartitionTracker;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.util.Version;

public class QueryIndexFilterWireTest {
    private static final DataSet DATA_SET = DataSet.of("test", "vehicles");
    private static final String RESIDUAL_WHERE = "$.status == 'active'";

    @Test
    void indexNameCollectionFilterSendsIndexFieldsAndResidualFilterExp() {
        Filter filter = Filter.containsByIndex("licenses_idx", IndexCollectionType.LIST, "7XYZ789");

        QueryCommand cmd = buildCommand(filter, null);
        CommandBuffer cb = encodeQuery(cmd);
        List<Integer> types = fieldTypes(cb);

        assertFalse(cmd.isPlanDriven());
        assertEquals("licenses_idx", fieldUtf8(cb, FieldType.INDEX_NAME));
        assertEquals((byte) IndexCollectionType.LIST.ordinal(), fieldBytes(cb, FieldType.INDEX_TYPE)[0]);
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
        assertTrue(types.contains(FieldType.FILTER_EXP));
        assertFalse(types.contains(FieldType.WHERE));
        assertFalse(types.contains(FieldType.INDEX_EXPRESSION));
    }

    @Test
    void expressionCollectionFilterSendsExpressionFieldsAndResidualFilterExp() {
        Expression licensesExp = Exp.build(Exp.listBin("licenses"));
        Filter filter = Filter.contains(licensesExp, IndexCollectionType.LIST, "7XYZ789");

        QueryCommand cmd = buildCommand(filter, null);
        CommandBuffer cb = encodeQuery(cmd);
        List<Integer> types = fieldTypes(cb);

        assertFalse(cmd.isPlanDriven());
        assertArrayEquals(licensesExp.getBytes(), fieldBytes(cb, FieldType.INDEX_EXPRESSION));
        assertEquals((byte) IndexCollectionType.LIST.ordinal(), fieldBytes(cb, FieldType.INDEX_TYPE)[0]);
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE));
        assertTrue(types.contains(FieldType.FILTER_EXP));
        assertFalse(types.contains(FieldType.WHERE));
        assertFalse(types.contains(FieldType.INDEX_NAME));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexSelectionHints")
    void explicitFilterPreservesIndexFieldsAcrossSelectionHints(
        String label,
        Function<QueryHint.Start, ? extends QueryHint.Result> hint
    ) {
        Filter filter = Filter.containsByIndex("licenses_idx", IndexCollectionType.LIST, "7XYZ789");

        CommandBuffer cb = encodeQuery(buildCommand(filter, hint));

        assertEquals("licenses_idx", fieldUtf8(cb, FieldType.INDEX_NAME), label);
        assertArrayEquals(indexRangeBody(filter), fieldBytes(cb, FieldType.INDEX_RANGE), label);
        assertFalse(fieldTypes(cb).contains(FieldType.WHERE), label);
    }

    @Test
    void queryDurationHintStillSetsShortQueryHeader() {
        Filter filter = Filter.containsByIndex("licenses_idx", IndexCollectionType.LIST, "7XYZ789");
        QueryCommand cmd = buildCommand(filter, hint -> hint.queryDuration(QueryDuration.SHORT));

        CommandBuffer cb = encodeQuery(cmd);

        assertTrue((cb.getBuffer()[9] & Command.INFO1_SHORT_QUERY) != 0);
    }

    @Test
    void publicConstructorAppliesHistoricalHintOverrideButExplicitFactoryDoesNot() {
        try (Cluster cluster = TestClusters.disconnected(Version.SERVER_VERSION_8_2)) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            ResolvedSettings settings = settings(session);
            Filter filter = Filter.equal("age", 30);
            QueryBuilder hinted = new QueryBuilder(session, DATA_SET)
                .withHint(hint -> hint.forIndex("hint_idx"));

            CommandBuffer publicCtor = encodeQuery(
                new QueryCommand(cluster, DATA_SET, filter, null, settings, hinted));
            CommandBuffer explicit = encodeQuery(
                QueryCommand.forExplicitFilter(cluster, DATA_SET, filter, null, settings, hinted));

            assertEquals("hint_idx", fieldUtf8(publicCtor, FieldType.INDEX_NAME));
            assertFalse(fieldTypes(explicit).contains(FieldType.INDEX_NAME));
        }
    }

    static Stream<Arguments> indexSelectionHints() {
        return Stream.of(
            Arguments.of("forIndex-hardHint",
                (Function<QueryHint.Start, ? extends QueryHint.Result>) hint -> hint.forIndex("other_idx").hardHint()),
            Arguments.of("forBin",
                (Function<QueryHint.Start, ? extends QueryHint.Result>) hint -> hint.forBin("other_bin")),
            Arguments.of("allowScansWithWhere",
                (Function<QueryHint.Start, ? extends QueryHint.Result>) QueryHint.Start::allowScansWithWhere),
            Arguments.of("disallowScansWithWhere",
                (Function<QueryHint.Start, ? extends QueryHint.Result>) QueryHint.Start::disallowScansWithWhere));
    }

    private static QueryCommand buildCommand(
        Filter filter,
        Function<QueryHint.Start, ? extends QueryHint.Result> hint
    ) {
        try (Cluster cluster = TestClusters.disconnected(Version.SERVER_VERSION_8_2)) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            QueryBuilder qb = new QueryBuilder(session, DATA_SET)
                .filter(filter)
                .where(RESIDUAL_WHERE);
            if (hint != null) {
                qb.withHint(hint);
            }
            return IndexProbePlanner.buildCommand(
                session, DATA_SET, qb.getAel(), qb.getQueryHint(), settings(session), qb);
        }
    }

    private static ResolvedSettings settings(Session session) {
        return session.getBehavior().getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
    }

    private static CommandBuffer encodeQuery(QueryCommand cmd) {
        PartitionTracker tracker = new PartitionTracker(
            cmd, new Node[1], PartitionFilter.all());
        CommandBuffer cb = new CommandBuffer();
        cb.setQuery(cmd, tracker, null, 9L);
        assertNotNull(cb.getBuffer());
        return cb;
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
