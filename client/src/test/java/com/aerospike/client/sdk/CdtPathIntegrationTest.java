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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.util.Version;

/**
 * Server integration tests for CDT path expressions: {@link CTX}, {@link CdtOperation}
 * {@code selectByPath}/{@code modifyByPath}, and fluent path terminals
 * ({@code onEachChild}, {@code collectValues}, {@code modifyBy}, {@code removeMatches}, …).
 */
class CdtPathIntegrationTest extends ClusterTest {

    private static final String BIN = "cdtPath";

    @BeforeAll
    static void requireCdtPathServer() {
        Version serverVersion = cluster.getRandomNode().getVersion();
        Assumptions.assumeTrue(serverVersion.isGreaterOrEqual(8, 1, 1, 0),
            "CDT path ops require server version 8.1.1+");
    }

    /** {@code mapKeysIn} / {@code andFilter} CTX entries require server 8.1.2+ (see CdtExpTest). */
    private static void assumeMapKeysInAndFilterContexts() {
        Assumptions.assumeTrue(
            cluster.getRandomNode().getVersion().isGreaterOrEqual(8, 1, 2, 0),
            "mapKeysIn/andFilter CTX ops require server version 8.1.2+");
    }

    @Nested
    class SelectMapKeysIn {

        @BeforeEach
        void requireMapKeysInServer() {
            assumeMapKeysInAndFilterContexts();
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("com.aerospike.client.sdk.CdtPathIntegrationTest#mapKeysInCases")
        void returnsMatchingValues(String label, Map<?, ?> map, CTX ctx, Long... expectedValues) {
            Key key = args.set.id("cdtPathMapKeysIn_" + label);
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(map).execute();

            Record result = session.query(key)
                .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
                .execute()
                .getFirstRecord();

            assertThat(toLongList(result.getList(BIN)))
                .containsExactlyInAnyOrderElementsOf(List.of(expectedValues));
        }

        @Test
        void withAndFilterReturnsMatchingKeyValues() {
            Key key = args.set.id("cdtPathMapKeysInAndFilter");
            session.delete(key).execute();

            Map<String, Object> map = new HashMap<>();
            map.put("a", 5);
            map.put("b", 15);
            map.put("c", 25);
            map.put("d", 35);
            session.upsert(key).bin(BIN).setTo(map).execute();

            CTX keys = CTX.mapKeysIn("a", "b", "c");
            CTX filter = CTX.andFilter(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)));

            Record result = session.query(key)
                .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.MAP_KEY_VALUE, keys, filter))
                .execute()
                .getFirstRecord();

            Map<String, Long> entries = keyValuesFromSelectResult(result.getValue(BIN));
            assertThat(entries).hasSize(2);
            assertThat(entries).containsEntry("b", 15L).containsEntry("c", 25L);
        }
    }

    static Stream<Arguments> mapKeysInCases() {
        Map<String, Object> stringKeys = new HashMap<>();
        stringKeys.put("alpha", 10);
        stringKeys.put("beta", 20);
        stringKeys.put("gamma", 30);
        stringKeys.put("delta", 40);

        Map<Integer, Object> intKeys = new HashMap<>();
        intKeys.put(1, 100L);
        intKeys.put(2, 200L);
        intKeys.put(3, 300L);
        intKeys.put(4, 400L);

        Map<Long, Object> longKeys = new LinkedHashMap<>();
        longKeys.put(1L, 100L);
        longKeys.put(2L, 200L);
        longKeys.put(3L, 300L);

        return Stream.of(
            Arguments.of("string", stringKeys, CTX.mapKeysIn("alpha", "gamma"), new Long[] {10L, 30L}),
            Arguments.of("int", intKeys, CTX.mapKeysIn(1, 3), new Long[] {100L, 300L}),
            Arguments.of("long", longKeys, CTX.mapKeysIn(1L, 3L), new Long[] {100L, 300L})
        );
    }

    @Test
    void selectMapIndexValue() {
        Key key = args.set.id("cdtPathMapIndex");
        session.delete(key).execute();

        AerospikeMap<String, Integer> map = AerospikeMap.of(MapOrder.KEY_ORDERED, 4);
        map.put("a", 10);
        map.put("b", 20);
        map.put("c", 30);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX ctx = CTX.mapIndex(1);
        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
            .execute()
            .getFirstRecord();

        assertThat(toLongList(result.getList(BIN))).containsExactly(20L);
    }

    private static List<Long> toLongList(List<?> values) {
        return values.stream().map(v -> ((Number) v).longValue()).toList();
    }

    @Test
    void removeMatchesFiltersListElements() {
        Key key = args.set.id("cdtPathRemoveMatches");
        session.delete(key).execute();

        List<Integer> numbers = new ArrayList<>(List.of(1, 5, 10, 15, 20, 25, 30));
        session.upsert(key).bin(BIN).setTo(numbers).execute();

        session.update(key)
            .bin(BIN)
                .onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)))
                .removeMatches()
            .execute();

        assertThat(toLongList(session.query(key).execute().getFirstRecord().getList(BIN)))
            .containsExactly(1L, 5L, 10L);
    }

    @Test
    void collectValuesNoFailOnInvalidFilterType() {
        Key key = args.set.id("cdtPathCollectNoFailInvalidType");
        session.delete(key).execute();

        session.upsert(key)
            .bin(BIN).setTo(Map.of("items", List.of(5, "not-an-int", 20)))
            .execute();

        Exp filter = Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10));

        assertThrows(AerospikeException.class, () ->
            session.query(key)
                .bin(BIN)
                    .onMapKey("items")
                    .onEachChild(filter)
                    .collectValues()
                .execute()
                .getFirstRecord());

        Record result = session.query(key)
            .bin(BIN)
                .onMapKey("items")
                .onEachChild(filter)
                .collectValues(o -> o.noFail(true))
            .execute()
            .getFirstRecord();
        assertThat(toLongList(result.getList(BIN))).containsExactly(20L);
    }

    @Nested
    class EmptyContextRejectedByServer {

        @Test
        void selectByPath() {
            Key key = args.set.id("cdtPathEmptyContextSelectServer");
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(Map.of("a", 10, "b", 20)).execute();

            AerospikeException ae = assertThrows(AerospikeException.class, () ->
                session.query(key)
                    .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE))
                    .execute());
            assertThat(ae.getResultCode()).isEqualTo(ResultCode.PARAMETER_ERROR);
        }

        @Test
        void modifyByPath() {
            Key key = args.set.id("cdtPathEmptyContextModifyServer");
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(List.of(10, 20, 30)).execute();

            Expression modifyExp = Exp.build(Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5)));

            AerospikeException ae = assertThrows(AerospikeException.class, () ->
                session.update(key)
                    .appendOperations(CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, modifyExp))
                    .execute());
            assertThat(ae.getResultCode()).isEqualTo(ResultCode.PARAMETER_ERROR);
        }
    }

    @Nested
    class AndFilterAsFirstContext {

        private Key key;

        @BeforeEach
        void seed() {
            assumeMapKeysInAndFilterContexts();

            key = args.set.id("cdtPathAndFilterFirst");
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(Map.of("a", 1)).execute();
        }

        @Test
        void rejectedByServer() {
            CTX[] ctx = new CTX[] {
                CTX.andFilter(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0))),
            };

            AerospikeException ae = assertThrows(AerospikeException.class, () ->
                session.query(key)
                    .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
                    .execute());
            assertThat(ae.getResultCode()).isEqualTo(ResultCode.PARAMETER_ERROR);
        }
    }

    @Nested
    class AndFilterAfterExpressionContext {

        private Key key;

        @BeforeEach
        void seed() {
            key = args.set.id("cdtPathAndFilterAfterExp");
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(Map.of("items", List.of(1, 2, 3))).execute();
        }

        @Test
        void rejectedByServer() {
            CTX[] ctx = new CTX[] {
                CTX.allChildrenWithFilter(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0))),
                CTX.andFilter(Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))),
            };

            AerospikeException ae = assertThrows(AerospikeException.class, () ->
                session.query(key)
                    .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
                    .execute());
            assertThat(ae.getResultCode()).isEqualTo(ResultCode.PARAMETER_ERROR);
        }
    }

    @Nested
    class ChainedAndFilters {

        private Key key;

        @BeforeEach
        void seed() {
            assumeMapKeysInAndFilterContexts();

            key = args.set.id("cdtPathAndFilterChained");
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(Map.of("a", 5, "b", 15, "c", 25)).execute();
        }

        @Test
        void rejectedByServer() {
            CTX[] ctx = new CTX[] {
                CTX.mapKeysIn("a", "b", "c"),
                CTX.andFilter(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))),
                CTX.andFilter(Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(30))),
            };

            AerospikeException ae = assertThrows(AerospikeException.class, () ->
                session.query(key)
                    .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
                    .execute());
            assertThat(ae.getResultCode()).isEqualTo(ResultCode.PARAMETER_ERROR);
        }
    }

    @Nested
    class CollectTree {

        @Test
        void onValidPathReturnsPartialTree() {
            Key key = args.set.id("cdtPathCollectTreeValid");
            session.delete(key).execute();
            session.upsert(key).bin(BIN).setTo(Map.of("nums", List.of(10, 20))).execute();

            Record result = session.query(key)
                .bin(BIN)
                    .onMapKey("nums")
                    .onEachChild()
                    .collectTree()
                .execute()
                .getFirstRecord();

            AerospikeMap<?, ?> mapTree = assertInstanceOf(AerospikeMap.class, result.getValue(BIN));
            assertThat(mapTree).hasSize(1);
            assertThat(mapTree.get("nums")).isEqualTo(List.of(10L, 20L));
        }

        @Nested
        class OnMissingPath {

            private Key key;

            @BeforeEach
            void seed() {
                key = args.set.id("cdtPathCollectTreeMissing");
                session.delete(key).execute();
                session.upsert(key).bin(BIN).setTo(Map.of("nums", List.of(1, 2, 3))).execute();
            }

            @ParameterizedTest
            @ValueSource(booleans = {false, true})
            void returnsEmptyTree(boolean noFail) {
                Record result;
                if (noFail) {
                    result = session.query(key)
                        .bin(BIN)
                            .onMapKey("missing")
                            .onEachChild()
                            .collectTree(o -> o.noFail(true))
                        .execute()
                        .getFirstRecord();
                }
                else {
                    result = session.query(key)
                        .bin(BIN)
                            .onMapKey("missing")
                            .onEachChild()
                            .collectTree()
                        .execute()
                        .getFirstRecord();
                }

                AerospikeMap<?, ?> tree = assertInstanceOf(AerospikeMap.class, result.getValue(BIN));
                assertThat(tree).isEmpty();
            }
        }
    }

    private static Map<String, Long> keyValuesFromSelectResult(Object raw) {
        assertNotNull(raw);

        if (raw instanceof AerospikeMap<?, ?> map) {
            Map<String, Long> out = new HashMap<>();
            map.forEach((k, v) -> out.put(k.toString(), ((Number) v).longValue()));
            return out;
        }

        AerospikeList<?> list = assertInstanceOf(AerospikeList.class, raw,
            () -> "unexpected MAP_KEY_VALUE result type: " + raw.getClass());

        if (list.size() == 1) {
            Object sole = list.get(0);
            if (sole instanceof AerospikeMap<?, ?>) {
                return keyValuesFromSelectResult(sole);
            }
        }

        Map<String, Long> out = new HashMap<>();
        if (!list.isEmpty() && list.get(0) instanceof AerospikeList<?>) {
            for (Object item : list) {
                AerospikeList<?> pair = (AerospikeList<?>) item;
                out.put(pair.get(0).toString(), ((Number) pair.get(1)).longValue());
            }
            return out;
        }

        for (int i = 0; i + 1 < list.size(); i += 2) {
            out.put(list.get(i).toString(), ((Number) list.get(i + 1)).longValue());
        }
        return out;
    }
}
