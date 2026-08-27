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

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assertPlan;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assumeQuerySelection;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.deleteKeys;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.dropIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.plan;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Query planner integration tests for MAPKEYS / MAPVALUES / LIST collection indexes with CDT
 * {@code .exists()} predicates on field {@code 44} explain.
 *
 * <p>Bare {@code .exists()} and {@code .exists() == true} both plan as collection SI candidates
 * when the selector carries a probe value (MAPKEYS key, MAPVALUES value, LIST value path).
 * Interval selectors — {@code [=lo:hi]} (list values), {@code {=lo:hi}} (map values) and
 * {@code {@lo:hi}} (map keys) — plan as collection SI candidates only when both bounds are
 * integers; string bounds stay on PI. LIST index paths ({@code [0]}) also fall back to PI —
 * value-containment indexes cannot answer positional existence.</p>
 */
public class QueryPlannerCollectionCdtTest extends ClusterTest {
    private static final String setName = "qp_cdt";
    private static final String keyPrefix = "qpcdt";
    private static final String mapBin = "map_bin";
    private static final String listBin = "list_bin";
    private static final String listStrBin = "list_str_bin";
    private static final String listStrTarget = "ls_target";
    private static final String mapKey = "mkey2";
    private static final String mapValueTarget = "mv_target";
    private static final String mapIndex = "qp_mapkeys_idx";
    private static final String mapValuesIndex = "qp_mapvalues_idx";
    private static final String listIndex = "qp_list_idx";
    private static final String listStrIndex = "qp_list_str_idx";
    private static final String intListBin = "int_list_bin";
    private static final String intListIndex = "qp_int_list_idx";
    private static final String nestedBin = "nested_bin";
    private static final String nestedListKey = "inner";
    private static final String nestedListTarget = "nested_target";
    private static final String nestedListIndex = "qp_nested_list_idx";
    private static final int listRangeLo = 10;
    private static final int listRangeHi = 30;
    private static final String intMapBin = "int_map_bin";
    private static final String intMapKeysIndex = "qp_int_mapkeys_idx";
    private static final String intMapValuesIndex = "qp_int_mapvalues_idx";
    private static final int mapKeyRangeLo = 10;
    private static final int mapKeyRangeHi = 30;
    private static final int mapValueRangeLo = 100;
    private static final int mapValueRangeHi = 300;
    private static final String strMapKeyRangeLo = "mkey10";
    private static final String strMapKeyRangeHi = "mkey20";
    private static final String strMapValueRangeLo = "mv10";
    private static final String strMapValueRangeHi = "mv20";
    private static final int size = 20;

    private static DataSet dataSet;
    private static byte[] listBlobBytes;

    @BeforeAll
    static void prepare() {
        assumeQuerySelection();

        dataSet = DataSet.of(args.namespace, setName);
        deleteKeys(dataSet, i -> keyPrefix + i, 1, size);

        createIndexQuietly(session, dataSet, mapIndex, mapBin, IndexType.STRING, IndexCollectionType.MAPKEYS);
        createIndexQuietly(session, dataSet, mapValuesIndex, mapBin, IndexType.STRING,
            IndexCollectionType.MAPVALUES);
        createIndexQuietly(session, dataSet, listIndex, listBin, IndexType.BLOB, IndexCollectionType.LIST);
        createIndexQuietly(session, dataSet, listStrIndex, listStrBin, IndexType.STRING,
            IndexCollectionType.LIST);
        createIndexQuietly(session, dataSet, intListIndex, intListBin, IndexType.INTEGER,
            IndexCollectionType.LIST);
        createIndexQuietly(session, dataSet, intMapKeysIndex, intMapBin, IndexType.INTEGER,
            IndexCollectionType.MAPKEYS);
        createIndexQuietly(session, dataSet, intMapValuesIndex, intMapBin, IndexType.INTEGER,
            IndexCollectionType.MAPVALUES);
        createIndexQuietly(session, dataSet, nestedListIndex, nestedBin, IndexType.STRING,
            IndexCollectionType.LIST, CTX.mapKey(Value.get(nestedListKey)));

        listBlobBytes = new byte[8];
        Buffer.longToBytes(50003, listBlobBytes, 0);

        for (int i = 1; i <= size; i++) {
            Map<String, String> map = new HashMap<>();
            map.put("mkey1", "v" + i);
            if (i % 2 == 0) {
                map.put(mapKey, mapValueTarget);
                map.put(strMapKeyRangeLo, "inRangeKey");
                map.put("mkey15", "inRangeKeyMid");
                map.put("mkey40", "outOfRangeKey");
                map.put("slotA", strMapValueRangeLo);
                map.put("slotB", "mv15");
                map.put("slotD", "mv35");
            }
            else {
                map.put("mkey05", "belowRangeKey");
            }

            // Integer keys and integer values in one bin - the planner only turns range
            // selectors into index candidates when both bounds are integers.
            Map<Integer, Integer> intMap = new HashMap<>();
            if (i % 2 == 0) {
                intMap.put(mapKeyRangeLo, mapValueRangeLo);
                intMap.put(15, 150);
                intMap.put(25, 250);
            }
            else {
                intMap.put(1, 1);
                intMap.put(2, 2);
            }

            List<Integer> intList = new ArrayList<>();
            if (i % 2 == 0) {
                intList.add(5);
                intList.add(15);
                intList.add(25);
            }
            else {
                intList.add(1);
                intList.add(2);
                intList.add(3);
            }

            List<byte[]> list = new ArrayList<>();
            if (i == 3) {
                list.add(listBlobBytes);
            }
            else {
                byte[] other = new byte[8];
                Buffer.longToBytes(50000 + i, other, 0);
                list.add(other);
            }

            List<String> strList = new ArrayList<>();
            if (i % 2 == 0) {
                strList.add(listStrTarget);
            }
            else {
                strList.add("other" + i);
            }

            Map<String, List<String>> nested = new HashMap<>();
            nested.put(nestedListKey,
                List.of(i % 2 == 0 ? nestedListTarget : "nested_other" + i));

            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(mapBin, listBin, listStrBin, intListBin, intMapBin, nestedBin)
                .values(map, list, strList, intList, intMap, nested)
                .execute();
        }
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        deleteKeys(dataSet, i -> keyPrefix + i, 1, size);
        dropIndexQuietly(dataSet, mapIndex);
        dropIndexQuietly(dataSet, mapValuesIndex);
        dropIndexQuietly(dataSet, listIndex);
        dropIndexQuietly(dataSet, listStrIndex);
        dropIndexQuietly(dataSet, intListIndex);
        dropIndexQuietly(dataSet, intMapKeysIndex);
        dropIndexQuietly(dataSet, intMapValuesIndex);
        dropIndexQuietly(dataSet, nestedListIndex);
    }

    /**
     * Bare {@code .exists()} and {@code .exists() == true} share the same collection-SI (or PI)
     * plan — the planner unwraps {@code EXP_CMP_EQ} against a boolean literal.
     */
    @ParameterizedTest
    @MethodSource("collectionExistsCases")
    void planExistsUnwrapsExpCmpEq(
        boolean eqTrue,
        String existsAel,
        String indexName,
        IndexCollectionType collectionType,
        QuerySelection selection
    ) {
        String where = existsAel + (eqTrue ? " == true" : "");
        QueryPlan plan = plan(dataSet, where);
        if (selection == QuerySelection.SECONDARY_INDEX) {
            assertPlan(plan, selection, indexName, true, collectionType);
        }
        else {
            assertPlan(plan, selection, null, false);
        }
    }

    /**
     * Integer interval selectors {@code [=lo:hi]}, {@code {@lo:hi}}, and {@code {=lo:hi}} plan as
     * collection SI candidates when both bounds are integers.
     */
    @ParameterizedTest
    @MethodSource("rangeExistsCases")
    void planRangeExistsSecondaryIndex(String where, String indexName, IndexCollectionType collectionType) {
        QueryPlan plan = plan(dataSet, where);
        assertPlan(plan, QuerySelection.SECONDARY_INDEX, indexName, true, collectionType);
    }

    /**
     * Range selectors with non-integer bounds are PI-only: the planner parses interval bounds as
     * integers, so string bounds leave the probe value unresolved. The residual filter still
     * evaluates the range, so the row set is unchanged — only the access path differs.
     */
    @Test
    void planStringBoundRangeSelectorsFallBackToPrimaryIndex() {
        String mapKeysRange = "$." + mapBin + ".{@" + strMapKeyRangeLo + ":" + strMapKeyRangeHi + "}.exists()";
        String mapValuesRange = "$." + mapBin + ".{=" + strMapValueRangeLo + ":"
            + strMapValueRangeHi + "}.exists()";
        assertAll(
            () -> assertPlan(plan(dataSet, mapKeysRange), QuerySelection.PRIMARY_INDEX, null, false),
            () -> assertPlan(plan(dataSet, mapValuesRange), QuerySelection.PRIMARY_INDEX, null, false),
            () -> assertEquals(10, countMatches(mapKeysRange, mapBin)),
            () -> assertEquals(10, countMatches(mapValuesRange, mapBin)));
    }

    /**
     * CDT range-selector E2E — server-led explain → execute without {@code forBin}.
     */
    @Test
    void executeRangeSelectorsWithoutForBinReturnsMatchingRows() {
        assertAll(
            () -> assertEquals(10, countMatching(
                "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].exists()",
                intListBin, record -> true)),
            () -> assertEquals(10, countMatching(
                "$." + intMapBin + ".{@" + mapKeyRangeLo + ":" + mapKeyRangeHi + "}.exists()",
                intMapBin, intMapRangeMatch(true, mapKeyRangeLo, mapKeyRangeHi))),
            () -> assertEquals(10, countMatching(
                "$." + intMapBin + ".{=" + mapValueRangeLo + ":" + mapValueRangeHi + "}.exists()",
                intMapBin, intMapRangeMatch(false, mapValueRangeLo, mapValueRangeHi))));
    }

    /**
     * CDT EXISTS E2E without {@code forBin} — server-led explain → execute on field {@code 44}.
     */
    @Test
    void executeCdtExistsWithoutForBinReturnsMatchingRows() {
        assertAll(
            () -> assertEquals(10, countMatching(
                "$." + mapBin + "." + mapKey + ".exists()", mapBin,
                record -> record.getMap(mapBin).containsKey(mapKey))),
            () -> assertEquals(10, countMatching(
                "$." + mapBin + ".{=" + mapValueTarget + "}.exists()", mapBin,
                record -> record.getMap(mapBin).containsValue(mapValueTarget))),
            () -> assertEquals(10, countMatching(
                "$." + listStrBin + ".[=" + listStrTarget + "].exists()", listStrBin,
                record -> record.getList(listStrBin).contains(listStrTarget))),
            () -> assertEquals(size, countMatching(
                "$." + listBin + ".[0].exists()", listBin,
                record -> record.getList(listBin).size() == 1)));
    }

    /**
     * {@code in} has its own planner opcode. Pin both the top-level LIST candidate and the nested
     * LIST candidate whose index definition includes a CDT context.
     */
    @Test
    void planInListSelectsTopLevelAndNestedListIndexes() {
        QueryPlan topLevel = plan(dataSet, "'" + listStrTarget + "' in $." + listStrBin);
        QueryPlan nested = plan(dataSet, "'" + nestedListTarget + "' in $." + nestedBin + "." + nestedListKey);

        assertAll(
            () -> assertPlan(topLevel, QuerySelection.SECONDARY_INDEX, listStrIndex, true,
                IndexCollectionType.LIST),
            () -> assertPlan(nested, QuerySelection.SECONDARY_INDEX, nestedListIndex, true,
                IndexCollectionType.LIST));
    }

    /**
     * Pin the containment boundaries implemented by the planner: positive count predicates and
     * {@code exists() == true} can drive an index; zero counts and {@code exists() == false}
     * remain residual predicates on a primary-index scan.
     */
    @Test
    void planContainmentImplicationBoundaries() {
        QueryPlan nestedExists = plan(dataSet,
            "$." + nestedBin + "." + nestedListKey + ".[=" + nestedListTarget + "].exists()");
        QueryPlan positiveCount = plan(dataSet,
            "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].count() > 0");
        QueryPlan reverseExists = plan(dataSet,
            "true == $." + mapBin + "." + mapKey + ".exists()");
        QueryPlan zeroCount = plan(dataSet,
            "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].count() == 0");
        QueryPlan falseExists = plan(dataSet, "$." + mapBin + "." + mapKey + ".exists() == false");

        assertAll(
            () -> assertPlan(nestedExists, QuerySelection.SECONDARY_INDEX, nestedListIndex, true,
                IndexCollectionType.LIST),
            () -> assertPlan(positiveCount, QuerySelection.SECONDARY_INDEX, intListIndex, true,
                IndexCollectionType.LIST),
            () -> assertPlan(reverseExists, QuerySelection.SECONDARY_INDEX, mapIndex, true,
                IndexCollectionType.MAPKEYS),
            () -> assertPlan(zeroCount, QuerySelection.PRIMARY_INDEX, null, false),
            () -> assertPlan(falseExists, QuerySelection.PRIMARY_INDEX, null, false));
    }

    /**
     * Golden rows for the new candidate families. SI and PI cases deliberately share the same
     * parity fixture, proving that candidate extraction changes access paths without changing
     * residual-filter semantics.
     */
    @Test
    void executeInNestedAndCountPredicatesReturnGoldenRows() {
        assertAll(
            () -> assertEquals(10, countMatches("'" + listStrTarget + "' in $." + listStrBin, listStrBin)),
            () -> assertEquals(10, countMatches(
                "'" + nestedListTarget + "' in $." + nestedBin + "." + nestedListKey, nestedBin)),
            () -> assertEquals(10, countMatches(
                "$." + nestedBin + "." + nestedListKey + ".[=" + nestedListTarget + "].exists()",
                nestedBin)),
            () -> assertEquals(10, countMatches(
                "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].count() > 0",
                intListBin)),
            () -> assertEquals(10, countMatches(
                "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].count() == 0",
                intListBin)),
            () -> assertEquals(10, countMatches(
                "$." + mapBin + "." + mapKey + ".exists() == false", mapBin)));
    }

    private static int countMatching(String where, String binName, Predicate<Record> check) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute();
        try (rs) {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                assertTrue(check.test(record));
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
    }

    /** AEL interval selectors are begin-inclusive, end-exclusive. */
    private static Predicate<Record> intMapRangeMatch(boolean onKeys, int lo, int hi) {
        return record -> {
            Map<?, ?> map = record.getMap(intMapBin);
            Collection<?> probed = onKeys ? map.keySet() : map.values();
            return probed.stream().anyMatch(entry -> {
                long value = ((Number) entry).longValue();
                return value >= lo && value < hi;
            });
        };
    }

    private static Stream<Arguments> collectionExistsCases() {
        return Stream.of(false, true).flatMap(eqTrue -> Stream.of(
            Arguments.of(eqTrue, "$." + mapBin + "." + mapKey + ".exists()",
                mapIndex, IndexCollectionType.MAPKEYS, QuerySelection.SECONDARY_INDEX),
            Arguments.of(eqTrue, "$." + mapBin + ".{=" + mapValueTarget + "}.exists()",
                mapValuesIndex, IndexCollectionType.MAPVALUES, QuerySelection.SECONDARY_INDEX),
            Arguments.of(eqTrue, "$." + listStrBin + ".[=" + listStrTarget + "].exists()",
                listStrIndex, IndexCollectionType.LIST, QuerySelection.SECONDARY_INDEX),
            Arguments.of(eqTrue, "$." + listBin + ".[0].exists()",
                null, null, QuerySelection.PRIMARY_INDEX)
        ));
    }

    private static Stream<Arguments> rangeExistsCases() {
        return Stream.of(
            Arguments.of(
                "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].exists()",
                intListIndex, IndexCollectionType.LIST),
            Arguments.of(
                "$." + intMapBin + ".{@" + mapKeyRangeLo + ":" + mapKeyRangeHi + "}.exists()",
                intMapKeysIndex, IndexCollectionType.MAPKEYS),
            Arguments.of(
                "$." + intMapBin + ".{=" + mapValueRangeLo + ":" + mapValueRangeHi + "}.exists()",
                intMapValuesIndex, IndexCollectionType.MAPVALUES)
        );
    }

    private static int countMatches(String where, String binName) {
        return QuerySelectionIntegSupport.countRecords(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute());
    }
}
