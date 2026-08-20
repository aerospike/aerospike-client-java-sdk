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

import static com.aerospike.client.sdk.query.QueryPlannerSupport.assertPlan;
import static com.aerospike.client.sdk.query.QueryPlannerSupport.assumeQuerySelection;
import static com.aerospike.client.sdk.query.QueryPlannerSupport.deleteKeys;
import static com.aerospike.client.sdk.query.QueryPlannerSupport.dropIndexQuietly;
import static com.aerospike.client.sdk.query.QueryPlannerSupport.plan;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
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
 *
 * <p>Uses server AEL syntax ({@code .exists()}, not legacy {@code .get(return: EXISTS)}).
 * List <strong>index</strong> paths use {@code [0]}; list <strong>value</strong> paths use
 * {@code [=name]} (STRING). BLOB list value selectors ({@code [=X'…']}) are not in server
 * AEL {@code key_val} grammar today.</p>
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
    public static void prepare() {
        assumeQuerySelection();

        dataSet = DataSet.of(args.namespace, setName);
        deleteKeys(dataSet, i -> keyPrefix + i, 1, size);

        createCollectionIndex(mapIndex, mapBin, IndexType.STRING, IndexCollectionType.MAPKEYS);
        createCollectionIndex(mapValuesIndex, mapBin, IndexType.STRING, IndexCollectionType.MAPVALUES);
        createCollectionIndex(listIndex, listBin, IndexType.BLOB, IndexCollectionType.LIST);
        createCollectionIndex(listStrIndex, listStrBin, IndexType.STRING, IndexCollectionType.LIST);
        createCollectionIndex(intListIndex, intListBin, IndexType.INTEGER, IndexCollectionType.LIST);
        createCollectionIndex(intMapKeysIndex, intMapBin, IndexType.INTEGER, IndexCollectionType.MAPKEYS);
        createCollectionIndex(intMapValuesIndex, intMapBin, IndexType.INTEGER,
            IndexCollectionType.MAPVALUES);
        createCtxCollectionIndex(nestedListIndex, nestedBin, IndexType.STRING,
            IndexCollectionType.LIST, CTX.mapKey(Value.get(nestedListKey)));

        listBlobBytes = new byte[8];
        Buffer.longToBytes(50003, listBlobBytes, 0);

        for (int i = 1; i <= size; i++) {
            HashMap<String, String> map = new HashMap<>();
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
            HashMap<Integer, Integer> intMap = new HashMap<>();
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

            HashMap<String, List<String>> nested = new HashMap<>();
            nested.put(nestedListKey,
                List.of(i % 2 == 0 ? nestedListTarget : "nested_other" + i));

            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(mapBin, listBin, listStrBin, intListBin, intMapBin, nestedBin)
                .values(map, list, strList, intList, intMap, nested)
                .execute();
        }
    }

    @AfterAll
    public static void destroy() {
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
     * MAPKEYS + bare CDT EXISTS — field {@code 44} explain selects the MAPKEYS secondary index.
     */
    @Test
    void planMapKeysExistsSecondaryIndex() {
        QueryPlan plan = plan(dataSet, mapKeysExistsWhere());
        assertAll("mapKeysSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(mapIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPKEYS, plan.getIndexType()));
    }

    /**
     * MAPKEYS + {@code .exists() == true} — wrapped {@code EXP_CMP_EQ} still selects MAPKEYS SI.
     */
    @Test
    void planMapKeysExistsEqTrueSecondaryIndex() {
        QueryPlan plan = plan(dataSet, mapKeysExistsWhere() + " == true");
        assertAll("mapKeysEqTrueSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(mapIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPKEYS, plan.getIndexType()));
    }

    /**
     * MAPVALUES + bare CDT EXISTS — field {@code 44} explain selects the MAPVALUES secondary index.
     */
    @Test
    void planMapValuesExistsSecondaryIndex() {
        QueryPlan plan = plan(dataSet, mapValuesExistsWhere());
        assertAll("mapValuesSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(mapValuesIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPVALUES, plan.getIndexType()));
    }

    /**
     * MAPVALUES + {@code .exists() == true} — wrapped {@code EXP_CMP_EQ} still selects MAPVALUES SI.
     */
    @Test
    void planMapValuesExistsEqTrueSecondaryIndex() {
        QueryPlan plan = plan(dataSet, mapValuesExistsWhere() + " == true");
        assertAll("mapValuesEqTrueSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(mapValuesIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPVALUES, plan.getIndexType()));
    }

    /**
     * LIST + value-path bare EXISTS — field {@code 44} explain selects the LIST secondary index.
     */
    @Test
    void planListValueExistsSecondaryIndex() {
        QueryPlan plan = plan(dataSet, listValueExistsWhere());
        assertAll("listValueSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(listStrIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.LIST, plan.getIndexType()));
    }

    /**
     * LIST value path {@code .exists() == true} — wrapped {@code EXP_CMP_EQ} still selects LIST SI.
     */
    @Test
    void planListValueExistsEqTrueSecondaryIndex() {
        QueryPlan plan = plan(dataSet, listValueExistsWhere() + " == true");
        assertAll("listValueEqTrueSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(listStrIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.LIST, plan.getIndexType()));
    }

    /**
     * LIST + index-path EXISTS — field {@code 44} explain PI-fallback ({@code LIST|INDEX} selector
     * does not map to {@code AS_SINDEX_ITYPE_LIST} in {@code itype_from_selector}).
     */
    @Test
    void planListIndexExistsPrimaryIndexFallback() {
        assertPlan(plan(dataSet, listIndexExistsWhere()), QuerySelection.PRIMARY_INDEX, null, false);
    }

    /**
     * LIST + index-path {@code .exists() == true} — still PI (same selector limitation).
     */
    @Test
    void planListIndexExistsEqTruePrimaryIndexFallback() {
        assertPlan(plan(dataSet, listIndexExistsWhere() + " == true"),
            QuerySelection.PRIMARY_INDEX, null, false);
    }

    /**
     * LIST + integer interval selector {@code [=lo:hi].exists()} — field {@code 44} explain selects LIST SI.
     */
    @Test
    void planListValueRangeExistsSecondaryIndex() {
        QueryPlan plan = plan(dataSet, listValueRangeExistsWhere());
        assertAll("listValueRangeSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(intListIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.LIST, plan.getIndexType()));
    }

    /**
     * MAPKEYS + integer key interval selector {@code {@lo:hi}.exists()} — field {@code 44} explain
     * selects MAPKEYS SI.
     */
    @Test
    void planMapKeysRangeExistsSecondaryIndex() {
        QueryPlan plan = plan(dataSet, mapKeysRangeExistsWhere());
        assertAll("mapKeysRangeSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(intMapKeysIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPKEYS, plan.getIndexType()));
    }

    /**
     * MAPVALUES + integer value interval selector {@code {=lo:hi}.exists()} — field {@code 44}
     * explain selects MAPVALUES SI.
     */
    @Test
    void planMapValuesRangeExistsSecondaryIndex() {
        QueryPlan plan = plan(dataSet, mapValuesRangeExistsWhere());
        assertAll("mapValuesRangeSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(intMapValuesIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPVALUES, plan.getIndexType()));
    }

    /**
     * Range selectors with non-integer bounds are PI-only: the planner parses interval bounds as
     * integers, so string bounds leave the probe value unresolved. The residual filter still
     * evaluates the range, so the row set is unchanged — only the access path differs.
     */
    @Test
    void planStringBoundRangeSelectorsFallBackToPrimaryIndex() {
        assertAll("stringBoundRanges",
            () -> assertPlan(plan(dataSet, stringMapKeysRangeExistsWhere()),
                QuerySelection.PRIMARY_INDEX, null, false),
            () -> assertPlan(plan(dataSet, stringMapValuesRangeExistsWhere()),
                QuerySelection.PRIMARY_INDEX, null, false),
            () -> assertEquals(10, countMatches(stringMapKeysRangeExistsWhere(), mapBin)),
            () -> assertEquals(10, countMatches(stringMapValuesRangeExistsWhere(), mapBin)));
    }

    /**
     * CDT range-selector E2E — server-led explain → execute without {@code forBin}.
     */
    @Test
    void executeRangeSelectorsWithoutForBinReturnsMatchingRows() {
        assertAll("rangeSelectorExecute",
            () -> assertEquals(10, countRangeSelectorMatches(listValueRangeExistsWhere(), intListBin)),
            () -> assertEquals(10, countIntMapKeyRangeMatches(mapKeysRangeExistsWhere())),
            () -> assertEquals(10, countIntMapValueRangeMatches(mapValuesRangeExistsWhere())));
    }

    /**
     * CDT EXISTS E2E without {@code forBin} — server-led explain → execute on field {@code 44}.
     */
    @Test
    void executeCdtExistsWithoutForBinReturnsMatchingRows() {
        assertAll("cdtExistsExecute",
            () -> assertEquals(10, countMapKeyMatches(mapKeysExistsWhere())),
            () -> assertEquals(10, countMapValueMatches(mapValuesExistsWhere())),
            () -> assertEquals(10, countListValueMatches(listValueExistsWhere())),
            () -> assertEquals(size, countListIndexExistsMatches(listIndexExistsWhere())));
    }

    /**
     * {@code in} has its own planner opcode. Pin both the top-level LIST candidate and the nested
     * LIST candidate whose index definition includes a CDT context.
     */
    @Test
    void planInListSelectsTopLevelAndNestedListIndexes() {
        QueryPlan topLevel = plan(dataSet, topLevelInWhere());
        QueryPlan nested = plan(dataSet, nestedInWhere());

        assertAll("inListPlans",
            () -> assertCollectionPlan(topLevel, listStrIndex, IndexCollectionType.LIST),
            () -> assertCollectionPlan(nested, nestedListIndex, IndexCollectionType.LIST));
    }

    /**
     * Pin the containment boundaries implemented by the planner: positive count predicates and
     * {@code exists() == true} can drive an index; zero counts and {@code exists() == false}
     * remain residual predicates on a primary-index scan.
     */
    @Test
    void planContainmentImplicationBoundaries() {
        QueryPlan nestedExists = plan(dataSet, nestedExistsWhere());
        QueryPlan positiveCount = plan(dataSet, intListCountWhere("> 0"));
        QueryPlan reverseExists = plan(dataSet, "true == " + mapKeysExistsWhere());
        QueryPlan zeroCount = plan(dataSet, intListCountWhere("== 0"));
        QueryPlan falseExists = plan(dataSet, mapKeysExistsWhere() + " == false");

        assertAll("containmentBoundaries",
            () -> assertCollectionPlan(nestedExists, nestedListIndex, IndexCollectionType.LIST),
            () -> assertCollectionPlan(positiveCount, intListIndex, IndexCollectionType.LIST),
            () -> assertCollectionPlan(reverseExists, mapIndex, IndexCollectionType.MAPKEYS),
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
        assertAll("newCandidateRows",
            () -> assertEquals(10, countMatches(topLevelInWhere(), listStrBin)),
            () -> assertEquals(10, countMatches(nestedInWhere(), nestedBin)),
            () -> assertEquals(10, countMatches(nestedExistsWhere(), nestedBin)),
            () -> assertEquals(10, countMatches(intListCountWhere("> 0"), intListBin)),
            () -> assertEquals(10, countMatches(intListCountWhere("== 0"), intListBin)),
            () -> assertEquals(10, countMatches(mapKeysExistsWhere() + " == false", mapBin)));
    }

    private static String mapKeysExistsWhere() {
        return "$." + mapBin + "." + mapKey + ".exists()";
    }

    private static String mapValuesExistsWhere() {
        return "$." + mapBin + ".{=" + mapValueTarget + "}.exists()";
    }

    private static String listIndexExistsWhere() {
        return "$." + listBin + ".[0].exists()";
    }

    private static String listValueExistsWhere() {
        return "$." + listStrBin + ".[=" + listStrTarget + "].exists()";
    }

    private static String listValueRangeExistsWhere() {
        return "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].exists()";
    }

    private static String topLevelInWhere() {
        return "'" + listStrTarget + "' in $." + listStrBin;
    }

    private static String nestedInWhere() {
        return "'" + nestedListTarget + "' in $." + nestedBin + "." + nestedListKey;
    }

    private static String nestedExistsWhere() {
        return "$." + nestedBin + "." + nestedListKey + ".[=" + nestedListTarget + "].exists()";
    }

    /**
     * {@code count()} needs a plural selector. A singular by-value selector ({@code [=v]}) is a
     * scalar leaf, and {@code count()} on it is {@code OP_NOT_APPLICABLE} at execute time, so the
     * interval form is the one that carries both a planner candidate and a usable predicate.
     */
    private static String intListCountWhere(String comparison) {
        return "$." + intListBin + ".[=" + listRangeLo + ":" + listRangeHi + "].count() " + comparison;
    }

    private static String mapKeysRangeExistsWhere() {
        return "$." + intMapBin + ".{@" + mapKeyRangeLo + ":" + mapKeyRangeHi + "}.exists()";
    }

    private static String mapValuesRangeExistsWhere() {
        return "$." + intMapBin + ".{=" + mapValueRangeLo + ":" + mapValueRangeHi + "}.exists()";
    }

    private static String stringMapKeysRangeExistsWhere() {
        return "$." + mapBin + ".{@" + strMapKeyRangeLo + ":" + strMapKeyRangeHi + "}.exists()";
    }

    private static String stringMapValuesRangeExistsWhere() {
        return "$." + mapBin + ".{=" + strMapValueRangeLo + ":" + strMapValueRangeHi + "}.exists()";
    }

    private static int countRangeSelectorMatches(String where, String binName) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute();
        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next().recordOrThrow();
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
        finally {
            rs.close();
        }
    }

    private static int countIntMapKeyRangeMatches(String where) {
        return countIntMapRangeMatches(where, true, mapKeyRangeLo, mapKeyRangeHi);
    }

    private static int countIntMapValueRangeMatches(String where) {
        return countIntMapRangeMatches(where, false, mapValueRangeLo, mapValueRangeHi);
    }

    /** AEL interval selectors are begin-inclusive, end-exclusive. */
    private static int countIntMapRangeMatches(String where, boolean onKeys, int lo, int hi) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(intMapBin)
            .where(where)
            .execute();
        try {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                Map<?, ?> map = record.getMap(intMapBin);
                Collection<?> probed = onKeys ? map.keySet() : map.values();
                assertTrue(probed.stream().anyMatch(entry -> {
                    long value = ((Number) entry).longValue();
                    return value >= lo && value < hi;
                }), "expected " + (onKeys ? "key" : "value") + " in [" + lo + "," + hi + ") in " + map);
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
        finally {
            rs.close();
        }
    }

    private static void createCollectionIndex(
        String indexName,
        String binName,
        IndexType indexType,
        IndexCollectionType collectionType
    ) {
        try {
            session.createIndex(dataSet, indexName, binName, indexType, collectionType)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    private static void createCtxCollectionIndex(
        String indexName,
        String binName,
        IndexType indexType,
        IndexCollectionType collectionType,
        CTX... ctx
    ) {
        try {
            session.createIndex(dataSet, indexName, binName, indexType, collectionType, ctx)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    private static void assertCollectionPlan(
        QueryPlan plan,
        String indexName,
        IndexCollectionType collectionType
    ) {
        assertAll("collectionPlan",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(collectionType, plan.getIndexType()));
    }

    private static int countMatches(String where, String binName) {
        return QuerySelectionIntegSupport.countRecords(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute());
    }

    private static int countMapKeyMatches(String where) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(mapBin)
            .where(where)
            .execute();
        try {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                Map<?, ?> map = record.getMap(mapBin);
                assertTrue(map.containsKey(mapKey),
                    "expected map key " + mapKey + " in " + map);
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
        finally {
            rs.close();
        }
    }

    private static int countMapValueMatches(String where) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(mapBin)
            .where(where)
            .execute();
        try {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                Map<?, ?> map = record.getMap(mapBin);
                assertTrue(map.containsValue(mapValueTarget),
                    "expected map value " + mapValueTarget + " in " + map);
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
        finally {
            rs.close();
        }
    }

    private static int countListValueMatches(String where) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(listStrBin)
            .where(where)
            .execute();
        try {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                List<?> list = record.getList(listStrBin);
                assertTrue(list.contains(listStrTarget),
                    "expected list value " + listStrTarget + " in " + list);
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
        finally {
            rs.close();
        }
    }

    private static int countListIndexExistsMatches(String where) {
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(listBin)
            .where(where)
            .execute();
        try {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                List<?> list = record.getList(listBin);
                assertEquals(1, list.size());
                count++;
            }
            assertNotEquals(0, count);
            return count;
        }
        finally {
            rs.close();
        }
    }
}
