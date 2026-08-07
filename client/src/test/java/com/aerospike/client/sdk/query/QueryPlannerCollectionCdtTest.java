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
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Query planner integration tests for MAPKEYS / MAPVALUES / LIST collection indexes with CDT
 * {@code .exists()} predicates on field {@code 44} explain.
 *
 * <p>Bare {@code .exists()} and {@code .exists() == true} both plan as collection SI candidates
 * when the selector carries a probe value (MAPKEYS key, MAPVALUES value, LIST value path).
 * LIST index paths ({@code [0]}) still fall back to PI — value-containment indexes cannot
 * answer positional existence.</p>
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

        listBlobBytes = new byte[8];
        Buffer.longToBytes(50003, listBlobBytes, 0);

        for (int i = 1; i <= size; i++) {
            HashMap<String, String> map = new HashMap<>();
            map.put("mkey1", "v" + i);
            if (i % 2 == 0) {
                map.put(mapKey, mapValueTarget);
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

            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(mapBin, listBin, listStrBin)
                .values(map, list, strList)
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
