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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
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
 * Documents <strong>server</strong> field {@code 44} explain behavior across index shapes — pins
 * observed planner outcomes for product/server discussion.
 *
 * <p>The client always attempts explain for string AEL when query selection is enabled. It does
 * <strong>not</strong> parse AEL locally to pre-route. Explain failures propagate (no legacy
 * fallback on {@code execute()}).</p>
 *
 * <p><strong>Expected server behavior (integration branch, per {@code query_plan.c} /
 * {@code exp.c}):</strong></p>
 * <ul>
 *   <li>Scalar INTEGER / STRING / BLOB equality → SI explain when a matching index exists</li>
 *   <li>STRING on bin without SI → PI explain (OK, no index fields)</li>
 *   <li>MAPKEYS + CDT {@code .exists()} (no SI candidate in walker today) → PI fallback</li>
 * </ul>
 *
 * <p>Field {@code 44} uses <strong>server AEL</strong> ({@code .exists()}). Legacy client syntax
 * {@code .get(return: EXISTS)} fails explain with {@code PARAMETER} — see
 * {@link QueryPlannerCollectionCdtTest} for the supported shapes.</p>
 */
class QuerySelectionExplainScopeTest extends ClusterTest {
    private static final String setName = "qscexp";
    private static final String intIndexName = "qscexp_age_idx";
    private static final String ageBin = "age";
    private static final String countryBin = "country";
    private static final String blobBin = "bb";
    private static final String blobIndexName = "qscexp_bb_idx";
    private static final String mapBin = "map_bin";
    private static final String mapIndexName = "qscexp_map_idx";
    private static final String mapKey = "mkey2";

    private static DataSet dataSet;
    private static String blobHex;

    @BeforeAll
    static void prepare() {
        assumeTrue(cluster.supportsQuerySelection(), "server does not support query selection");

        dataSet = DataSet.of(args.namespace, setName);

        session.delete(dataSet.ids("k1"));
        session.delete(dataSet.ids("k2"));

        try {
            session.createIndex(dataSet, intIndexName, ageBin, IndexType.INTEGER,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        try {
            session.createIndex(dataSet, blobIndexName, blobBin, IndexType.BLOB,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        try {
            session.createIndex(dataSet, mapIndexName, mapBin, IndexType.STRING,
                IndexCollectionType.MAPKEYS).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        byte[] blobBytes = new byte[8];
        Buffer.longToBytes(50001, blobBytes, 0);
        blobHex = Buffer.bytesToHexString(blobBytes);

        HashMap<String, String> map = new HashMap<>();
        map.put(mapKey, "v1");

        session.upsert(dataSet.ids("k1"))
            .bins(ageBin, countryBin, blobBin, mapBin)
            .values(25, "US", blobBytes, map)
            .execute();

        session.upsert(dataSet.ids("k2"))
            .bins(ageBin, countryBin)
            .values(30, "CA")
            .execute();
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        session.delete(dataSet.ids("k1"));
        session.delete(dataSet.ids("k2"));
        session.dropIndex(dataSet, intIndexName);
        session.dropIndex(dataSet, blobIndexName);
        session.dropIndex(dataSet, mapIndexName);
    }

    @Test
    void explainScalarIntegerSecondaryIndex_succeeds() {
        QueryPlan plan = explain("$.age == 25");

        assertAll("integerSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(intIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()));
    }

    @Test
    void explainScalarStringPrimaryIndex_noIndexFields() {
        QueryPlan plan = explain("$.country == 'US'");

        assertAll("stringPiExplain",
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * BLOB scalar equality — server selects the BLOB secondary index on explain.
     */
    @Test
    void explainBlobEquality_selectsSecondaryIndex() {
        String where = "$." + blobBin + " == x'" + blobHex + "'";
        QueryPlan plan = explain(where);

        assertAll("blobSiExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(blobIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * MAPKEYS + CDT EXISTS — no SI candidate in server walker today → PI fallback on explain.
     * Uses server AEL {@code .exists()}, not legacy {@code .get(return: EXISTS)}.
     */
    @Test
    void explainMapKeysExists_primaryIndexFallback() {
        String where = "$." + mapBin + "." + mapKey + ".exists() == true";
        QueryPlan plan = explain(where);

        assertAll("mapKeysPiExplain",
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * E2E: BLOB query uses field {@code 44} explain → execute (SI plan when explain succeeds).
     */
    @Test
    void executeBlobEquality_returnsMatchingRow() {
        String where = "$." + blobBin + " == x'" + blobHex + "'";

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(blobBin)
            .where(where)
            .execute());

        assertEquals(1, count);
    }

    /**
     * E2E: MAPKEYS EXISTS uses field {@code 44} explain → execute (PI plan when no SI candidate).
     */
    @Test
    void executeMapKeysExists_returnsMatchingRows() {
        String where = "$." + mapBin + "." + mapKey + ".exists() == true";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(mapBin)
            .where(where)
            .execute();

        try {
            int count = 0;
            while (rs.hasNext()) {
                Record record = rs.next().recordOrThrow();
                Map<?, ?> result = record.getMap(mapBin);
                if (!result.containsKey(mapKey)) {
                    throw new AssertionError("expected map key " + mapKey + " in " + result);
                }
                count++;
            }
            assertNotEquals(0, count);
        }
        finally {
            rs.close();
        }
    }

    private static QueryPlan explain(String where) {
        return IndexProbePlanner.plan(
            session, dataSet, WhereClauseProcessor.from(where), null);
    }

    private static int countRecords(RecordStream rs) {
        try {
            int count = 0;
            while (rs.hasNext()) {
                rs.next().recordOrThrow();
                count++;
            }
            return count;
        }
        finally {
            rs.close();
        }
    }
}
