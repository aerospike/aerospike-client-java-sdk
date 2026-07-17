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
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Query planner integration tests for MAPKEYS / LIST collection indexes with CDT
 * {@code .exists()} predicates on field {@code 44} explain.
 *
 * <p>Uses server AEL syntax ({@code .exists()}, not legacy {@code .get(return: EXISTS)}).
 * List index paths use {@code [0]} (index segment); server {@code [=X'blob']} value selectors
 * are not valid in server AEL today ({@code key_val} rejects blob literals).</p>
 */
public class QueryPlannerCollectionCdtTest extends ClusterTest {
    private static final String setName = "qp_cdt";
    private static final String keyPrefix = "qpcdt";
    private static final String mapBin = "map_bin";
    private static final String listBin = "list_bin";
    private static final String mapKey = "mkey2";
    private static final String mapIndex = "qp_mapkeys_idx";
    private static final String listIndex = "qp_list_idx";
    private static final int size = 20;

    private static DataSet dataSet;
    private static byte[] listBlobBytes;

    @BeforeAll
    public static void prepare() {
        assumeQuerySelection();

        dataSet = DataSet.of(args.namespace, setName);
        deleteKeys(dataSet, i -> keyPrefix + i, 1, size);

        createCollectionIndex(mapIndex, mapBin, IndexType.STRING, IndexCollectionType.MAPKEYS);
        createCollectionIndex(listIndex, listBin, IndexType.BLOB, IndexCollectionType.LIST);

        listBlobBytes = new byte[8];
        Buffer.longToBytes(50003, listBlobBytes, 0);

        for (int i = 1; i <= size; i++) {
            HashMap<String, String> map = new HashMap<>();
            map.put("mkey1", "v" + i);
            if (i % 2 == 0) {
                map.put(mapKey, "v" + i);
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

            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(mapBin, listBin)
                .values(map, list)
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
        dropIndexQuietly(dataSet, listIndex);
    }

    /**
     * MAPKEYS + CDT EXISTS — field {@code 44} explain should PI-fallback (walker does not
     * emit a MAPKEYS SI candidate for {@code .exists()} paths today).
     */
    @Test
    void planMapKeysExistsPrimaryIndexFallback() {
        String where = "$." + mapBin + "." + mapKey + ".exists() == true";

        assertPlan(plan(dataSet, where), QuerySelection.PRIMARY_INDEX, null, false);
    }

    /**
     * LIST + CDT EXISTS — field {@code 44} explain should PI-fallback (walker does not
     * emit a LIST SI candidate for index-path {@code .exists()} today).
     *
     * <p>Uses {@code [0]} index segment; {@code [=X'blob']} is client-only syntax and fails
     * server AEL parse ({@code syntax error @ 13}).</p>
     */
    @Test
    void planListExistsPrimaryIndexFallback() {
        String where = "$." + listBin + ".[0].exists() == true";

        assertPlan(plan(dataSet, where), QuerySelection.PRIMARY_INDEX, null, false);
    }

    /**
     * CDT EXISTS E2E without {@code forBin} — server-led explain → execute on field {@code 44}.
     */
    @Test
    void executeCdtExistsWithoutForBinReturnsMatchingRows() {
        String mapWhere = "$." + mapBin + "." + mapKey + ".exists() == true";
        String listWhere = "$." + listBin + ".[0].exists() == true";

        assertAll("cdtExistsExecute",
            () -> assertEquals(10, countMapKeyMatches(mapWhere)),
            () -> assertEquals(size, countListIndexExistsMatches(listWhere)));
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
