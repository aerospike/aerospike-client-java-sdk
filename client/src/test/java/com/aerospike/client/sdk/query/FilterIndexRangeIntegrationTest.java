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

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assertIndexRangeRoundTrips;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assertPlan;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assumeQuerySelection;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.countRecords;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.dropIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.plan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Integration tests for server {@code INDEX_RANGE} replay through {@link Filter#fromWireRange}.
 *
 * <p>Each scenario explains a secondary-index plan, round-trips the opaque range bytes via
 * {@link Filter#write}, then executes the query and asserts golden row sets.</p>
 */
public class FilterIndexRangeIntegrationTest extends ClusterTest {
    private static final String setName = "filterir";
    private static final String ageBin = "fir_age";
    private static final String ageIndex = "filterir_age_idx";
    private static final String tagBin = "fir_tag";
    private static final String tagIndex = "filterir_tag_idx";
    private static final String tagMatch = "featured";
    private static final String mapBin = "fir_map";
    private static final String mapIndex = "filterir_map_idx";
    private static final String mapKey = "fir_mkey";
    private static final String locBin = "fir_loc";
    private static final String geoIndex = "filterir_loc_idx";
    private static final String blobBin = "fir_blob";
    private static final String blobIndex = "filterir_blob_idx";
    private static final double matchLng = -122.0986857;
    private static final double matchLat = 37.4214209;
    private static final String matchPointGeoJson =
        "{\"type\":\"Point\",\"coordinates\":[" + matchLng + "," + matchLat + "]}";

    private static DataSet dataSet;
    private static String blobHex;

    @BeforeAll
    static void prepare() {
        assumeQuerySelection();

        dataSet = DataSet.of(args.namespace, setName);
        session.delete(dataSet.ids("k1")).execute();
        session.delete(dataSet.ids("k2")).execute();

        createIndexQuietly(session, dataSet, ageIndex, ageBin, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, tagIndex, tagBin, IndexType.STRING,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, mapIndex, mapBin, IndexType.STRING,
            IndexCollectionType.MAPKEYS);
        createIndexQuietly(session, dataSet, geoIndex, locBin, IndexType.GEO2DSPHERE,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, blobIndex, blobBin, IndexType.BLOB,
            IndexCollectionType.DEFAULT);

        byte[] blobBytes = new byte[8];
        Buffer.longToBytes(77_001, blobBytes, 0);
        blobHex = Buffer.bytesToHexString(blobBytes);

        HashMap<String, String> mapWithKey = new HashMap<>();
        mapWithKey.put(mapKey, "v1");

        String k1Loc = "{ \"type\": \"AeroCircle\", \"coordinates\": [[" +
            matchLng + ", " + matchLat + "], 3000.0 ] }";
        String k2Loc = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-121.0, 38.0], 3000.0 ] }";

        session.upsert(dataSet.ids("k1"))
            .bins(ageBin, tagBin, mapBin, blobBin)
            .values(25, tagMatch, mapWithKey, blobBytes)
            .execute();
        session.upsert(dataSet.ids("k1"))
            .bin(locBin).setToGeoJson(k1Loc)
            .execute();

        session.upsert(dataSet.ids("k2"))
            .bins(ageBin, tagBin, mapBin)
            .values(30, "ordinary", new HashMap<String, String>())
            .execute();
        session.upsert(dataSet.ids("k2"))
            .bin(locBin).setToGeoJson(k2Loc)
            .execute();
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        session.delete(dataSet.ids("k1")).execute();
        session.delete(dataSet.ids("k2")).execute();
        dropIndexQuietly(session, dataSet, ageIndex);
        dropIndexQuietly(session, dataSet, tagIndex);
        dropIndexQuietly(session, dataSet, mapIndex);
        dropIndexQuietly(session, dataSet, geoIndex);
        dropIndexQuietly(session, dataSet, blobIndex);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexRangeCases")
    void indexRangeRoundTripsAndReturnsExpectedRows(
        String label,
        String where,
        String expectedIndex,
        IndexCollectionType expectedCollectionType,
        String readBin,
        List<Integer> expectedRows
    ) {
        QueryPlan queryPlan = plan(dataSet, where);

        assertPlan(queryPlan, QuerySelection.SECONDARY_INDEX, expectedIndex, true,
            expectedCollectionType);
        assertIndexRangeRoundTrips(queryPlan);

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(readBin)
            .where(where)
            .limit(5)
            .execute();

        if (expectedRows != null) {
            assertEquals(expectedRows, collectAges(rs, readBin), label);
            return;
        }

        if (mapBin.equals(readBin)) {
            try (rs) {
                int count = 0;
                while (rs.hasNext()) {
                    Map<?, ?> result = rs.next().recordOrThrow().getMap(mapBin);
                    assertTrue(result.containsKey(mapKey),
                        () -> "expected map key " + mapKey + " in " + result);
                    count++;
                }
                assertEquals(1, count, label);
            }
            return;
        }

        assertEquals(1, countRecords(rs), label);
    }

    static Stream<Arguments> indexRangeCases() {
        return Stream.of(
            Arguments.of(
                "integer-eq",
                "$." + ageBin + " == 25",
                ageIndex,
                IndexCollectionType.DEFAULT,
                ageBin,
                List.of(25)),
            Arguments.of(
                "integer-range",
                "$." + ageBin + " >= 24 and $." + ageBin + " <= 26",
                ageIndex,
                IndexCollectionType.DEFAULT,
                ageBin,
                List.of(25)),
            Arguments.of(
                "string-eq",
                "$." + tagBin + " == '" + tagMatch + "'",
                tagIndex,
                IndexCollectionType.DEFAULT,
                tagBin,
                null),
            Arguments.of(
                "geo-region",
                "geoCompare($." + locBin + ", geoJson('" + matchPointGeoJson + "'))",
                geoIndex,
                IndexCollectionType.DEFAULT,
                locBin,
                null),
            Arguments.of(
                "mapkeys-exists",
                "$." + mapBin + "." + mapKey + ".exists()",
                mapIndex,
                IndexCollectionType.MAPKEYS,
                mapBin,
                null),
            Arguments.of(
                "blob-eq",
                "$." + blobBin + " == x'" + blobHex + "'",
                blobIndex,
                IndexCollectionType.DEFAULT,
                blobBin,
                null));
    }
}
