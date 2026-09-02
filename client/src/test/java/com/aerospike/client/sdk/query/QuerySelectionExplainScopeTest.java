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

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assumeQuerySelection;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createExpIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.countRecords;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.dropIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.plan;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.StringExp;
import com.aerospike.client.sdk.operation.StringWriteFlags;
import com.aerospike.client.sdk.query.plan.IndexRangeWire;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.util.Version;

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
 *   <li>Scalar INTEGER / STRING / BLOB / GEO → SI explain when a matching index exists</li>
 *   <li>STRING on bin without SI → PI explain (OK, no index fields)</li>
 *   <li>MAPKEYS / MAPVALUES / LIST-value {@code .exists()} (bare or {@code == true}) → SI</li>
 *   <li>Bare or wrapped {@code geoCompare(...)} → GEO SI when index exists</li>
 *   <li>Region literals above the {@code 2048}-byte STRING/BLOB bound → GEO SI (geo bounds are
 *       capped at the 1 MiB GeoJSON wire limit instead)</li>
 *   <li>Ctx-path scalar ({@code $.bin.[N]}) and ctx-path geo ({@code $.map.key}) → DEFAULT SI</li>
 *   <li>Expression sindexes over arithmetic ({@code $.age + 1}) → SI with {@code bin_name_len == 0};
 *       AEL string path functions ({@code $.name.upper()}) → PI</li>
 *   <li>LIST index path {@code [N].exists()} → PI (positional existence; see
 *       {@link QueryPlannerCollectionCdtTest})</li>
 * </ul>
 *
 * <p>Field {@code 44} uses <strong>server AEL</strong> ({@code .exists()}). Legacy client syntax
 * {@code .get(return: EXISTS)} fails explain with {@code PARAMETER} — see
 * {@link QueryPlannerCollectionCdtTest} for the supported shapes.</p>
 */
public class QuerySelectionExplainScopeTest extends ClusterTest {
    private static final String setName = "qscexp";
    private static final String intIndexName = "qscexp_age_idx";
    private static final String ageBin = "age";
    private static final String countryBin = "country";
    private static final String tagBin = "tag";
    private static final String tagIndexName = "qscexp_tag_idx";
    private static final String tagMatch = "featured";
    private static final String blobBin = "bb";
    private static final String blobIndexName = "qscexp_bb_idx";
    private static final String mapBin = "map_bin";
    private static final String mapIndexName = "qscexp_map_idx";
    private static final String mapKey = "mkey2";
    private static final String locBin = "loc";
    private static final String geoIndexName = "qscexp_loc_idx";
    private static final double matchLng = -122.0986857;
    private static final double matchLat = 37.4214209;
    private static final String matchPointGeoJson =
        "{\"type\":\"Point\",\"coordinates\":[" + matchLng + "," + matchLat + "]}";
    private static final String ptBin = "pt";
    private static final String ptGeoIndex = "qscexp_pt_idx";
    private static final double farLng = -121.0;
    private static final double farLat = 38.0;
    private static final int stringBoundMax = 2048;
    private static final String largeRegionGeoJson = circlePolygon(matchLng, matchLat, 0.5, 200);
    private static final String scoreListBin = "scoreList";
    private static final String scoreListIndex = "qscexp_score_list_idx";
    private static final int scoreListMatchIndex = 2;
    private static final int scoreListMatchValue = 42;
    private static final String venueBin = "venue";
    private static final String venueGeoIndex = "qscexp_venue_loc_idx";
    private static final String venueLocationKey = "location";
    private static final String nameBin = "name";
    private static final String upperExpIndex = "qscexp_upper_name_idx";
    private static final String upperMatch = "ALICE";
    private static final Expression upperExpIndexExpression =
        Exp.build(StringExp.upper(StringWriteFlags.DEFAULT, Exp.stringBin(nameBin)));
    private static final String agePlusExpIndex = "qscexp_age_plus_idx";
    private static final int agePlusMatch = 26;
    private static final Expression agePlusExpIndexExpression =
        Exp.build(Exp.add(Exp.intBin(ageBin), Exp.val(1)));

    private static DataSet dataSet;
    private static String blobHex;

    @BeforeAll
    static void prepare() {
        assumeQuerySelection();

        dataSet = DataSet.of(args.namespace, setName);

        session.delete(dataSet.ids("k1"));
        session.delete(dataSet.ids("k2"));

        createIndexQuietly(session, dataSet, intIndexName, ageBin, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, blobIndexName, blobBin, IndexType.BLOB,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, tagIndexName, tagBin, IndexType.STRING,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, mapIndexName, mapBin, IndexType.STRING,
            IndexCollectionType.MAPKEYS);
        createIndexQuietly(session, dataSet, geoIndexName, locBin, IndexType.GEO2DSPHERE,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, ptGeoIndex, ptBin, IndexType.GEO2DSPHERE,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, scoreListIndex, scoreListBin, IndexType.INTEGER,
            IndexCollectionType.DEFAULT, CTX.listIndex(scoreListMatchIndex));
        createIndexQuietly(session, dataSet, venueGeoIndex, venueBin, IndexType.GEO2DSPHERE,
            IndexCollectionType.DEFAULT, CTX.mapKey(Value.get(venueLocationKey)));
        createExpIndexQuietly(session, dataSet, upperExpIndex, IndexType.STRING, upperExpIndexExpression);
        createExpIndexQuietly(session, dataSet, agePlusExpIndex, IndexType.INTEGER, agePlusExpIndexExpression);

        byte[] blobBytes = new byte[8];
        Buffer.longToBytes(50001, blobBytes, 0);
        blobHex = Buffer.bytesToHexString(blobBytes);

        HashMap<String, String> map = new HashMap<>();
        map.put(mapKey, "v1");

        // Indexed regions (AeroCircle): geo SI probe uses the query point from AEL; the bin
        // must be a region that spatially contains that point, not an identical Point shape.
        String k1Loc = "{ \"type\": \"AeroCircle\", \"coordinates\": [[" +
            matchLng + ", " + matchLat + "], 3000.0 ] }";
        String k2Loc = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-121.0, 38.0], 3000.0 ] }";

        List<Integer> k1Scores = new ArrayList<>(List.of(10, 20, scoreListMatchValue, 30));
        List<Integer> k2Scores = new ArrayList<>(List.of(1, 2, 3, 4));

        HashMap<String, Object> k1Venue = new HashMap<>();
        k1Venue.put(venueLocationKey, Value.getAsGeoJSON(k1Loc));
        HashMap<String, Object> k2Venue = new HashMap<>();
        k2Venue.put(venueLocationKey, Value.getAsGeoJSON(k2Loc));

        session.upsert(dataSet.ids("k1"))
            .bins(ageBin, countryBin, tagBin, blobBin, mapBin, scoreListBin, nameBin, venueBin)
            .values(25, "US", tagMatch, blobBytes, map, k1Scores, "alice", k1Venue)
            .execute();

        session.upsert(dataSet.ids("k1"))
            .bin(locBin).setToGeoJson(k1Loc)
            .execute();

        session.upsert(dataSet.ids("k2"))
            .bins(ageBin, countryBin, tagBin, scoreListBin, nameBin, venueBin)
            .values(30, "CA", "ordinary", k2Scores, "bob", k2Venue)
            .execute();

        session.upsert(dataSet.ids("k2"))
            .bin(locBin).setToGeoJson(k2Loc)
            .execute();

        // Indexed points: the large-region tests query in the other direction from locBin -
        // a Polygon region literal against point-valued bins.
        session.upsert(dataSet.ids("k1"))
            .bin(ptBin).setToGeoJson(pointGeoJson(matchLng, matchLat))
            .execute();

        session.upsert(dataSet.ids("k2"))
            .bin(ptBin).setToGeoJson(pointGeoJson(farLng, farLat))
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
        session.dropIndex(dataSet, tagIndexName);
        session.dropIndex(dataSet, blobIndexName);
        session.dropIndex(dataSet, mapIndexName);
        session.dropIndex(dataSet, geoIndexName);
        dropIndexQuietly(dataSet, ptGeoIndex);
        dropIndexQuietly(dataSet, scoreListIndex);
        dropIndexQuietly(dataSet, venueGeoIndex);
        dropIndexQuietly(dataSet, upperExpIndex);
        dropIndexQuietly(dataSet, agePlusExpIndex);
    }

    @Test
    void planScalarIntegerSecondaryIndex() {
        QueryPlan plan = plan(dataSet,"$.age == 25");

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(intIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()));
    }

    @Test
    void planScalarStringPrimaryIndex() {
        QueryPlan plan = plan(dataSet,"$.country == 'US'");

        assertAll(
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * Scalar STRING equality uses the same hash-shaped range wire as BLOB, but carries UTF-8 bytes.
     * Assert the selected index and the golden result together so this complements, rather than
     * duplicates, the unindexed STRING primary-index case above.
     */
    @Test
    void scalarStringSecondaryIndexPlansAndExecutes() {
        String where = "$." + tagBin + " == '" + tagMatch + "'";
        QueryPlan plan = plan(dataSet,where);
        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(tagBin)
            .where(where)
            .execute());

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(tagIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(1, count));
    }

    /**
     * BLOB scalar equality — server selects the BLOB secondary index on explain.
     */
    @Test
    void planBlobEqualitySecondaryIndex() {
        String where = "$." + blobBin + " == x'" + blobHex + "'";
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(blobIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * E2E: BLOB query uses field {@code 44} explain → execute (SI plan when explain succeeds).
     */
    @Test
    void executeBlobEqualityReturnsMatchingRow() {
        String where = "$." + blobBin + " == x'" + blobHex + "'";

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(blobBin)
            .where(where)
            .execute());

        assertEquals(1, count);
    }

    /**
     * Bare {@code geoCompare(...)} and {@code geoCompare(...) == true} share the same GEO SI plan.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void explainGeoCompareUnwrapsExpCmpEq(boolean eqTrue) {
        String where = "geoCompare($." + locBin + ", geoJson('" + matchPointGeoJson + "'))"
            + (eqTrue ? " == true" : "");
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(geoIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.DEFAULT, plan.getIndexType()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * E2E: GEO query uses field {@code 44} explain → execute (SI plan when explain succeeds).
     */
    @Test
    void executeGeoCompareReturnsMatchingRow() {
        String where = "geoCompare($." + locBin + ", geoJson('" + matchPointGeoJson + "'))";

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(locBin)
            .where(where)
            .execute());

        assertEquals(1, count);
    }

    /**
     * A region literal past the {@code 2048}-byte STRING/BLOB sindex bound still selects the GEO
     * SI — geo bounds are capped at the GeoJSON wire limit (1 MiB), not the STRING/BLOB limit.
     * Servers that shared the STRING/BLOB cap PI-fell-back here even with a matching geo index.
     */
    @Test
    void planLargeGeoRegionSecondaryIndex() {
        assertTrue(largeRegionGeoJson.length() > stringBoundMax,
            "region literal must exceed the STRING/BLOB sindex bound to be meaningful");

        QueryPlan plan = plan(dataSet,
            "geoCompare($." + ptBin + ", geoJson('" + largeRegionGeoJson + "'))");

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(ptGeoIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.DEFAULT, plan.getIndexType()));
    }

    /**
     * The whole region literal survives into {@code INDEX_RANGE}. The server carries geo bounds by
     * borrowed pointer rather than by inline copy, so a truncated or stale bound would still
     * present as a well-formed range field — only the bytes themselves catch it.
     */
    @Test
    void planLargeGeoRegionIndexRange() {
        QueryPlan plan = plan(dataSet,
            "geoCompare($." + ptBin + ", geoJson('" + largeRegionGeoJson + "'))");
        byte[] rangeBytes = plan.getIndexRangeBytes();
        byte[] expected = largeRegionGeoJson.getBytes(StandardCharsets.UTF_8);

        assertAll(
            () -> assertEquals(ptBin, indexRangeBinName(rangeBytes)),
            () -> assertEquals(ParticleType.GEOJSON, indexRangeKtype(rangeBytes)),
            () -> assertArrayEquals(expected, indexRangeBound(rangeBytes)),
            () -> assertTrue(
                IndexRangeWire.describeProbeRange(rangeBytes).endsWith(" len=" + expected.length),
                "describeProbeRange should report the full geo bound length"));
    }

    /**
     * E2E: the large-region plan replays a multi-KB {@code INDEX_RANGE} on execute.
     */
    @Test
    void executeLargeGeoRegionReturnsMatchingRow() {
        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(ptBin)
            .where("geoCompare($." + ptBin + ", geoJson('" + largeRegionGeoJson + "'))")
            .execute());

        assertEquals(1, count);
    }

    /**
     * MAPKEYS + bare CDT EXISTS — server selects the MAPKEYS secondary index on explain.
     */
    @Test
    void planMapKeysExistsSecondaryIndex() {
        String where = "$." + mapBin + "." + mapKey + ".exists()";
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(mapIndexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.MAPKEYS, plan.getIndexType()));
    }

    /**
     * E2E: MAPKEYS EXISTS uses field {@code 44} explain → execute (SI when bare {@code .exists()}).
     */
    @Test
    void executeMapKeysExistsReturnsMatchingRows() {
        String where = "$." + mapBin + "." + mapKey + ".exists()";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(mapBin)
            .where(where)
            .execute();

        try (rs) {
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
    }

    /**
     * Ctx-path scalar equality — {@code $.scoreList.[N] == v} selects the ctx DEFAULT SI.
     */
    @Test
    void planCtxPathScalarEqualitySecondaryIndex() {
        String where = "$." + scoreListBin + ".[" + scoreListMatchIndex + "] == " + scoreListMatchValue;
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(scoreListIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.DEFAULT, plan.getIndexType()));
    }

    /**
     * E2E: ctx-path scalar query uses field {@code 44} explain → execute.
     */
    @Test
    void executeCtxPathScalarEqualityReturnsMatchingRow() {
        String where = "$." + scoreListBin + ".[" + scoreListMatchIndex + "] == " + scoreListMatchValue;

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(scoreListBin)
            .where(where)
            .execute());

        assertEquals(1, count);
    }

    /**
     * Ctx-path geoCompare — nested map key geo selects the ctx GEO SI.
     */
    @Test
    void planCtxPathGeoCompareSecondaryIndex() {
        String where = "geoCompare($." + venueBin + "." + venueLocationKey
            + ", geoJson('" + matchPointGeoJson + "'))";
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(venueGeoIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(IndexCollectionType.DEFAULT, plan.getIndexType()));
    }

    /**
     * E2E: ctx-path geo query uses field {@code 44} explain → execute.
     */
    @Test
    void executeCtxPathGeoCompareReturnsMatchingRow() {
        String where = "geoCompare($." + venueBin + "." + venueLocationKey
            + ", geoJson('" + matchPointGeoJson + "'))";

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(venueBin)
            .where(where)
            .execute());

        assertEquals(1, count);
    }

    /**
     * Expression sindex SI — the AEL arithmetic subtree {@code $.age + 1} structurally matches the
     * {@code exp=} index built from {@code Exp.add(Exp.intBin("age"), Exp.val(1))}. Expression
     * candidates carry no bin name, so the index range arrives with {@code bin_name_len == 0}.
     */
    @Test
    void planExpArithSecondaryIndex() {
        assumeExpressionSecondaryIndexSupported();

        QueryPlan plan = plan(dataSet,"($." + ageBin + " + 1) == " + agePlusMatch);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(agePlusExpIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(0, indexRangeBinNameLen(plan.getIndexRangeBytes())));
    }

    /**
     * E2E: expression-sindex query uses field {@code 44} explain → execute.
     */
    @Test
    void executeExpArithReturnsMatchingRow() {
        assumeExpressionSecondaryIndexSupported();

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(ageBin)
            .where("($." + ageBin + " + 1) == " + agePlusMatch)
            .execute());

        assertEquals(1, count);
    }

    /**
     * String path functions are PI-only even with a matching {@code exp=} index in place. AEL
     * compiles {@code .upper()} as a path/CDT string op, while {@code StringExp.upper(...)}
     * compiles through the client expression pipeline, so the two instruction trees never compare
     * equal in expression-candidate matching. The predicate itself still evaluates correctly as a
     * residual filter.
     */
    @Test
    void planExpCallUpperPrimaryIndex() {
        assumeExpressionSecondaryIndexSupported();

        String where = "$." + nameBin + ".upper() == '" + upperMatch + "'";
        assertAll(
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan(dataSet,where).getSelection()),
            () -> assertNull(plan(dataSet,where).getIndexName()),
            () -> assertNull(plan(dataSet,where).getIndexRangeBytes()),
            () -> assertEquals(1, countRecords(session.query(dataSet)
                .readingOnlyBins(nameBin)
                .where(where)
                .execute())));
    }

    private static String pointGeoJson(double lng, double lat) {
        return String.format(Locale.ROOT, "{\"type\":\"Point\",\"coordinates\":[%.7f,%.7f]}", lng, lat);
    }

    /**
     * Closed counter-clockwise ring approximating a circle, sized by vertex count so the literal
     * lands well above the STRING/BLOB bound while staying far below the 1 MiB geo ceiling.
     */
    private static String circlePolygon(double centerLng, double centerLat, double radiusDeg, int vertices) {
        StringBuilder sb = new StringBuilder("{\"type\":\"Polygon\",\"coordinates\":[[");
        for (int i = 0; i <= vertices; i++) {
            double theta = 2 * Math.PI * (i % vertices) / vertices;
            if (i > 0) {
                sb.append(',');
            }
            sb.append(String.format(Locale.ROOT, "[%.6f,%.6f]",
                centerLng + (radiusDeg * Math.cos(theta)),
                centerLat + (radiusDeg * Math.sin(theta))));
        }
        return sb.append("]]}").toString();
    }

    private static int indexRangeBinNameLen(byte[] rangeBytes) {
        return rangeBytes[1] & 0xFF;
    }

    private static String indexRangeBinName(byte[] rangeBytes) {
        return new String(rangeBytes, 2, indexRangeBinNameLen(rangeBytes), StandardCharsets.UTF_8);
    }

    private static int indexRangeKtype(byte[] rangeBytes) {
        return rangeBytes[2 + indexRangeBinNameLen(rangeBytes)] & 0xFF;
    }

    private static byte[] indexRangeBound(byte[] rangeBytes) {
        int offset = 2 + indexRangeBinNameLen(rangeBytes) + 1;
        int len = Buffer.bytesToInt(rangeBytes, offset);
        byte[] bound = new byte[len];
        System.arraycopy(rangeBytes, offset + 4, bound, 0, len);
        return bound;
    }

    private static void assumeExpressionSecondaryIndexSupported() {
        Version serverVersion = cluster.getRandomNode().getVersion();
        assumeTrue(serverVersion.isGreaterOrEqual(8, 1, 0, 0),
            "expression secondary index tests require server 8.1.0+");
    }
}
