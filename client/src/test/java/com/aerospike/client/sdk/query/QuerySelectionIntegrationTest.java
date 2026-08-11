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

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.AGE_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.AGE_INDEX;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.COUNTRY_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.RECORD_COUNT;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.SCORE_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.SCORE_INDEX;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.SET_NAME;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.countRecords;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.destroyQselint;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.PartitionFilter;
import com.aerospike.client.sdk.command.PartitionTracker;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.util.Version;


import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for two-phase server query selection (explain → execute).
 *
 * <p>Requires cluster minimum version {@link Version#SERVER_VERSION_8_1_3}
 * ({@link com.aerospike.client.sdk.Cluster#supportsQuerySelection()}).</p>
 */
public class QuerySelectionIntegrationTest extends ClusterTest {
    private static final String bogusIndexName = "qsel_nonexistent_idx";
    private static final String EXP_SET_NAME = "qselexpint";
    private static final String EXP_KEY_MATCH = "qselexp1";
    private static final String EXP_KEY_OTHER = "qselexp2";
    private static final String EXP_INDEX = "qsel_exp_idx";
    private static final Expression EXP_INDEX_EXPRESSION = Exp.build(
        Exp.cond(
            Exp.and(
                Exp.ge(Exp.intBin(AGE_BIN), Exp.val(14)),
                Exp.eq(Exp.stringBin(COUNTRY_BIN), Exp.val("US"))),
            Exp.val(1),
            Exp.unknown()));

    private static DataSet dataSet;

    @BeforeAll
    public static void prepare() {
        QuerySelectionIntegSupport.assumeQuerySelectionEnabled();
        dataSet = prepareQselint(session, args.namespace);
    }

    @AfterAll
    public static void destroy() {
        destroyQselint(session, dataSet);
    }

    /**
     * Expression secondary index explain — field {@code 22} omits bin name ({@code bin_name_len == 0}).
     */
    @Test
    void expressionIndexExplainOmitsBinNameInRange() {
        assumeExpressionSecondaryIndexSupported();

        QueryPlan binPlan = explainPlan("$.age >= 14 and $.age <= 18");
        byte[] binRange = binPlan.getIndexRangeBytes();
        assertAll("binIndexContrast",
            () -> assertNotNull(binRange),
            () -> assertTrue(indexRangeBinNameLen(binRange) > 0,
                "bin index explain INDEX_RANGE should include driving bin name"));

        DataSet expDataSet = prepareExpIndexFixture();
        try {
            String where = "$.age >= 14 and $.country == 'US'";

            QueryPlan plan = explainPlan(expDataSet, where);

            assertAll("expressionIndexExplain",
                () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
                () -> assertEquals(EXP_INDEX, plan.getIndexName()),
                () -> assertNotNull(plan.getIndexRangeBytes()),
                () -> assertEquals(0, indexRangeBinNameLen(plan.getIndexRangeBytes()),
                    "expression SI explain INDEX_RANGE should omit driving bin name"));

            List<Integer> ages = collectAges(session.query(expDataSet)
                .readingOnlyBins(AGE_BIN, COUNTRY_BIN)
                .where(where)
                .execute(), AGE_BIN);

            assertAll("expressionIndexExecute",
                () -> assertEquals(1, ages.size()),
                () -> assertEquals(List.of(16), ages));
        }
        finally {
            destroyExpIndexFixture(expDataSet);
        }
    }

    /**
     * Oversized string literal on an unindexed bin — explain PI-fallback, execute succeeds (full scan).
     */
    @Test
    void explainOversizedLiteralOnUnindexedBinFallsBackToPrimaryIndex() {
        String where = "$." + COUNTRY_BIN + " == '" + oversizedLiteral() + "'";
        QueryPlan plan = explainPlan(where);

        assertAll("oversizedLiteralPiExplain",
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()));

        assertEquals(0, countRecords(session.query(dataSet)
            .readingOnlyBins(COUNTRY_BIN)
            .where(where)
            .execute()));
    }

    /**
     * Partial AND with an oversized literal — age SI still selected; execute filters both conjuncts.
     */
    @Test
    void explainPartialAndWithOversizedLiteralStillSelectsAgeIndex() {
        String where = "$." + AGE_BIN + " >= 14 and $." + AGE_BIN + " <= 18 and $." +
            COUNTRY_BIN + " == '" + oversizedLiteral() + "'";

        QueryPlan plan = explainPlan(where);

        assertAll("partialAndOversizedExplain",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()));

        assertEquals(0, countRecords(session.query(dataSet)
            .readingOnlyBins(AGE_BIN, COUNTRY_BIN)
            .where(where)
            .execute()));
    }

    private static int indexRangeBinNameLen(byte[] rangeBytes) {
        return rangeBytes[1] & 0xFF;
    }

    private static String oversizedLiteral() {
        return "x".repeat(2048);
    }

    /**
     * Index on bin age.
     * Range query on age -> server selects age index.
     */
    @Test
    void planSimpleRangeSelectsSecondaryIndex() {
        QueryPlan plan = explainPlan("$.age >= 14 and $.age <= 18");

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(args.namespace, plan.getNamespace()),
            () -> assertEquals(SET_NAME, plan.getSet()),
            () -> assertNotNull(plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()));
    }

    @Test
    void planBytesStableAcrossRepeatedProbes() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryPlan first = explainPlan(where);
        QueryPlan second = explainPlan(where);

        assertAll("firstProbeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, first.getSelection()),
            () -> assertEquals(AGE_INDEX, first.getIndexName()));
        assertAll("repeatedProbeStability",
            () -> assertEquals(first.getSelection(), second.getSelection()),
            () -> assertEquals(first.getIndexName(), second.getIndexName()),
            () -> assertArrayEquals(first.getExplainWhereBytes(), second.getExplainWhereBytes()),
            () -> assertArrayEquals(first.getIndexRangeBytes(), second.getIndexRangeBytes()));
    }

    /**
     * Index on bin age.
     * Range query on age; hint names non-existent index -> server selects age index.
     */
    @Test
    void planForIndexHintOnNonExistentIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .withHint(hint -> hint.forIndex(bogusIndexName));
        QueryPlan plan = explainPlan(qb);

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertNotEquals(bogusIndexName, plan.getIndexName()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * Index on bin age.
     * Contradictory age range -> probe filtered out.
     */
    @Test
    void planContradictionPredicate() {
        QueryPlan plan = explainPlan("$.age > 100 and $.age < 10");

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.FILTERED_OUT, plan.getSelection()),
            () -> assertNotNull(plan.getExplainWhereBytes()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()));
    }

    /**
     * No index on country.
     * Equality query on country -> primary-index path; matching records returned.
     */
    @Test
    void executePrimaryIndexPredicateReturnsMatchingRecords() {
        String where = "$.country == 'US'";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(COUNTRY_BIN)
            .where(where)
            .execute();

        try {
            List<String> countries = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                countries.add(rec.getString(COUNTRY_BIN));
            }

            assertAll("executeResults",
                () -> assertEquals(25, countries.size()),
                () -> assertTrue(countries.stream().allMatch("US"::equals)));
        }
        finally {
            rs.close();
        }
    }

    /**
     * Index on bin age.
     * Probe and execute on same age range -> consistent index choice and row set.
     */
    @Test
    void planThenExecuteConsistencyForSecondaryIndex() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryPlan plan = explainPlan(where);

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()));

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(AGE_BIN));
            }
            ages.sort(Integer::compareTo);

            assertAll("executeResults",
                () -> assertEquals(5, ages.size()),
                () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
        }
        finally {
            rs.close();
        }
    }

    /**
     * Field {@code 44} on execute must match the plan execute WHERE bytes (EXPLAIN cleared).
     */
    @Test
    void executeReplaysPlanWhereBytesOnWire() {
        assertAll("whereReplay",
            () -> assertExecuteWireWhereMatchesPlan("$.age >= 14 and $.age <= 18"),
            () -> assertExecuteWireWhereMatchesPlan("$.country == 'US'"));
    }

    /**
     * Index on bin age.
     * Compound query (age + country) -> server selects age index; rows match both conjuncts.
     */
    @Test
    void executeCompoundPredicateReturnsMatchingRecords() {
        String where = "$.age > 30 and $.country == 'US'";

        QueryPlan plan = explainPlan(where);

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertNotNull(plan.getExplainWhereBytes()));

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(AGE_BIN, COUNTRY_BIN)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                assertEquals("US", rec.getString(COUNTRY_BIN));
                int age = rec.getInt(AGE_BIN);
                assertTrue(age > 30);
                ages.add(age);
            }
            ages.sort(Integer::compareTo);

            assertAll("executeResults",
                () -> assertEquals(10, ages.size()),
                () -> assertEquals(List.of(32, 34, 36, 38, 40, 42, 44, 46, 48, 50), ages));
        }
        finally {
            rs.close();
        }
    }

    /**
     * Index on bin age.
     * Compound WHERE (age + country): server-led (field 44) vs forBin legacy (residual field 43)
     * -> same matching row set.
     */
    @Test
    void compoundPredicateServerLedMatchesLegacyForBin() {
        String where = "$.age > 30 and $.country == 'US'";

        List<Integer> serverLedAges = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute(), AGE_BIN);

        List<Integer> legacyAges = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.forBin(AGE_BIN))
            .execute(), AGE_BIN);

        assertAll("compoundLegacyVsServerLed",
            () -> assertEquals(serverLedAges, legacyAges),
            () -> assertEquals(List.of(32, 34, 36, 38, 40, 42, 44, 46, 48, 50), serverLedAges),
            () -> assertCompoundPredicateField44DiffersFromLegacyField43(where));
    }

    /**
     * Index on bin age.
     * Range query with readingOnlyBins(age) -> projected bins only.
     */
    @Test
    void executeWithReadingOnlyBinsProjectsRequestedBins() {
        String where = "$.age >= 14 and $.age <= 18";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(AGE_BIN));
                assertNull(rec.getValue(COUNTRY_BIN),
                    "country bin should not be returned when not requested");
            }
            ages.sort(Integer::compareTo);

            assertAll("executeResults",
                () -> assertEquals(5, ages.size()),
                () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
        }
        finally {
            rs.close();
        }
    }

    // -- Tier 3: hints & routing ------------------------------------------------

    /**
     * Index on bin age.
     * forBin(age) hint -> legacy path; same row set as default probe path.
     */
    @Test
    void forBinHintUsesLegacyExecutePath() {
        String where = "$.age >= 14 and $.age <= 18";

        List<Integer> defaultAges = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute(), AGE_BIN);

        List<Integer> forBinAges = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.forBin(AGE_BIN))
            .execute(), AGE_BIN);

        assertAll("forBinLegacyPath",
            () -> assertEquals(defaultAges, forBinAges),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), forBinAges));
    }

    /**
     * Index on bin age.
     * Range query on age with forIndex(age) hint -> probe and execute return matching records.
     */
    @Test
    void forIndexHintProbesAndExecutes() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryBuilder qb = session.query(dataSet)
            .where(where)
            .withHint(hint -> hint.forIndex(AGE_INDEX));
        QueryPlan plan = explainPlan(qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.forIndex(AGE_INDEX))
            .execute(), AGE_BIN);

        assertAll("forIndexProbeAndExecute",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertEquals(5, ages.size()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    /**
     * Index on bin age.
     * queryDuration-only hint (no index/bin hint) -> still probes; matching records returned.
     */
    @Test
    void queryDurationOnlyHintStillProbesAndExecutes() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryBuilder qb = session.query(dataSet)
            .where(where)
            .withHint(hint -> hint.queryDuration(QueryDuration.SHORT));
        QueryPlan plan = explainPlan(qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
            .execute(), AGE_BIN);

        assertAll("durationOnlyHint",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    /**
     * No WHERE clause.
     * Scan -> all records in set returned.
     */
    @Test
    void noWhereScanReturnsAllRecords() {
        int count = countRecords(session.query(dataSet).execute());

        assertEquals(RECORD_COUNT, count);
    }

    /**
     * Server below AEL / query-selection version.
     * String AEL on dataset query fails at execute (no client parse fallback).
     */
    @Test
    void gateOffStringAelFailsAtExecute() {
        String where = "$.age >= 14 and $.age <= 18";
        Version saved = cluster.getVersion();

        try {
            cluster.setVersion(Version.SERVER_VERSION_8_1_2);
            assumeFalse(cluster.supportsQuerySelection(),
                "server version below 8.1.3 should not use query selection");
            assumeFalse(cluster.supportsAel(),
                "server version below 8.1.3 should not support string AEL");

            AerospikeException ex = assertThrows(AerospikeException.class, () ->
                session.query(dataSet)
                    .readingOnlyBins(AGE_BIN)
                    .where(where)
                    .execute());

            assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
        }
        finally {
            cluster.setVersion(saved);
        }
    }

    // -- Tier 4: errors & edge cases --------------------------------------------

    /**
     * Index on bin age.
     * Contradictory age range -> execute throws filtered out.
     */
    @Test
    void executeContradictionPredicateThrowsFilteredOut() {
        AerospikeException ex = assertThrows(AerospikeException.class, () ->
            session.query(dataSet)
                .where("$.age > 100 and $.age < 10")
                .execute());

        assertEquals(ResultCode.FILTERED_OUT, ex.getResultCode());
    }

    /**
     * Index on bin age.
     * Valid age equality with no matching data -> secondary index plan, empty stream (not error).
     */
    @Test
    void executeValidSecondaryIndexQueryWithNoMatchesReturnsEmptyStream() {
        String where = "$.age == 999";

        QueryPlan plan = explainPlan(where);

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute());

        assertAll("emptySiResult",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertEquals(0, count));
    }

    /**
     * Indexes on bins age and score.
     * Age range query -> server selects age index; score range query -> server selects score index.
     */
    @Test
    void multipleIndexesServerAutoSelectsMatchingIndex() {
        String ageWhere = "$.age >= 14 and $.age <= 18";
        String scoreWhere = "$.score >= 40 and $.score <= 44";

        QueryPlan agePlan = explainPlan(ageWhere);
        QueryPlan scorePlan = explainPlan(scoreWhere);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(ageWhere)
            .execute(), AGE_BIN);

        List<Integer> scores = collectScores(session.query(dataSet)
            .readingOnlyBins(SCORE_BIN)
            .where(scoreWhere)
            .execute());

        assertAll("multiIndexAutoSelect",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, agePlan.getSelection()),
            () -> assertEquals(AGE_INDEX, agePlan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages),
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, scorePlan.getSelection()),
            () -> assertEquals(SCORE_INDEX, scorePlan.getIndexName()),
            () -> assertEquals(List.of(40, 41, 42, 43, 44), scores));
    }

    /**
     * Indexes on bins age and score.
     * Range query suits age; hint names score index -> server selects age index.
     */
    @Test
    void planForIndexHintOnWrongExistingIndex() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryBuilder qb = session.query(dataSet)
            .where(where)
            .withHint(hint -> hint.forIndex(SCORE_INDEX));
        QueryPlan plan = explainPlan(qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.forIndex(SCORE_INDEX))
            .execute(), AGE_BIN);

        assertAll("wrongExistingIndexHint",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertNotEquals(SCORE_INDEX, plan.getIndexName()),
            () -> assertEquals(AGE_INDEX, plan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    private static List<Integer> collectScores(RecordStream rs) {
        try {
            List<Integer> scores = new ArrayList<>();
            while (rs.hasNext()) {
                scores.add(rs.next().recordOrThrow().getInt(SCORE_BIN));
            }
            scores.sort(Integer::compareTo);
            return scores;
        }
        finally {
            rs.close();
        }
    }

    private static void assumeExpressionSecondaryIndexSupported() {
        Version serverVersion = cluster.getRandomNode().getVersion();
        assumeTrue(serverVersion.isGreaterOrEqual(8, 1, 0, 0),
            "expression secondary index tests require server 8.1.0+");
    }

    private static DataSet prepareExpIndexFixture() {
        DataSet expDataSet = DataSet.of(args.namespace, EXP_SET_NAME);

        session.delete(expDataSet.ids(EXP_KEY_MATCH));
        session.delete(expDataSet.ids(EXP_KEY_OTHER));

        try {
            session.createIndex(expDataSet, EXP_INDEX, IndexType.INTEGER, IndexCollectionType.DEFAULT,
                EXP_INDEX_EXPRESSION)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        session.upsert(expDataSet.ids(EXP_KEY_MATCH))
            .bins(AGE_BIN, SCORE_BIN, COUNTRY_BIN)
            .values(16, 16, "US")
            .execute();
        session.upsert(expDataSet.ids(EXP_KEY_OTHER))
            .bins(AGE_BIN, SCORE_BIN, COUNTRY_BIN)
            .values(30, 30, "CA")
            .execute();

        return expDataSet;
    }

    private static void destroyExpIndexFixture(DataSet expDataSet) {
        session.delete(expDataSet.ids(EXP_KEY_MATCH));
        session.delete(expDataSet.ids(EXP_KEY_OTHER));
        try {
            session.dropIndex(expDataSet, EXP_INDEX);
        }
        catch (AerospikeException ignored) {
        }
    }

    private static QueryPlan explainPlan(DataSet ds, String where) {
        return IndexProbePlanner.plan(
            session, ds, WhereClauseProcessor.from(where), null);
    }

    private static QueryPlan explainPlan(String where) {
        return explainPlan(dataSet, where);
    }

    private static QueryPlan explainPlan(QueryBuilder qb) {
        return IndexProbePlanner.plan(session, dataSet, qb.getAel(), qb.getQueryHint());
    }

    private void assertExecuteWireWhereMatchesPlan(String where) {
        QueryBuilder qb = session.query(dataSet).where(where);
        QueryPlan plan = explainPlan(qb);

        assertNotNull(plan.getExplainWhereBytes());

        ResolvedSettings settings = session.getBehavior().getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
        QueryCommand cmd = QueryCommand.forPlan(cluster, dataSet, plan, settings, qb);
        CommandBuffer cb = encodeExecuteQuery(cmd);

        assertArrayEquals(plan.getExecuteWhereBytes(), fieldBytes(cb, FieldType.WHERE));
        assertEquals(0, com.aerospike.client.sdk.query.plan.QueryWhereWire.flags(
            fieldBytes(cb, FieldType.WHERE)));
    }

    private void assertCompoundPredicateField44DiffersFromLegacyField43(String where) {
        QueryBuilder serverLedQb = session.query(dataSet).where(where);
        QueryPlan plan = explainPlan(serverLedQb);

        assertNotNull(plan.getExplainWhereBytes());

        ResolvedSettings settings = session.getBehavior().getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);

        byte[] serverLed44 = fieldBytes(
            encodeExecuteQuery(QueryCommand.forPlan(cluster, dataSet, plan, settings, serverLedQb)),
            FieldType.WHERE);

        QueryBuilder legacyQb = session.query(dataSet)
            .where(where)
            .withHint(hint -> hint.forBin(AGE_BIN));
        QueryCommand legacyCmd = IndexProbePlanner.buildCommand(
            session, dataSet, legacyQb.getAel(), legacyQb.getQueryHint(), settings, legacyQb);
        byte[] legacy43 = fieldBytes(encodeExecuteQuery(legacyCmd), FieldType.FILTER_EXP);

        assertArrayEquals(plan.getExecuteWhereBytes(), serverLed44);
        assertNotEquals(legacy43, serverLed44,
            "legacy residual field 43 should differ from server-led field 44 on compound WHERE");
    }

    private static CommandBuffer encodeExecuteQuery(QueryCommand cmd) {
        PartitionTracker tracker = new PartitionTracker(cmd, new Node[1], PartitionFilter.all());
        CommandBuffer cb = new CommandBuffer();
        cb.setQuery(cmd, tracker, null, 1L);
        return cb;
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
}
