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
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.COUNTRY_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.RECORD_COUNT;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.SCORE_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.countRecords;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.destroyQselint;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.plan;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.restoreQselintBaseline;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assumeQuerySelection;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.QuerySelectionIntegSupport.Fixture;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.util.Version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Integration tests for two-phase server query selection (explain → execute).
 *
 * <p>Requires cluster minimum version {@link Version#SERVER_VERSION_8_1_3}
 * ({@link com.aerospike.client.sdk.Cluster#supportsQuerySelection()}).</p>
 */
public class QuerySelectionIntegrationTest extends ClusterTest {
    private static final Fixture FIXTURE = Fixture.forSuffix("integ");
    private static final String bogusIndexName = "qsel_nonexistent_idx";
    private static final String OVERSIZED_LITERAL = "x".repeat(2048);

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        assumeQuerySelection();
        dataSet = prepareQselint(session, args.namespace, FIXTURE);
    }

    @AfterAll
    static void destroy() {
        destroyQselint(session, dataSet, FIXTURE);
    }

    @AfterEach
    void restoreFixture() {
        restoreQselintBaseline(session, dataSet, FIXTURE);
    }

    /**
     * Oversized string literal on an unindexed bin — explain PI-fallback, execute succeeds (full scan).
     */
    @Test
    void explainOversizedLiteralOnUnindexedBinFallsBackToPrimaryIndex() {
        String where = "$." + COUNTRY_BIN + " == '" + OVERSIZED_LITERAL + "'";
        QueryPlan plan = plan(dataSet,where);

        assertAll(
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
            COUNTRY_BIN + " == '" + OVERSIZED_LITERAL + "'";

        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()));

        assertEquals(0, countRecords(session.query(dataSet)
            .readingOnlyBins(AGE_BIN, COUNTRY_BIN)
            .where(where)
            .execute()));
    }

    /**
     * Oversized string literal on an indexed bin — unindexable term PI-fallbacks; execute succeeds.
     */
    @Test
    void explainOversizedLiteralOnIndexedBinFallsBackToPrimaryIndex() {
        String where = "$." + AGE_BIN + " == '" + OVERSIZED_LITERAL + "'";
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()));

        assertEquals(0, countRecords(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute()));
    }

    /**
     * OR inside one AND conjunct must not poison a sibling conjunct's SI candidate.
     */
    @Test
    void explainOrConjunctWithIndexableSiblingSelectsAgeIndex() {
        assertOrConjunctStillSelectsAgeIndex(
            "$.age > 10 and ($.age < 50 or $.country == 'US')");
        assertOrConjunctStillSelectsAgeIndex(
            "($.age < 50 or $.country == 'US') and $.age > 10");
    }

    /**
     * OR conjunct E2E — age index plan with residual filter over the unindexed OR branch.
     */
    @Test
    void executeOrConjunctWithIndexableSiblingReturnsMatchingRecords() {
        String where = "$.age > 10 and ($.age < 50 or $.country == 'US')";

        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()));

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN, COUNTRY_BIN)
            .where(where)
            .execute(), AGE_BIN);

        List<Integer> expected = new ArrayList<>();
        for (int age = 11; age <= 50; age++) {
            expected.add(age);
        }

        assertEquals(expected, ages);
    }

    private static void assertOrConjunctStillSelectsAgeIndex(String where) {
        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()));
    }

    /**
     * Index on bin age.
     * Range query on age -> server selects age index.
     */
    @Test
    void planSimpleRangeSelectsSecondaryIndex() {
        QueryPlan plan = plan(dataSet,"$.age >= 14 and $.age <= 18");

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(args.namespace, plan.getNamespace()),
            () -> assertEquals(FIXTURE.setName, plan.getSet()),
            () -> assertNotNull(plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()));
    }

    @Test
    void planBytesStableAcrossRepeatedProbes() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryPlan first = plan(dataSet,where);
        QueryPlan second = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, first.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, first.getIndexName()));
        assertAll(
            () -> assertEquals(first.getSelection(), second.getSelection()),
            () -> assertEquals(first.getIndexName(), second.getIndexName()),
            () -> assertArrayEquals(first.getExplainWhereBytes(), second.getExplainWhereBytes()),
            () -> assertArrayEquals(first.getIndexRangeBytes(), second.getIndexRangeBytes()));
    }

    /**
     * Index on bin age.
     * Range query on age; soft hint names non-existent index -> server selects age index on explain.
     * Execute parity (same rows as no hint) is in {@link QuerySelectionHintExecuteTest}.
     */
    @Test
    void planForIndexHintOnNonExistentIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .withHint(hint -> hint.forIndex(bogusIndexName));
        QueryPlan plan = plan(dataSet,qb);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertNotEquals(bogusIndexName, plan.getIndexName()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * Index on bin age.
     * Contradictory age range -> probe filtered out.
     */
    @Test
    void planContradictionPredicate() {
        QueryPlan plan = plan(dataSet,"$.age > 100 and $.age < 10");

        assertAll(
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

        try (rs) {
            List<String> countries = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                countries.add(rec.getString(COUNTRY_BIN));
            }

            assertAll(
                () -> assertEquals(25, countries.size()),
                () -> assertTrue(countries.stream().allMatch("US"::equals)));
        }
    }

    /**
     * Index on bin age.
     * Probe and execute on same age range -> consistent index choice and row set.
     */
    @Test
    void planThenExecuteConsistencyForSecondaryIndex() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()));

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute();

        try (rs) {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(AGE_BIN));
            }
            ages.sort(Integer::compareTo);

            assertAll(
                () -> assertEquals(5, ages.size()),
                () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
        }
    }

    /**
     * Field {@code 44} on execute must match the plan execute WHERE bytes (EXPLAIN cleared).
     */
    @Test
    void executeReplaysPlanWhereBytesOnWire() {
        assertAll(
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

        QueryPlan plan = plan(dataSet,where);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getExplainWhereBytes()));

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(AGE_BIN, COUNTRY_BIN)
            .where(where)
            .execute();

        try (rs) {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                assertEquals("US", rec.getString(COUNTRY_BIN));
                int age = rec.getInt(AGE_BIN);
                assertTrue(age > 30);
                ages.add(age);
            }
            ages.sort(Integer::compareTo);

            assertAll(
                () -> assertEquals(10, ages.size()),
                () -> assertEquals(List.of(32, 34, 36, 38, 40, 42, 44, 46, 48, 50), ages));
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

        assertAll(
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

        try (rs) {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(AGE_BIN));
                assertNull(rec.getValue(COUNTRY_BIN),
                    "country bin should not be returned when not requested");
            }
            ages.sort(Integer::compareTo);

            assertAll(
                () -> assertEquals(5, ages.size()),
                () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
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

        assertAll(
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
            .withHint(hint -> hint.forIndex(FIXTURE.ageIndex));
        QueryPlan plan = plan(dataSet,qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.forIndex(FIXTURE.ageIndex))
            .execute(), AGE_BIN);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
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
        QueryPlan plan = plan(dataSet,qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
            .execute(), AGE_BIN);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
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

        QueryPlan plan = plan(dataSet,where);

        int count = countRecords(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute());

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
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

        QueryPlan agePlan = plan(dataSet,ageWhere);
        QueryPlan scorePlan = plan(dataSet,scoreWhere);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(ageWhere)
            .execute(), AGE_BIN);

        List<Integer> scores = collectAges(session.query(dataSet)
            .readingOnlyBins(SCORE_BIN)
            .where(scoreWhere)
            .execute(), SCORE_BIN);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, agePlan.getSelection()),
            () -> assertEquals(FIXTURE.ageIndex, agePlan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages),
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, scorePlan.getSelection()),
            () -> assertEquals(FIXTURE.scoreIndex, scorePlan.getIndexName()),
            () -> assertEquals(List.of(40, 41, 42, 43, 44), scores));
    }

    /**
     * Indexes on bins age and score.
     * Range query suits age; soft hint names score index -> server selects age index on explain.
     * Execute parity (same rows as no hint) is in {@link QuerySelectionHintExecuteTest}.
     */
    @Test
    void planForIndexHintOnWrongExistingIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .withHint(hint -> hint.forIndex(FIXTURE.scoreIndex));
        QueryPlan plan = plan(dataSet,qb);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertNotEquals(FIXTURE.scoreIndex, plan.getIndexName()),
            () -> assertEquals(FIXTURE.ageIndex, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * Index on bin age, values 1..{@link QuerySelectionIntegSupport#RECORD_COUNT}.
     * Range bounds sent to the secondary index are inclusive for {@code >=}/{@code <=} and exclusive
     * for {@code >}/{@code <}, including at the first and last values in the set, where an
     * off-by-one in the index range would otherwise hide behind neighbouring rows.
     */
    @Test
    void executeIntegerRangeBoundaryRowsAreInclusive() {
        assertAll(
            () -> assertEquals(List.of(10, 11, 12), agesWhere("$.age >= 10 and $.age <= 12")),
            () -> assertEquals(List.of(11), agesWhere("$.age > 10 and $.age < 12")),
            () -> assertEquals(List.of(10, 11), agesWhere("$.age >= 10 and $.age < 12")),
            () -> assertEquals(List.of(11, 12), agesWhere("$.age > 10 and $.age <= 12")),
            () -> assertEquals(List.of(1), agesWhere("$.age >= 1 and $.age <= 1")),
            () -> assertEquals(List.of(1, 2), agesWhere("$.age <= 2")),
            () -> assertEquals(List.of(RECORD_COUNT), agesWhere("$.age >= " + RECORD_COUNT)));
    }

    /**
     * Delete a few keys so the indexed range has gaps in stored ages.
     */
    @Test
    void executeRangeAcrossSparseAgesReturnsExistingSubset() {
        session.delete(dataSet.ids(FIXTURE.keyPrefix + 15)).execute();
        session.delete(dataSet.ids(FIXTURE.keyPrefix + 16)).execute();
        session.delete(dataSet.ids(FIXTURE.keyPrefix + 17)).execute();

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where("$.age >= 14 and $.age <= 18")
            .execute(), AGE_BIN);

        assertEquals(List.of(14, 18), ages);
    }

    /**
     * Records without an {@code age} bin do not appear in an age filter result set.
     */
    @Test
    void executeRangeSkipsRecordsMissingAgeBin() {
        session.upsert(dataSet.ids(FIXTURE.keyPrefix + "missing"))
            .bin(COUNTRY_BIN).setTo("US")
            .execute();

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where("$.age >= 14 and $.age <= 18")
            .execute(), AGE_BIN);

        assertEquals(List.of(14, 15, 16, 17, 18), ages);
    }

    /**
     * Exclusive bounds one apart enclose no integer, so the range is empty before any record is read
     * and the server rejects the plan outright. This is the boundary case of
     * {@link #executeContradictionPredicateThrowsFilteredOut} and must not be confused with
     * {@link #executeValidSecondaryIndexQueryWithNoMatchesReturnsEmptyStream}: a satisfiable range
     * that happens to match nothing yields an empty stream, whereas an unsatisfiable one throws.
     */
    @Test
    void executeIntegerRangeWithNoRepresentableValuesThrowsFilteredOut() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> agesWhere("$.age > 1 and $.age < 2"));

        assertEquals(ResultCode.FILTERED_OUT, ex.getResultCode());
    }

    private static List<Integer> agesWhere(String where) {
        return collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute(), AGE_BIN);
    }

    private void assertExecuteWireWhereMatchesPlan(String where) {
        QueryBuilder qb = session.query(dataSet).where(where);
        QueryPlan plan = plan(dataSet,qb);

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
        QueryPlan plan = plan(dataSet,serverLedQb);

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
        assertFalse(Arrays.equals(legacy43, serverLed44),
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
