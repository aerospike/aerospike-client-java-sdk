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
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.util.Version;

import com.aerospike.ael.ParseResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for two-phase server query selection (explain → execute).
 *
 * <p>Requires cluster minimum version {@link Version#SERVER_VERSION_8_1_3}
 * ({@link com.aerospike.client.sdk.Cluster#supportsQuerySelection()}).</p>
 */
public class QuerySelectionIntegrationTest extends ClusterTest {
    private static final String setName = "qselint";
    private static final String indexName = "qsel_age_idx";
    private static final String scoreIndexName = "qsel_score_idx";
    private static final String bogusIndexName = "qsel_nonexistent_idx";
    private static final String keyPrefix = "qselkey";
    private static final String binName = "age";
    private static final String scoreBinName = "score";
    private static final String countryBinName = "country";
    private static final int size = 50;

    private static DataSet dataSet;

    @BeforeAll
    public static void prepare() {
        assumeTrue(cluster.supportsQuerySelection(), "server does not support query selection");

        dataSet = DataSet.of(args.namespace, setName);

        for (int i = 1; i <= size; i++) {
            session.delete(dataSet.ids(keyPrefix + i));
        }

        try {
            session.createIndex(dataSet, indexName, binName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        try {
            session.createIndex(dataSet, scoreIndexName, scoreBinName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        for (int i = 1; i <= size; i++) {
            String country = (i % 2 == 0) ? "US" : "CA";
            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(binName, scoreBinName, countryBinName)
                .values(i, i, country)
                .execute();
        }
    }

    @AfterAll
    public static void destroy() {
        if (dataSet == null) {
            return;
        }

        for (int i = 1; i <= size; i++) {
            session.delete(dataSet.ids(keyPrefix + i));
        }
        session.dropIndex(dataSet, indexName);
        session.dropIndex(dataSet, scoreIndexName);
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
            () -> assertEquals(setName, plan.getSet()),
            () -> assertNotNull(plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()),
            () -> assertEquals(indexName, plan.getIndexName()));
    }

    /**
     * No index on country.
     * Equality query on country -> server selects primary index.
     */
    @Test
    void planNonIndexedPredicateSelectsPrimaryIndex() {
        QueryPlan plan = explainPlan("$.country == 'US'");

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection()),
            () -> assertEquals(args.namespace, plan.getNamespace()),
            () -> assertEquals(setName, plan.getSet()),
            () -> assertNull(plan.getIndexName()),
            () -> assertNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    /**
     * Index on bin age.
     * Range query on age with forIndex(age) hint -> server selects age index.
     */
    @Test
    void planForIndexHintUsesHintedIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .withHint(hint -> hint.forIndex(indexName));
        QueryPlan plan = explainPlan(qb);

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
    }

    @Test
    void planBytesStableAcrossRepeatedProbes() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryPlan first = explainPlan(where);
        QueryPlan second = explainPlan(where);

        assertAll("firstProbeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, first.getSelection()),
            () -> assertEquals(indexName, first.getIndexName()));
        assertAll("repeatedProbeStability",
            () -> assertEquals(first.getSelection(), second.getSelection()),
            () -> assertEquals(first.getIndexName(), second.getIndexName()),
            () -> assertArrayEquals(first.getExplainWhereBytes(), second.getExplainWhereBytes()),
            () -> assertArrayEquals(first.getIndexRangeBytes(), second.getIndexRangeBytes()));
    }

    @Test
    void indexProbePlannerSmoke() {
        String where = "$.age >= 14 and $.age <= 18";
        WhereClauseProcessor whereClause = WhereClauseProcessor.from(true, where);

        QueryPlan plan = IndexProbePlanner.plan(session, dataSet, whereClause, null);

        assertAll("probeResponse",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertNotNull(plan.getExplainWhereBytes()));
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
            () -> assertEquals(indexName, plan.getIndexName()),
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
     * Index on bin age.
     * Range query on age -> matching records returned.
     */
    @Test
    void executeSimpleRangeReturnsMatchingRecords() {
        String where = "$.age >= 14 and $.age <= 18";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(binName));
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
     * Index on bin age.
     * Equality query on age -> single matching record.
     */
    @Test
    void executeEqualityReturnsSingleRecord() {
        String where = "$.age == 25";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(binName));
            }

            assertAll("executeResults",
                () -> assertEquals(1, ages.size()),
                () -> assertEquals(25, ages.get(0)));
        }
        finally {
            rs.close();
        }
    }

    /**
     * No index on country.
     * Equality query on country -> primary-index path; matching records returned.
     */
    @Test
    void executePrimaryIndexPredicateReturnsMatchingRecords() {
        String where = "$.country == 'US'";

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(countryBinName)
            .where(where)
            .execute();

        try {
            List<String> countries = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                countries.add(rec.getString(countryBinName));
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
            () -> assertEquals(indexName, plan.getIndexName()));

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(binName));
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
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertNotNull(plan.getExplainWhereBytes()));

        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(binName, countryBinName)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                assertEquals("US", rec.getString(countryBinName));
                int age = rec.getInt(binName);
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
            .readingOnlyBins(binName)
            .where(where)
            .execute());

        List<Integer> legacyAges = collectAges(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .withHint(hint -> hint.forBin(binName))
            .execute());

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
            .readingOnlyBins(binName)
            .where(where)
            .execute();

        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                ages.add(rec.getInt(binName));
                assertNull(rec.getValue(countryBinName),
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
            .readingOnlyBins(binName)
            .where(where)
            .execute());

        List<Integer> forBinAges = collectAges(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .withHint(hint -> hint.forBin(binName))
            .execute());

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
            .withHint(hint -> hint.forIndex(indexName));
        QueryPlan plan = explainPlan(qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .withHint(hint -> hint.forIndex(indexName))
            .execute());

        assertAll("forIndexProbeAndExecute",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
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
            .readingOnlyBins(binName)
            .where(where)
            .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
            .execute());

        assertAll("durationOnlyHint",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    /**
     * Index on bin age.
     * where(Exp) on age -> non-probe path; matching records returned.
     */
    @Test
    void whereExpUsesNonProbeExecutePath() {
        int begin = 14;
        int end = 18;

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(Exp.and(
                Exp.ge(Exp.intBin(binName), Exp.val(begin)),
                Exp.le(Exp.intBin(binName), Exp.val(end))))
            .execute());

        assertAll("whereExpResults",
            () -> assertEquals(5, ages.size()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    /**
     * No WHERE clause.
     * Scan -> all records in set returned.
     */
    @Test
    void noWhereScanReturnsAllRecords() {
        int count = countRecords(session.query(dataSet).execute());

        assertEquals(size, count);
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
                    .readingOnlyBins(binName)
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
            .readingOnlyBins(binName)
            .where(where)
            .execute());

        assertAll("emptySiResult",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
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
            .readingOnlyBins(binName)
            .where(ageWhere)
            .execute());

        List<Integer> scores = collectScores(session.query(dataSet)
            .readingOnlyBins(scoreBinName)
            .where(scoreWhere)
            .execute());

        assertAll("multiIndexAutoSelect",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, agePlan.getSelection()),
            () -> assertEquals(indexName, agePlan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages),
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, scorePlan.getSelection()),
            () -> assertEquals(scoreIndexName, scorePlan.getIndexName()),
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
            .withHint(hint -> hint.forIndex(scoreIndexName));
        QueryPlan plan = explainPlan(qb);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(binName)
            .where(where)
            .withHint(hint -> hint.forIndex(scoreIndexName))
            .execute());

        assertAll("wrongExistingIndexHint",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertNotEquals(scoreIndexName, plan.getIndexName()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    private static List<Integer> collectScores(RecordStream rs) {
        try {
            List<Integer> scores = new ArrayList<>();
            while (rs.hasNext()) {
                scores.add(rs.next().recordOrThrow().getInt(scoreBinName));
            }
            scores.sort(Integer::compareTo);
            return scores;
        }
        finally {
            rs.close();
        }
    }

    private static List<Integer> collectAges(RecordStream rs) {
        try {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                ages.add(rs.next().recordOrThrow().getInt(binName));
            }
            ages.sort(Integer::compareTo);
            return ages;
        }
        finally {
            rs.close();
        }
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

    private static QueryPlan explainPlan(String where) {
        return IndexProbePlanner.plan(
            session, dataSet, WhereClauseProcessor.from(true, where), null);
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
            .withHint(hint -> hint.forBin(binName));
        ParseResult pr = legacyQb.getAel().process(dataSet.getNamespace(), dataSet.getSet(), session);
        QueryCommand legacyCmd = new QueryCommand(
            cluster, dataSet, pr.getFilter(), pr.getExpression(), settings, legacyQb);
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
