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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.junit.ServerFeature;
import com.aerospike.client.sdk.junit.ServerFeatureSupport;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/** Shared fixture and helpers for server query-selection integration tests. */
final class QuerySelectionIntegSupport {
    static final String AGE_BIN = "age";
    static final String SCORE_BIN = "score";
    static final String COUNTRY_BIN = "country";
    static final int RECORD_COUNT = 50;

    /** Names for one class-scoped qselint dataset; suffix keeps parallel test classes isolated. */
    static final class Fixture {
        final String setName;
        final String ageIndex;
        final String scoreIndex;
        final String keyPrefix;

        private Fixture(String setName, String ageIndex, String scoreIndex, String keyPrefix) {
            this.setName = setName;
            this.ageIndex = ageIndex;
            this.scoreIndex = scoreIndex;
            this.keyPrefix = keyPrefix;
        }

        static Fixture forSuffix(String suffix) {
            return new Fixture(
                "qselint_" + suffix,
                "qsel_age_idx_" + suffix,
                "qsel_score_idx_" + suffix,
                "qselkey_" + suffix);
        }

        DataSet dataSet(String namespace) {
            return DataSet.of(namespace, setName);
        }
    }

    private QuerySelectionIntegSupport() {
    }

    static void assumeQuerySelection() {
        ServerFeatureSupport.assume(ServerFeature.QUERY_SELECTION);
    }

    static QueryPlan plan(DataSet dataSet, String where) {
        return plan(ClusterTest.session, dataSet, where);
    }

    static QueryPlan plan(DataSet dataSet, QueryBuilder qb) {
        return plan(ClusterTest.session, dataSet, qb);
    }

    static QueryPlan plan(Session session, DataSet dataSet, String where) {
        return IndexProbePlanner.plan(
            session,
            dataSet,
            WhereClauseProcessor.from(where),
            null);
    }

    static QueryPlan plan(Session session, DataSet dataSet, QueryBuilder qb) {
        return IndexProbePlanner.plan(session, dataSet, qb.getAel(), qb.getQueryHint());
    }

    static void assertPlan(
        QueryPlan plan,
        QuerySelection expectedSelection,
        String expectedIndexName,
        boolean expectIndexRange
    ) {
        assertPlan(plan, expectedSelection, expectedIndexName, expectIndexRange, null);
    }

    static void assertPlan(
        QueryPlan plan,
        QuerySelection expectedSelection,
        String expectedIndexName,
        boolean expectIndexRange,
        IndexCollectionType expectedCollectionType
    ) {
        if (expectedCollectionType == null) {
            assertAll(
                () -> assertEquals(expectedSelection, plan.getSelection()),
                () -> assertEquals(expectedIndexName, plan.getIndexName()),
                () -> {
                    if (expectIndexRange) {
                        assertNotNull(plan.getIndexRangeBytes());
                    }
                    else {
                        assertNull(plan.getIndexRangeBytes());
                    }
                },
                () -> assertNotNull(plan.getExplainWhereBytes()));
        }
        else {
            assertAll(
                () -> assertEquals(expectedSelection, plan.getSelection()),
                () -> assertEquals(expectedIndexName, plan.getIndexName()),
                () -> assertNotNull(plan.getIndexRangeBytes()),
                () -> assertEquals(expectedCollectionType, plan.getIndexType()),
                () -> assertNotNull(plan.getExplainWhereBytes()));
        }
    }

    /**
     * Replays explain {@code INDEX_RANGE} bytes through {@link Filter#fromWireRange} and asserts
     * {@link Filter#write} round-trips the opaque payload verbatim.
     */
    static Filter assertIndexRangeRoundTrips(QueryPlan plan) {
        byte[] rangeBytes = plan.getIndexRangeBytes();
        assertNotNull(rangeBytes);
        IndexCollectionType indexType = plan.getIndexType() != null
            ? plan.getIndexType()
            : IndexCollectionType.DEFAULT;
        Filter filter = Filter.fromWireRange(plan.getIndexName(), rangeBytes, indexType);
        assertTrue(filter.hasWireRange());
        assertEquals(plan.getIndexName(), filter.getIndexName());
        assertEquals(indexType, filter.getCollectionType());
        assertEquals(indexType, filter.getColType());
        assertEquals(rangeBytes.length, filter.estimateSize());
        byte[] out = new byte[rangeBytes.length];
        filter.write(out, 0);
        assertArrayEquals(rangeBytes, out);
        return filter;
    }

    static void deleteKeys(DataSet dataSet, IntFunction<String> keyFn, int fromInclusive, int toInclusive) {
        for (int i = fromInclusive; i <= toInclusive; i++) {
            ClusterTest.session.delete(dataSet.ids(keyFn.apply(i)));
        }
    }

    static DataSet prepareQselint(Session session, String namespace, Fixture fixture) {
        DataSet dataSet = fixture.dataSet(namespace);

        for (int i = 1; i <= RECORD_COUNT; i++) {
            session.delete(dataSet.ids(fixture.keyPrefix + i)).execute();
        }

        createIndexQuietly(session, dataSet, fixture.ageIndex, AGE_BIN, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, fixture.scoreIndex, SCORE_BIN, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);

        seedQselintRows(session, dataSet, fixture, 1, RECORD_COUNT);
        return dataSet;
    }

    static void destroyQselint(Session session, DataSet dataSet, Fixture fixture) {
        if (dataSet == null) {
            return;
        }

        for (int i = 1; i <= RECORD_COUNT; i++) {
            session.delete(dataSet.ids(fixture.keyPrefix + i)).execute();
        }
        session.delete(dataSet.ids(fixture.keyPrefix + "missing")).execute();
        dropIndexQuietly(session, dataSet, fixture.ageIndex);
        dropIndexQuietly(session, dataSet, fixture.scoreIndex);
    }

    /** Restores rows tests may delete or rewrite so later methods see the baseline catalog. */
    static void restoreQselintBaseline(Session session, DataSet dataSet, Fixture fixture) {
        seedQselintRows(session, dataSet, fixture, 15, 17);
        session.delete(dataSet.ids(fixture.keyPrefix + "missing")).execute();
    }

    private static void seedQselintRows(
        Session session,
        DataSet dataSet,
        Fixture fixture,
        int fromInclusive,
        int toInclusive
    ) {
        for (int i = fromInclusive; i <= toInclusive; i++) {
            String country = (i % 2 == 0) ? "US" : "CA";
            upsertRow(session, dataSet, fixture, i, i, i, country);
        }
    }

    static void upsertRow(
        Session session,
        DataSet dataSet,
        Fixture fixture,
        int keyNum,
        int age,
        int score,
        String country
    ) {
        session.upsert(dataSet.ids(fixture.keyPrefix + keyNum))
            .bins(AGE_BIN, SCORE_BIN, COUNTRY_BIN)
            .values(age, score, country)
            .execute();
    }

    static List<Integer> collectAges(RecordStream rs, String ageBin) {
        try (rs) {
            List<Integer> ages = new ArrayList<>();
            while (rs.hasNext()) {
                ages.add(rs.next().recordOrThrow().getInt(ageBin));
            }
            ages.sort(Integer::compareTo);
            return ages;
        }
    }

    static int countRecords(RecordStream rs) {
        try (rs) {
            int count = 0;
            while (rs.hasNext()) {
                rs.next().recordOrThrow();
                count++;
            }
            return count;
        }
    }

    static void createIndexQuietly(
        Session session,
        DataSet dataSet,
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

    static void createIndexQuietly(
        Session session,
        DataSet dataSet,
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

    static void createExpIndexQuietly(
        Session session,
        DataSet dataSet,
        String indexName,
        IndexType indexType,
        Expression exp
    ) {
        try {
            session.createIndex(dataSet, indexName, indexType, IndexCollectionType.DEFAULT, exp)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    static void dropIndexQuietly(DataSet dataSet, String indexName) {
        dropIndexQuietly(ClusterTest.session, dataSet, indexName);
    }

    static void dropIndexQuietly(Session session, DataSet dataSet, String indexName) {
        try {
            session.dropIndex(dataSet, indexName);
        }
        catch (AerospikeException ignored) {
        }
    }

    /**
     * Waits for the drop rather than only dispatching it. Use when indexes are dropped and
     * recreated in the same test class so a pending drop cannot race a later create.
     */
    static void dropIndexQuietlyAndWait(Session session, DataSet dataSet, String indexName) {
        try {
            session.dropIndex(dataSet, indexName).waitTillComplete();
        }
        catch (AerospikeException ignored) {
        }
    }
}
