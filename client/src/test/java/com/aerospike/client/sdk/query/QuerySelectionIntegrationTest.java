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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Integration tests for two-phase server query selection (probe → execute).
 *
 * <p>Requires a server build with {@code INFO4_QUERY_SELECTION} support.</p>
 */
public class QuerySelectionIntegrationTest extends ClusterTest {
    private static final String setName = "qselint";
    private static final String indexName = "qsel_age_idx";
    private static final String bogusIndexName = "qsel_nonexistent_idx";
    private static final String keyPrefix = "qselkey";
    private static final String binName = "age";
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

        for (int i = 1; i <= size; i++) {
            String country = (i % 2 == 0) ? "US" : "CA";
            session.upsert(dataSet.ids(keyPrefix + i))
                .bins(binName, countryBinName)
                .values(i, country)
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
    }

    @Test
    void planSimpleRangeSelectsSecondaryIndex() {
        QueryPlan plan = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .plan();

        assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection());
        assertEquals(args.namespace, plan.getNamespace());
        assertEquals(setName, plan.getSet());
        assertNotNull(plan.getIndexName());
        assertNotNull(plan.getIndexRangeBytes());
        assertNotNull(plan.getPredicateBytes());
        assertEquals(indexName, plan.getIndexName());
    }

    @Test
    void planNonIndexedPredicateSelectsPrimaryIndex() {
        QueryPlan plan = session.query(dataSet)
            .where("$.country == 'US'")
            .plan();

        assertEquals(QuerySelection.PRIMARY_INDEX, plan.getSelection());
        assertEquals(args.namespace, plan.getNamespace());
        assertEquals(setName, plan.getSet());
        assertNull(plan.getIndexName());
        assertNull(plan.getIndexRangeBytes());
        assertNotNull(plan.getPredicateBytes());
    }

    @Test
    void planForIndexHintUsesHintedIndex() {
        QueryPlan plan = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .withHint(hint -> hint.forIndex(indexName))
            .plan();

        assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection());
        assertEquals(indexName, plan.getIndexName());
        assertNotNull(plan.getIndexRangeBytes());
        assertNotNull(plan.getPredicateBytes());
    }

    @Test
    void planBytesStableAcrossRepeatedProbes() {
        String where = "$.age >= 14 and $.age <= 18";

        QueryPlan first = session.query(dataSet).where(where).plan();
        QueryPlan second = session.query(dataSet).where(where).plan();

        assertEquals(QuerySelection.SECONDARY_INDEX, first.getSelection());
        assertEquals(first.getSelection(), second.getSelection());
        assertEquals(indexName, first.getIndexName());
        assertEquals(first.getIndexName(), second.getIndexName());
        assertArrayEquals(first.getPredicateBytes(), second.getPredicateBytes());
        assertArrayEquals(first.getIndexRangeBytes(), second.getIndexRangeBytes());
    }

    @Test
    void indexProbePlannerSmoke() {
        String where = "$.age >= 14 and $.age <= 18";
        WhereClauseProcessor whereClause = WhereClauseProcessor.from(true, where);

        QueryPlan viaPlanner = IndexProbePlanner.plan(session, dataSet, whereClause, null);
        QueryPlan viaBuilder = session.query(dataSet).where(where).plan();

        assertEquals(QuerySelection.SECONDARY_INDEX, viaPlanner.getSelection());
        assertEquals(indexName, viaPlanner.getIndexName());
        assertNotNull(viaPlanner.getIndexRangeBytes());
        assertNotNull(viaPlanner.getPredicateBytes());

        assertEquals(viaPlanner.getSelection(), viaBuilder.getSelection());
        assertEquals(viaPlanner.getIndexName(), viaBuilder.getIndexName());
        assertArrayEquals(viaPlanner.getPredicateBytes(), viaBuilder.getPredicateBytes());
        assertArrayEquals(viaPlanner.getIndexRangeBytes(), viaBuilder.getIndexRangeBytes());
    }

    /**
     * Pins probe behavior when field {@code 21} names an index that does not exist on this set.
     *
     * <p>Observed on query-selection server: probe succeeds; bogus hint is ignored and the
     * server auto-selects the same index as without a hint. Product may later require
     * {@link ResultCode#INDEX_NOTFOUND} instead — update assertions when contract is set.</p>
     */
    @Test
    void planForIndexHintOnNonExistentIndex() {
        QueryPlan plan = session.query(dataSet)
            .where("$.age >= 14 and $.age <= 18")
            .withHint(hint -> hint.forIndex(bogusIndexName))
            .plan();

        assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection());
        assertNotEquals(bogusIndexName, plan.getIndexName());
        assertEquals(indexName, plan.getIndexName());
        assertNotNull(plan.getIndexRangeBytes());
        assertNotNull(plan.getPredicateBytes());
    }

    /**
     * Unsatisfiable range on indexed {@code age} — probe returns {@link QuerySelection#FILTERED_OUT}.
     *
     * <p>Observed on query-selection server: contradiction fails at plan time, not as an empty execute.</p>
     */
    @Test
    void planContradictionPredicate() {
        QueryPlan plan = session.query(dataSet)
            .where("$.age > 100 and $.age < 10")
            .plan();

        assertEquals(QuerySelection.FILTERED_OUT, plan.getSelection());
        assertNotNull(plan.getPredicateBytes());
        assertNull(plan.getIndexName());
        assertNull(plan.getIndexRangeBytes());
    }
}
