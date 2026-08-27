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
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.dropIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.plan;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.query.QuerySelectionIntegSupport.Fixture;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Query-selection scenarios not covered elsewhere: prepared AEL routing and index scope resolution.
 *
 * <p>Partition-range and chunked execute invariants live in {@link QuerySelectionLifecycleTest} —
 * they exercise pagination/partition routing, not index selection.</p>
 */
public class QuerySelectionOperationalIntegrationTest extends ClusterTest {
    private static final Fixture FIXTURE = Fixture.forSuffix("oper");
    private static final String scopeSetName = "qsel_scope";
    private static final String scopeKeyPrefix = "qselscope";
    private static final String setScopedBin = "qsel_set_value";
    private static final String namespaceScopedBin = "qsel_ns_value";
    private static final String setScopedIndex = "qsel_set_scope_idx";
    private static final String namespaceScopedIndex = "qsel_ns_scope_idx";

    private static DataSet dataSet;
    private static DataSet scopeDataSet;
    private static DataSet namespaceDataSet;

    @BeforeAll
    static void prepare() {
        QuerySelectionIntegSupport.assumeQuerySelection();
        dataSet = prepareQselint(session, args.namespace, FIXTURE);
        scopeDataSet = DataSet.of(args.namespace, scopeSetName);
        namespaceDataSet = DataSet.of(args.namespace, null);

        session.delete(scopeDataSet.ids(scopeKeyPrefix + "1")).execute();
        session.delete(scopeDataSet.ids(scopeKeyPrefix + "2")).execute();
        createIndexQuietly(session, scopeDataSet, setScopedIndex, setScopedBin, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, namespaceDataSet, namespaceScopedIndex, namespaceScopedBin,
            IndexType.INTEGER, IndexCollectionType.DEFAULT);

        session.upsert(scopeDataSet.ids(scopeKeyPrefix + "1"))
            .bins(setScopedBin, namespaceScopedBin)
            .values(101, 201)
            .execute();
        session.upsert(scopeDataSet.ids(scopeKeyPrefix + "2"))
            .bins(setScopedBin, namespaceScopedBin)
            .values(102, 202)
            .execute();
    }

    @AfterAll
    static void destroy() {
        QuerySelectionIntegSupport.destroyQselint(session, dataSet, FIXTURE);
        if (scopeDataSet != null) {
            session.delete(scopeDataSet.ids(scopeKeyPrefix + "1")).execute();
            session.delete(scopeDataSet.ids(scopeKeyPrefix + "2")).execute();
            dropIndexQuietly(scopeDataSet, setScopedIndex);
            dropIndexQuietly(namespaceDataSet, namespaceScopedIndex);
        }
    }

    /** Prepared AEL uses the same field {@code 44} selection path as raw string AEL. */
    @Test
    void preparedAelExecutesWithServerSelection() {
        PreparedAel template = PreparedAel.prepare("$.age >= ?0 and $.age <= ?1");

        QueryPlan plan = IndexProbePlanner.plan(
            session,
            dataSet,
            WhereClauseProcessor.from(template, 14, 18),
            null);

        List<Integer> ages = collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(template, 14, 18)
            .execute(), AGE_BIN);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
    }

    /**
     * The planner resolves set identity independently from the candidate expression. A set query
     * must select its set-scoped index, while an omitted set must select the namespace-wide index.
     * Distinct bins keep both definitions live concurrently and make the assertion order-independent.
     */
    @Test
    void setScopedAndNamespaceWideQueriesSelectMatchingIndexes() {
        String setWhere = "$." + setScopedBin + " >= 101 and $." + setScopedBin + " <= 102";
        String namespaceWhere =
            "$." + namespaceScopedBin + " >= 201 and $." + namespaceScopedBin + " <= 202";

        QueryPlan setPlan = plan(scopeDataSet, setWhere);
        QueryPlan namespacePlan = plan(namespaceDataSet, namespaceWhere);
        List<Integer> setValues = collectAges(session.query(scopeDataSet)
            .readingOnlyBins(setScopedBin)
            .where(setWhere)
            .execute(), setScopedBin);
        List<Integer> namespaceValues = collectAges(session.query(namespaceDataSet)
            .readingOnlyBins(namespaceScopedBin)
            .where(namespaceWhere)
            .execute(), namespaceScopedBin);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, setPlan.getSelection()),
            () -> assertEquals(setScopedIndex, setPlan.getIndexName()),
            () -> assertEquals(List.of(101, 102), setValues),
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, namespacePlan.getSelection()),
            () -> assertEquals(namespaceScopedIndex, namespacePlan.getIndexName()),
            () -> assertEquals(List.of(201, 202), namespaceValues));
    }
}
