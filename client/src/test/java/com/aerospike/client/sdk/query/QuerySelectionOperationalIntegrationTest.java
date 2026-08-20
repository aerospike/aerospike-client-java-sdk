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
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/**
 * Operational scenarios for server-led query selection.
 */
class QuerySelectionOperationalIntegrationTest extends ClusterTest {
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
        QuerySelectionIntegSupport.assumeQuerySelectionEnabled();
        dataSet = prepareQselint(session, args.namespace);
        scopeDataSet = DataSet.of(args.namespace, scopeSetName);
        namespaceDataSet = DataSet.of(args.namespace, null);

        session.delete(scopeDataSet.ids(scopeKeyPrefix + "1")).execute();
        session.delete(scopeDataSet.ids(scopeKeyPrefix + "2")).execute();
        createIndexQuietly(scopeDataSet, setScopedIndex, setScopedBin);
        createIndexQuietly(namespaceDataSet, namespaceScopedIndex, namespaceScopedBin);

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
        QuerySelectionIntegSupport.destroyQselint(session, dataSet);
        if (scopeDataSet != null) {
            session.delete(scopeDataSet.ids(scopeKeyPrefix + "1")).execute();
            session.delete(scopeDataSet.ids(scopeKeyPrefix + "2")).execute();
            QueryPlannerSupport.dropIndexQuietly(scopeDataSet, setScopedIndex);
            QueryPlannerSupport.dropIndexQuietly(namespaceDataSet, namespaceScopedIndex);
        }
    }

    /**
     * A partition-restricted query still uses server selection and returns a subset of the
     * unrestricted result.
     *
     * <p>Asserted as a partition of the result rather than as "the lower range returns something":
     * only five records match, spread over 4096 partitions by digest, so any particular range may
     * legitimately be empty. Two complementary ranges must together yield exactly the unrestricted
     * result, which holds for any key distribution while still catching a range that drops or
     * duplicates rows.</p>
     */
    @Test
    void executeWithPartitionRangeAndWhereReturnsMatchingSubset() {
        String where = "$.age >= 14 and $.age <= 18";

        List<Integer> full = matchingAges(where);
        List<Integer> lower = matchingAges(where, 0, 2048);
        List<Integer> upper = matchingAges(where, 2048, 4096);

        List<Integer> union = new ArrayList<>(lower);
        union.addAll(upper);
        union.sort(Integer::compareTo);

        assertAll("partitionRestricted",
            () -> assertEquals(List.of(14, 15, 16, 17, 18), full),
            () -> assertEquals(full, union),
            () -> assertTrue(Collections.disjoint(lower, upper),
                "a record must not be returned by both partition ranges"));
    }

    private static List<Integer> matchingAges(String where) {
        return collectAges(session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute(), AGE_BIN);
    }

    private static List<Integer> matchingAges(String where, int startIncl, int endExcl) {
        return collectAges(session.query(dataSet)
            .onPartitionRange(startIncl, endExcl)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .execute(), AGE_BIN);
    }

    /**
     * chunked execute returns the full matching set without re-planning between chunks.
     */
    @Test
    void chunkedExecuteReturnsFullMatchingSet() {
        String where = "$.age >= 14 and $.age <= 18";
        QueryPlan plan = QueryPlannerSupport.plan(dataSet, where);

        List<Integer> ages = new ArrayList<>();
        RecordStream rs = session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(where)
            .chunkSize(2)
            .execute();

        try {
            while (rs.hasMoreChunks()) {
                while (rs.hasNext()) {
                    ages.add(rs.next().recordOrThrow().getInt(AGE_BIN));
                }
            }
            ages.sort(Integer::compareTo);
        }
        finally {
            rs.close();
        }

        assertAll("chunkedExecute",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(List.of(14, 15, 16, 17, 18), ages));
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

        assertAll("preparedAel",
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

        QueryPlan setPlan = QueryPlannerSupport.plan(scopeDataSet, setWhere);
        QueryPlan namespacePlan = QueryPlannerSupport.plan(namespaceDataSet, namespaceWhere);
        List<Integer> setValues = collectAges(session.query(scopeDataSet)
            .readingOnlyBins(setScopedBin)
            .where(setWhere)
            .execute(), setScopedBin);
        List<Integer> namespaceValues = collectAges(session.query(namespaceDataSet)
            .readingOnlyBins(namespaceScopedBin)
            .where(namespaceWhere)
            .execute(), namespaceScopedBin);

        assertAll("indexScopes",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, setPlan.getSelection()),
            () -> assertEquals(setScopedIndex, setPlan.getIndexName()),
            () -> assertEquals(List.of(101, 102), setValues),
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, namespacePlan.getSelection()),
            () -> assertEquals(namespaceScopedIndex, namespacePlan.getIndexName()),
            () -> assertEquals(List.of(201, 202), namespaceValues));
    }

    private static void createIndexQuietly(DataSet target, String indexName, String binName) {
        try {
            session.createIndex(target, indexName, binName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }
}
