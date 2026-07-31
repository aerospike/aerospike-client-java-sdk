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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.function.IntFunction;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;

/** Shared helpers for field {@code 44} query-planner integration tests. */
final class QueryPlannerSupport {
    private QueryPlannerSupport() {
    }

    static void assumeQuerySelection() {
        assumeTrue(ClusterTest.cluster.supportsQuerySelection(),
            "server does not support query selection");
    }

    static QueryPlan plan(DataSet dataSet, String where) {
        return IndexProbePlanner.plan(
            ClusterTest.session,
            dataSet,
            WhereClauseProcessor.from(where),
            null);
    }

    static void assertPlan(
        QueryPlan plan,
        QuerySelection expectedSelection,
        String expectedIndexName,
        boolean expectIndexRange
    ) {
        assertAll("queryPlan",
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

    static void deleteKeys(DataSet dataSet, IntFunction<String> keyFn, int fromInclusive, int toInclusive) {
        for (int i = fromInclusive; i <= toInclusive; i++) {
            ClusterTest.session.delete(dataSet.ids(keyFn.apply(i)));
        }
    }

    static void dropIndexQuietly(DataSet dataSet, String indexName) {
        try {
            ClusterTest.session.dropIndex(dataSet, indexName);
        }
        catch (AerospikeException ignored) {
        }
    }
}
