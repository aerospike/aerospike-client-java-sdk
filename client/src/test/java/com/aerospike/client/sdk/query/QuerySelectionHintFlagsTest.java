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
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.plan;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

/**
 * Tier D integration tests: {@code REQUIRE_INDEX} and {@code HARD_HINT} on field {@code 44} explain
 * wire flags and successful index selection. Explain-time failures ({@code INDEX_NOTFOUND},
 * {@code PARAMETER_ERROR}) are covered by {@link QuerySelectionErrorDetailTest}.
 */
class QuerySelectionHintFlagsTest extends ClusterTest {
    private static final String setName = "qselhint";
    private static final String indexName = "qselhint_age_idx";
    private static final String scoreIndexName = "qselhint_score_idx";
    private static final String binName = "age";
    private static final String scoreBinName = "score";
    private static final String countryBinName = "country";
    private static final String keyPrefix = "qselhintkey";

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        assumeQuerySelection();

        dataSet = DataSet.of(args.namespace, setName);

        session.delete(dataSet.ids(keyPrefix + "1")).execute();
        session.delete(dataSet.ids(keyPrefix + "2")).execute();

        createIndexQuietly(session, dataSet, indexName, binName, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
        createIndexQuietly(session, dataSet, scoreIndexName, scoreBinName, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);

        session.upsert(dataSet.ids(keyPrefix + "1"))
            .bins(binName, scoreBinName, countryBinName)
            .values(25, 25, "US")
            .execute();

        session.upsert(dataSet.ids(keyPrefix + "2"))
            .bins(binName, scoreBinName, countryBinName)
            .values(30, 30, "CA")
            .execute();
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        session.delete(dataSet.ids(keyPrefix + "1")).execute();
        session.delete(dataSet.ids(keyPrefix + "2")).execute();
        session.dropIndex(dataSet, indexName);
        session.dropIndex(dataSet, scoreIndexName);
    }

    /** D.4 — {@code REQUIRE_INDEX} + soft {@code forIndex} still selects a secondary index. */
    @Test
    void requireIndexWithSoftHintSelectsSecondaryIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.disallowScansWithWhere().forIndex(scoreIndexName));

        QueryPlan queryPlan = plan(dataSet, qb);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, queryPlan.getSelection()),
            () -> assertEquals(indexName, queryPlan.getIndexName()),
            () -> assertNotNull(queryPlan.getIndexRangeBytes()),
            () -> assertEquals(
                QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
                QueryWhereWire.flags(queryPlan.getExplainWhereBytes())));
    }

    /**
     * {@code HARD_HINT} + matching {@code forIndex} selects that index. {@code REQUIRE_INDEX}
     * rides along because the query behavior default is {@code allowScansWithWhere(false)} — so an
     * explicit {@code disallowScansWithWhere().hardHint()} adds no distinct wire shape.
     */
    @Test
    void hardHintWithMatchingIndexSelectsHintedIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.forIndex(indexName).hardHint());

        QueryPlan queryPlan = plan(dataSet, qb);

        assertAll(
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, queryPlan.getSelection()),
            () -> assertEquals(indexName, queryPlan.getIndexName()),
            () -> assertEquals(
                QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX
                    | QueryWhereWire.FLAG_HARD_HINT,
                QueryWhereWire.flags(queryPlan.getExplainWhereBytes())));
    }

    /**
     * The same hint with {@code allowScansWithWhere()} drops {@code REQUIRE_INDEX}, pinning that the
     * flag comes from the scan policy rather than from {@code hardHint}.
     */
    @Test
    void hardHintWithScansAllowedOmitsRequireIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.forIndex(indexName).allowScansWithWhere().hardHint());

        QueryPlan queryPlan = plan(dataSet, qb);

        assertAll(
            () -> assertEquals(indexName, queryPlan.getIndexName()),
            () -> assertEquals(
                QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT,
                QueryWhereWire.flags(queryPlan.getExplainWhereBytes())));
    }
}
