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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

/**
 * Tier D integration tests: {@code REQUIRE_INDEX} and {@code HARD_HINT} on field {@code 44} explain.
 */
class QuerySelectionHintFlagsTest extends ClusterTest {
    private static final String setName = "qselhint";
    private static final String indexName = "qselhint_age_idx";
    private static final String scoreIndexName = "qselhint_score_idx";
    private static final String bogusIndexName = "qselhint_missing_idx";
    private static final String binName = "age";
    private static final String scoreBinName = "score";
    private static final String countryBinName = "country";
    private static final String keyPrefix = "qselhintkey";

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        assumeTrue(cluster.supportsQuerySelection(), "server does not support query selection");

        dataSet = DataSet.of(args.namespace, setName);

        session.delete(dataSet.ids(keyPrefix + "1")).execute();
        session.delete(dataSet.ids(keyPrefix + "2")).execute();

        try {
            session.createIndex(dataSet, indexName, binName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        try {
            session.createIndex(dataSet, scoreIndexName, scoreBinName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

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

    /** D.3 — {@code REQUIRE_INDEX} on PI-eligible WHERE rejects explain. */
    @Test
    void requireIndexOnPrimaryIndexPlanFailsExplain() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.country == 'US'")
            .withHint(hint -> hint.requireIndex());

        AerospikeException e = assertThrows(AerospikeException.class, () -> explainPlan(qb));
        assertEquals(ResultCode.INDEX_NOTFOUND, e.getResultCode());
    }

    /** D.4 — {@code REQUIRE_INDEX} + soft {@code forIndex} still selects a secondary index. */
    @Test
    void requireIndexWithSoftHintSelectsSecondaryIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.requireIndex().forIndex(scoreIndexName));

        QueryPlan plan = explainPlan(qb);

        assertAll("requireIndexSoftHint",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertNotNull(plan.getIndexRangeBytes()),
            () -> assertEquals(
                QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
                QueryWhereWire.flags(plan.getExplainWhereBytes())));
    }

    /** D.6 — {@code HARD_HINT} + matching {@code forIndex} selects that index. */
    @Test
    void hardHintWithMatchingIndexSelectsHintedIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.forIndex(indexName).hardHint());

        QueryPlan plan = explainPlan(qb);

        assertAll("hardHintMatch",
            () -> assertEquals(QuerySelection.SECONDARY_INDEX, plan.getSelection()),
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertEquals(
                QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT,
                QueryWhereWire.flags(plan.getExplainWhereBytes())));
    }

    /** D.7 — both flags with index hint. */
    @Test
    void requireIndexAndHardHintSelectsHintedIndex() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.forIndex(indexName).requireIndex().hardHint());

        QueryPlan plan = explainPlan(qb);

        assertAll("bothFlags",
            () -> assertEquals(indexName, plan.getIndexName()),
            () -> assertEquals(
                QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX
                    | QueryWhereWire.FLAG_HARD_HINT,
                QueryWhereWire.flags(plan.getExplainWhereBytes())));
    }

    /** D.8 — {@code HARD_HINT} with wrong index name fails explain. */
    @Test
    void hardHintWithWrongIndexFailsExplain() {
        QueryBuilder qb = session.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.forIndex(bogusIndexName).hardHint());

        AerospikeException e = assertThrows(AerospikeException.class, () -> explainPlan(qb));
        assertEquals(ResultCode.INDEX_NOTFOUND, e.getResultCode());
    }

    /**
     * D.9 — syntactically invalid AEL fails explain with {@code PARAMETER}.
     * (Unknown bin with valid syntax returns PI, not {@code PARAMETER}.)
     */
    @Test
    void badAelFailsExplainWithParameter() {
        AerospikeException e = assertThrows(AerospikeException.class,
            () -> explainPlan("$.age > 30 and"));
        assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
    }

    private static QueryPlan explainPlan(String where) {
        return IndexProbePlanner.plan(
            session, dataSet, WhereClauseProcessor.from(where), null);
    }

    private static QueryPlan explainPlan(QueryBuilder qb) {
        return IndexProbePlanner.plan(session, dataSet, qb.getAel(), qb.getQueryHint());
    }
}
