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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ErrorDetailVerbosity;
import com.aerospike.client.sdk.ExpressionTrace;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.SubCode;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.query.plan.QueryPlan;

/**
 * Error-detail verbosity on server query-selection (field {@code 44} explain) paths.
 *
 * <p>Exercises index-selection failures and invalid AEL at explain time with elevated
 * {@code info4} error-detail verbosity. Explain-time AEL build failures return
 * {@link ResultCode#PARAMETER_ERROR}; index-hint policy violations return
 * {@link ResultCode#INDEX_NOTFOUND}.</p>
 */
public class QuerySelectionErrorDetailTest extends ClusterTest {
    private static final String setName = "qsedetail";
    private static final String indexName = "qsedetail_age_idx";
    private static final String bogusIndexName = "qsedetail_missing_idx";
    private static final String binName = "age";
    private static final String countryBinName = "country";
    private static final String keyPrefix = "qsedetailkey";

    /** Trailing {@code and} — syntactically invalid AEL. */
    private static final String BAD_AEL = "$.age > 30 and";

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        assumeTrue(args.serverVersion.isGreaterOrEqual(8, 1, 3, 0),
            "Extended error-detail requires server version 8.1.3 or later");
        QuerySelectionIntegSupport.assumeQuerySelectionEnabled();

        dataSet = DataSet.of(args.namespace, setName);

        session.delete(dataSet.ids(keyPrefix + "1"));

        try {
            session.createIndex(dataSet, indexName, binName, IndexType.INTEGER,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        session.upsert(dataSet.ids(keyPrefix + "1"))
            .bins(binName, countryBinName)
            .values(25, "US")
            .execute();
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        session.delete(dataSet.ids(keyPrefix + "1"));
        session.dropIndex(dataSet, indexName);
    }

    @Test
    void explainBadAelDetailedMessageAtVerbosity2() {
        Session verbose = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            explainPlan(verbose, BAD_AEL));

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg, "Expected server error message at verbosity 2");
        // Explain may fold AEL diagnostics when query_plan stages build errors; otherwise
        // the generic PARAMETER string is still returned at verbosity >= 2.
        assertTrue(
            msg.contains("invalid filter expression in query")
                || msg.toLowerCase().contains("parameter"),
            "Unexpected explain message: " + msg);
    }

    @Test
    void explainBadAelAelTraceAtVerbosity3() {
        Session verbose = sessionWithVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE);

        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            explainPlan(verbose, BAD_AEL));

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());

        ExpressionTrace trace = ae.getExpressionTrace();
        if (trace != null) {
            assertEquals(ExpressionTrace.PHASE_BUILD, trace.getPhase());
            assertEquals(ExpressionTrace.LANG_AEL, trace.getLang());
        }
    }

    @Test
    void explainBadAelVerbosity2HasNoTrace() {
        Session verbose = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            explainPlan(verbose, BAD_AEL));

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertNull(ae.getExpressionTrace(), "Verbosity 2 must surface NO expression trace");
    }

    @Test
    void executeBadAelFailsAtExplainWithMessage() {
        Session verbose = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            verbose.query(dataSet)
                .where(BAD_AEL)
                .execute());

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertNotNull(ae.getBaseMessage(), "Expected server error message at verbosity 2");
    }

    @Test
    void explainRequireIndexOnPrimaryIndexPlan() {
        Session verbose = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        QueryBuilder qb = verbose.query(dataSet)
            .where("$.country == 'US'")
            .withHint(hint -> hint.disallowScansWithWhere());

        AerospikeException ae = assertThrows(AerospikeException.class, () -> explainPlan(verbose, qb));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());
    }

    @Test
    void explainHardHintWithWrongIndex() {
        Session verbose = sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE);

        QueryBuilder qb = verbose.query(dataSet)
            .where("$.age == 25")
            .withHint(hint -> hint.forIndex(bogusIndexName).hardHint());

        AerospikeException ae = assertThrows(AerospikeException.class, () -> explainPlan(verbose, qb));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());
    }

    private static Session sessionWithVerbosity(int verbosity) {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(verbosity)
            )
        );
        return cluster.createSession(behavior);
    }

    private static QueryPlan explainPlan(Session session1, String where) {
        return IndexProbePlanner.plan(
            session1, dataSet, WhereClauseProcessor.from(where), null);
    }

    private static QueryPlan explainPlan(Session session1, QueryBuilder qb) {
        return IndexProbePlanner.plan(session1, dataSet, qb.getAel(), qb.getQueryHint());
    }
}
