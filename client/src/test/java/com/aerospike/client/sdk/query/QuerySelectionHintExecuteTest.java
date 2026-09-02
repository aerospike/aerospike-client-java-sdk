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
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assumeQuerySelection;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.destroyQselint;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.query.QuerySelectionIntegSupport.Fixture;

/**
 * Hint contracts as an application observes them: rows returned from {@code execute()}, or the
 * {@link AerospikeException} it throws.
 *
 * <p>{@link QuerySelectionHintFlagsTest} covers successful hint combinations against the explain
 * response (wire flags and index choice). These tests pin what the application actually sees from
 * {@code execute()}: thrown errors for strict-hint violations, soft-hint fallback parity, and
 * duration hints. Happy-path strict hints that only return {@code ageRangeRows} are not repeated
 * here — wire-level coverage there is enough unless {@code execute()} diverges from explain.</p>
 *
 * <p>The two halves are read together: a <em>soft</em> hint that cannot be honored must fall back
 * silently and return the same rows as no hint at all, whereas the same hint marked
 * {@code hardHint()} must fail. That pairing is what pins {@code hardHint()} as the thing that
 * converts a fallback into an error, rather than either behavior being incidental.</p>
 */
public class QuerySelectionHintExecuteTest extends ClusterTest {
    private static final Fixture FIXTURE = Fixture.forSuffix("hintexec");
    private static final String bogusIndexName = "qselhintexec_missing_idx";
    private static final String ageRangeWhere = "$." + AGE_BIN + " >= 14 and $." + AGE_BIN + " <= 18";
    private static final List<Integer> ageRangeRows = List.of(14, 15, 16, 17, 18);

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

    // ---------------------------------------------------------------- strict hints must be enforced

    /**
     * Hard hint naming an index that exists but cannot serve the predicate (it indexes a different
     * bin) must fail rather than quietly falling back.
     */
    @Test
    void executeHardHintOnWrongIndexThrowsIndexNotFound() {
        AerospikeException e = assertThrows(AerospikeException.class, () ->
            ageRows(hint -> hint.forIndex(FIXTURE.scoreIndex).hardHint()));

        assertEquals(ResultCode.INDEX_NOTFOUND, e.getResultCode());
    }

    /** Hard hint naming an index that does not exist must fail, not fall back to the planner. */
    @Test
    void executeHardHintOnNonExistentIndexThrowsIndexNotFound() {
        AerospikeException e = assertThrows(AerospikeException.class, () ->
            ageRows(hint -> hint.forIndex(bogusIndexName).hardHint()));

        assertEquals(ResultCode.INDEX_NOTFOUND, e.getResultCode());
    }

    /**
     * {@code disallowScansWithWhere()} forbids the primary-index fallback, so a predicate on an unindexed bin
     * must fail instead of scanning.
     */
    @Test
    void executeRequireIndexWhenOnlyPrimaryIndexPossibleThrows() {
        AerospikeException e = assertThrows(AerospikeException.class, () ->
            collectAges(session.query(dataSet)
                .readingOnlyBins(COUNTRY_BIN)
                .where("$." + COUNTRY_BIN + " == 'US'")
                .withHint(hint -> hint.disallowScansWithWhere())
                .execute(), COUNTRY_BIN));

        assertEquals(ResultCode.INDEX_NOTFOUND, e.getResultCode());
    }

    // ------------------------------------------------------------- soft hints must never break you

    /**
     * An unhonorable soft hint is advice, not a constraint: naming an index that does not exist must
     * be indistinguishable from passing no hint.
     */
    @Test
    void executeSoftHintOnMissingIndexReturnsSameRowsAsNoHint() {
        List<Integer> unhinted = ageRows(null);
        List<Integer> hinted = ageRows(hint -> hint.forIndex(bogusIndexName));

        assertAll(
            () -> assertEquals(ageRangeRows, unhinted),
            () -> assertEquals(unhinted, hinted));
    }

    /** Same tolerance when the named index exists but indexes a bin the predicate does not use. */
    @Test
    void executeSoftHintOnWrongIndexReturnsSameRowsAsNoHint() {
        List<Integer> unhinted = ageRows(null);
        List<Integer> hinted = ageRows(hint -> hint.forIndex(FIXTURE.scoreIndex));

        assertAll(
            () -> assertEquals(ageRangeRows, unhinted),
            () -> assertEquals(unhinted, hinted));
    }

    /**
     * {@code disallowScansWithWhere()} constrains the <em>plan</em>, not the hint: the unusable soft hint is
     * discarded, the planner still finds a secondary index, so the flag is satisfied and rows come
     * back. Contrast with {@code executeHardHintOnNonExistentIndexThrowsIndexNotFound}, where the
     * same missing index is fatal.
     */
    @Test
    void executeRequireIndexWithMissingHintStillReturnsRows() {
        List<Integer> ages = ageRows(hint -> hint.disallowScansWithWhere().forIndex(bogusIndexName));

        assertEquals(ageRangeRows, ages);
    }

    // ------------------------------------------------------------------------- expected duration

    /** {@code LONG} advertises a long-running query; it must not change the result set. */
    @Test
    void executeQueryDurationLongReturnsMatchingRows() {
        List<Integer> ages = ageRows(hint -> hint.queryDuration(QueryDuration.LONG));

        assertEquals(ageRangeRows, ages);
    }

    /** {@code LONG_RELAX_AP} relaxes AP-mode guarantees; on a healthy cluster rows are unchanged. */
    @Test
    void executeQueryDurationLongRelaxApReturnsMatchingRows() {
        List<Integer> ages = ageRows(hint -> hint.queryDuration(QueryDuration.LONG_RELAX_AP));

        assertEquals(ageRangeRows, ages);
    }

    /** Runs {@link #ageRangeWhere} over the public path; a {@code null} configurator means no hint. */
    private static List<Integer> ageRows(
        Function<QueryHint.Start, ? extends QueryHint.Result> configurator
    ) {
        QueryBuilder qb = session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(ageRangeWhere);

        if (configurator != null) {
            qb = qb.withHint(configurator);
        }
        return collectAges(qb.execute(), AGE_BIN);
    }
}
