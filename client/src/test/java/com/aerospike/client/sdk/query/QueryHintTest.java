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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

/**
 * Tests for the {@link QueryHint} type-state API.
 *
 * <p>These tests validate both the runtime behaviour (correct values are captured)
 * and, by their existence, that the type-state transitions compile. Invalid transitions
 * (e.g. {@code forIndex().forBin()}) are enforced by the compiler and cannot be tested
 * at runtime.</p>
 */
public class QueryHintTest {

    // -- forIndex paths -------------------------------------------------------

    @Test
    public void forIndexOnly() {
        QueryHint.Result result = apply(hint -> hint.forIndex("my_idx"));
        assertEquals("my_idx", result.getIndexName());
        assertNull(result.getBinName());
        assertNull(result.getQueryDuration());
    }

    @Test
    public void forIndexThenQueryDuration() {
        QueryHint.Result result = apply(hint ->
            hint.forIndex("my_idx").queryDuration(QueryDuration.SHORT));
        assertEquals("my_idx", result.getIndexName());
        assertNull(result.getBinName());
        assertEquals(QueryDuration.SHORT, result.getQueryDuration());
    }

    // -- forBin paths ---------------------------------------------------------

    @Test
    public void forBinOnly() {
        QueryHint.Result result = apply(hint -> hint.forBin("age"));
        assertNull(result.getIndexName());
        assertEquals("age", result.getBinName());
        assertNull(result.getQueryDuration());
    }

    @Test
    public void forBinThenQueryDuration() {
        QueryHint.Result result = apply(hint ->
            hint.forBin("age").queryDuration(QueryDuration.LONG_RELAX_AP));
        assertNull(result.getIndexName());
        assertEquals("age", result.getBinName());
        assertEquals(QueryDuration.LONG_RELAX_AP, result.getQueryDuration());
    }

    // -- queryDuration paths --------------------------------------------------

    @Test
    public void queryDurationOnly() {
        QueryHint.Result result = apply(hint ->
            hint.queryDuration(QueryDuration.SHORT));
        assertNull(result.getIndexName());
        assertNull(result.getBinName());
        assertEquals(QueryDuration.SHORT, result.getQueryDuration());
    }

    @Test
    public void queryDurationThenForIndex() {
        QueryHint.Result result = apply(hint ->
            hint.queryDuration(QueryDuration.LONG).forIndex("idx_name"));
        assertEquals("idx_name", result.getIndexName());
        assertNull(result.getBinName());
        assertEquals(QueryDuration.LONG, result.getQueryDuration());
    }

    @Test
    public void queryDurationThenForBin() {
        QueryHint.Result result = apply(hint ->
            hint.queryDuration(QueryDuration.SHORT).forBin("score"));
        assertNull(result.getIndexName());
        assertEquals("score", result.getBinName());
        assertEquals(QueryDuration.SHORT, result.getQueryDuration());
    }

    // -- empty hint (no-op) ---------------------------------------------------

    @Test
    public void emptyHint() {
        QueryHint.Result result = apply(hint -> hint);
        assertNull(result.getIndexName());
        assertNull(result.getBinName());
        assertNull(result.getQueryDuration());
    }

    // -- probe path index-name hint -------------------------------------------

    @Test
    public void probeIndexNameHintUsesForIndex() {
        QueryHint.Result result = apply(hint -> hint.forIndex("age_idx"));
        assertEquals("age_idx", IndexProbePlanner.indexNameHintForProbe(result));
    }

    @Test
    public void probeIndexNameHintIgnoresForBin() {
        QueryHint.Result result = apply(hint -> hint.forBin("age"));
        assertNull(IndexProbePlanner.indexNameHintForProbe(result));
    }

    @Test
    public void probeIndexNameHintIgnoresBlankForIndex() {
        QueryHint.Result result = apply(hint -> hint.forIndex("  "));
        assertNull(IndexProbePlanner.indexNameHintForProbe(result));
    }

    @Test
    public void probeIndexNameHintNullWhenNoHint() {
        assertNull(IndexProbePlanner.indexNameHintForProbe(null));
    }

    @Test
    public void requireIndexOnly() {
        QueryHint.Result result = apply(hint -> hint.requireIndex());
        assertTrue(result.isRequireIndex());
        assertFalse(result.isHardHint());
    }

    @Test
    public void forIndexThenHardHint() {
        QueryHint.Result result = apply(hint -> hint.forIndex("my_idx").hardHint());
        assertEquals("my_idx", result.getIndexName());
        assertTrue(result.isHardHint());
        assertFalse(result.isRequireIndex());
    }

    @Test
    public void requireIndexForIndexHardHint() {
        QueryHint.Result result = apply(hint ->
            hint.requireIndex().forIndex("my_idx").hardHint());
        assertTrue(result.isRequireIndex());
        assertTrue(result.isHardHint());
        assertEquals("my_idx", result.getIndexName());
    }

    @Test
    public void hardHintWithoutForIndexThrows() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
            () -> apply(hint -> hint.queryDuration(QueryDuration.SHORT).forIndex("  ").hardHint()));
    }

    @Test
    public void explainWhereFlagsRequireIndex() {
        QueryHint.Result result = apply(hint -> hint.requireIndex());
        assertEquals(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
            IndexProbePlanner.explainWhereFlags(result));
    }

    @Test
    public void explainWhereFlagsHardHint() {
        QueryHint.Result result = apply(hint -> hint.forIndex("age_idx").hardHint());
        assertEquals(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT,
            IndexProbePlanner.explainWhereFlags(result));
    }

    @Test
    public void explainWhereFlagsRequireIndexAndHardHint() {
        QueryHint.Result result = apply(hint ->
            hint.forIndex("age_idx").requireIndex().hardHint());
        assertEquals(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX
                | QueryWhereWire.FLAG_HARD_HINT,
            IndexProbePlanner.explainWhereFlags(result));
    }

    // -- helper ---------------------------------------------------------------

    private static QueryHint.Result apply(
            Function<QueryHint.Start, ? extends QueryHint.Result> configurator) {
        QueryHint.Start start = QueryHint.create();
        return configurator.apply(start);
    }
}
