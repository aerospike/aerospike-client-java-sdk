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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.policy.QueryDuration;

/**
 * Tests for the {@link QueryHint} type-state API.
 *
 * <p>Invalid transitions (e.g. {@code forIndex().forBin()}) are enforced by the compiler and
 * cannot be tested at runtime.</p>
 */
class QueryHintTest {

    @ParameterizedTest
    @MethodSource("hintCaptureCases")
    void hintCapturesConfiguredValues(
        Function<QueryHint.Start, ? extends QueryHint.Result> configurator,
        String indexName,
        String binName,
        QueryDuration duration,
        Boolean allowScansWithWhere
    ) {
        QueryHint.Result result = configurator.apply(QueryHint.create());

        assertEquals(indexName, result.getIndexName());
        assertEquals(binName, result.getBinName());
        assertEquals(duration, result.getQueryDuration());
        assertEquals(allowScansWithWhere, result.getAllowScansWithWhere());
    }

    @ParameterizedTest
    @MethodSource("probeIndexNameCases")
    void probeIndexNameHint(
        Function<QueryHint.Start, ? extends QueryHint.Result> configurator,
        String expected
    ) {
        QueryHint.Result result = configurator != null ? configurator.apply(QueryHint.create()) : null;
        assertEquals(expected, IndexProbePlanner.indexNameHintForProbe(result));
    }

    @Test
    void disallowScansWithWhereOnly() {
        QueryHint.Result result = QueryHint.create().disallowScansWithWhere();
        assertEquals(false, result.getAllowScansWithWhere());
        assertFalse(result.isHardHint());
    }

    @Test
    void forIndexThenHardHint() {
        QueryHint.Result result = QueryHint.create().forIndex("my_idx").hardHint();
        assertEquals("my_idx", result.getIndexName());
        assertTrue(result.isHardHint());
        assertNull(result.getAllowScansWithWhere());
    }

    @Test
    void disallowScansWithWhereForIndexHardHint() {
        QueryHint.Result result = QueryHint.create()
            .disallowScansWithWhere().forIndex("my_idx").hardHint();
        assertEquals(false, result.getAllowScansWithWhere());
        assertTrue(result.isHardHint());
        assertEquals("my_idx", result.getIndexName());
    }

    @Test
    void hardHintWithoutForIndexThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            QueryHint.create().queryDuration(QueryDuration.SHORT).forIndex("  ").hardHint());
    }

    private static Stream<Arguments> hintCaptureCases() {
        return Stream.of(
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint -> hint.forIndex("my_idx"),
                "my_idx", null, null, null),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint ->
                    hint.forIndex("my_idx").queryDuration(QueryDuration.SHORT),
                "my_idx", null, QueryDuration.SHORT, null),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint -> hint.forBin("age"),
                null, "age", null, null),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint ->
                    hint.forBin("age").queryDuration(QueryDuration.LONG_RELAX_AP),
                null, "age", QueryDuration.LONG_RELAX_AP, null),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint ->
                    hint.queryDuration(QueryDuration.SHORT).allowScansWithWhere(),
                null, null, QueryDuration.SHORT, true),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint ->
                    hint.queryDuration(QueryDuration.LONG).allowScansWithWhere().forIndex("idx_name"),
                "idx_name", null, QueryDuration.LONG, true),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint ->
                    hint.queryDuration(QueryDuration.SHORT).allowScansWithWhere().forBin("score"),
                null, "score", QueryDuration.SHORT, true),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint -> hint,
                null, null, null, null)
        );
    }

    private static Stream<Arguments> probeIndexNameCases() {
        return Stream.of(
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint -> hint.forIndex("age_idx"),
                "age_idx"),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint -> hint.forBin("age"),
                null),
            Arguments.of(
                (Function<QueryHint.Start, QueryHint.Result>) hint -> hint.forIndex("  "),
                null),
            Arguments.of(null, null)
        );
    }
}
