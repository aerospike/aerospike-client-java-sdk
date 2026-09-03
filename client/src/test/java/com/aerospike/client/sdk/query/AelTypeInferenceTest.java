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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

/**
 * Server-backed tests for AEL bin type inference.
 *
 * <p>A bin path in AEL text carries no type of its own. The server resolves it from the
 * surrounding context — a literal, an operator that forces a type, or an explicit
 * {@code :T} pin. If a bin is still untyped once the expression has been parsed, the
 * server refuses to guess a default and rejects the whole expression with
 * {@link ResultCode#PARAMETER_ERROR}.
 *
 * <p>That rule makes two innocuous-looking expressions illegal: a bare bin path
 * ({@code $.age}) and arithmetic or comparison between two bins ({@code $.age + $.value}),
 * because neither offers anything to infer from. The same expression with a literal or a
 * {@code :T} pin is accepted. These tests pin the contract down for all three AEL
 * surfaces — read expressions, write expressions, and filters — since the existing AEL
 * coverage happens to use literal-anchored expressions throughout and so never exercises
 * the unresolved case.
 */
public class AelTypeInferenceTest extends ClusterTest {
    private static final String KEY = "ael_type_inference";

    private Key key;

    @BeforeAll
    public static void requireAelServer() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        session.upsert(key)
            .bin("age").setTo(21)
            .bin("value").setTo(123)
            .execute();
    }

    // --- expressions the server cannot type ---

    @Test
    public void bareBinPathIsRejected() {
        assertParameterError(() -> selectValue("$.age"));
    }

    @Test
    public void arithmeticBetweenTwoBinsIsRejected() {
        // Both operands are untyped, so the server knows only that they are numeric —
        // not whether they are INT or FLOAT.
        assertParameterError(() -> selectValue("$.age + $.value"));
    }

    @Test
    public void comparisonBetweenTwoBinsIsRejectedInFilter() {
        assertParameterError(() -> {
            try (RecordStream rs = session.query(key)
                .bin("name").get()
                .where("$.age > $.value")
                .execute()) {
                rs.hasNext();
            }
        });
    }

    @Test
    public void arithmeticBetweenTwoBinsIsRejectedInWriteExpression() {
        assertParameterError(() -> session.upsert(key)
            .bin("sum").upsertFrom("$.age + $.value")
            .execute());
    }

    // --- resolving via a literal ---

    @Test
    public void literalOperandResolvesTheBin() {
        assertEquals(41L, selectLong("$.age + 20"));
    }

    @Test
    public void trailingLiteralResolvesBothBins() {
        // "+ 0" is semantically a no-op but gives the parser the anchor it needs.
        assertEquals(144L, selectLong("$.age + $.value + 0"));
    }

    // --- resolving via an explicit :T pin ---

    @Test
    public void pinResolvesBareBinPath() {
        assertEquals(21L, selectLong("$.age:INT"));
    }

    @Test
    public void pinningOneOperandResolvesTheOther() {
        // Only one side needs the pin — "value" inherits INT through the operator.
        assertEquals(144L, selectLong("$.age:INT + $.value"));
    }

    @Test
    public void pinningBothOperandsIsValid() {
        assertEquals(144L, selectLong("$.age:INT + $.value:INT"));
    }

    @Test
    public void pinResolvesFilterComparingTwoBins() {
        try (RecordStream rs = session.query(key)
            .bin("age").get()
            .where("$.value:INT > $.age")
            .execute()) {
            assertTrue(rs.hasNext(), "record should match: value 123 > age 21");
            assertEquals(21L, rs.next().recordOrThrow().getLong("age"));
        }
    }

    @Test
    public void pinResolvesWriteExpression() {
        session.upsert(key)
            .bin("sum").upsertFrom("$.age:INT + $.value")
            .execute();

        try (RecordStream rs = session.query(key).bin("sum").get().execute()) {
            assertTrue(rs.hasNext());
            assertEquals(144L, rs.next().recordOrThrow().getLong("sum"));
        }
    }

    // --- helpers ---

    private void assertParameterError(Runnable body) {
        AerospikeException ex = assertThrows(AerospikeException.class, body::run);
        assertEquals(ResultCode.PARAMETER_ERROR, ex.getResultCode(),
            () -> "expected the server to reject the unresolved bin type, got: " + ex.getMessage());
    }

    private long selectLong(String ael) {
        Object value = selectValue(ael);
        assertNotNull(value, () -> "null result for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private Object selectValue(String ael) {
        try (RecordStream rs = session.query(key)
            .bin("out")
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            return rec.getValue("out");
        }
    }
}
