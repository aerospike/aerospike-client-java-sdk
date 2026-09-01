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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 * Server-backed AEL integration tests for literals, the {@code in} membership operator,
 * {@code not(...)}, trilean handling and absent-bin semantics.
 *
 * <p>Scoped to complement {@code FilterExpTest}, which already pairs Exp and AEL for the
 * arithmetic, bitwise, {@code min}/{@code max}, {@code when} and {@code let} surface.
 */
public class AelLiteralTest extends ClusterTest {
    private static final String KEY = "ael_literal";
    private static final String BIN_INT = "num";
    private static final String BIN_STR = "s";
    private static final String BIN_LIST = "l";
    private static final String BIN_MAP = "m";
    private static final String BIN_ABSENT = "nosuchbin";

    private Key key;

    @BeforeAll
    public static void requireAelServer() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);

        session.upsert(key)
            .bin(BIN_INT).setTo(42)
            .bin(BIN_STR).setTo("hello")
            .bin(BIN_LIST).setTo(List.of(100, 200, 300))
            .bin(BIN_MAP).setTo(map)
            .execute();
    }

    // --- scalar literals ---

    @Test
    public void nilLiteralYieldsNull() {
        assertNull(rawValue("NIL"));
    }

    @Test
    public void booleanLiteralsCombineWithPredicates() {
        assertTrue(selectBoolean("$." + BIN_INT + ":INT == 42 and true"));
        assertFalse(selectBoolean("$." + BIN_INT + ":INT == 42 and false"));
    }

    @Test
    public void integerLiteralRadixForms() {
        assertTrue(matchesWhere("$." + BIN_INT + ":INT == 0x2A"));
        assertTrue(matchesWhere("$." + BIN_INT + ":INT == 0b101010"));
    }

    @Test
    public void negativeIntegerLiteral() {
        assertEquals(-42L, selectLong("0 - $." + BIN_INT + ":INT"));
        assertTrue(matchesWhere("$." + BIN_INT + ":INT > -1"));
    }

    @Test
    public void hexBlobLiteral() {
        assertEquals("0102", HexFormat.of().formatHex((byte[]) selectValue("x'0102'")));
    }

    @Test
    public void base64BlobLiteral() {
        assertEquals("68656c6c6f", HexFormat.of().formatHex((byte[]) selectValue("b64'aGVsbG8='")));
    }

    @Test
    public void singleAndDoubleQuotedStringsAreEquivalent() {
        assertTrue(matchesWhere("$." + BIN_STR + ":STRING == 'hello'"));
        assertTrue(matchesWhere("$." + BIN_STR + ":STRING == \"hello\""));
    }

    // --- collection literals ---

    @Test
    public void listLiteral() {
        assertEquals(List.of(1L, 2L, 3L), selectLongs("[1, 2, 3]"));
    }

    @Test
    public void mapLiteral() {
        Object value = selectValue("{a: 1, b: 2}");
        assertInstanceOf(Map.class, value);
        assertEquals(2, ((Map<?, ?>) value).size());
    }

    @Test
    public void unsortedListLiteralPreservesWrittenOrder() {
        assertEquals(List.of(3L, 1L, 2L), selectLongs("[3, 1, 2]:UNSORTED"));
    }

    /** KEY_ORDERED is the default for a map literal, so :UNORDERED is the one worth stating. */
    @Test
    public void unorderedMapLiteralAccepted() {
        Object value = selectValue("{b: 2, a: 1}:UNORDERED");
        assertInstanceOf(Map.class, value);
        assertEquals(2, ((Map<?, ?>) value).size());
    }

    /** SORTED is not an accepted list-literal order suffix; UNSORTED is. */
    @Test
    public void sortedListLiteralSuffixRejected() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("[3, 1, 2]:SORTED"));
        assertEquals(ResultCode.PARAMETER_ERROR, ex.getResultCode());
    }

    // --- membership ---

    @Test
    public void inMembershipOverListLiteral() {
        assertTrue(selectBoolean("$." + BIN_INT + ":INT in [1, 42, 3]"));
        assertFalse(selectBoolean("$." + BIN_INT + ":INT in [1, 2, 3]"));
    }

    @Test
    public void inMembershipOverStrings() {
        assertTrue(selectBoolean("$." + BIN_STR + ":STRING in ['hello', 'world']"));
        assertFalse(selectBoolean("$." + BIN_STR + ":STRING in ['nope']"));
    }

    @Test
    public void inMembershipUsableAsWhereFilter() {
        assertTrue(matchesWhere("$." + BIN_INT + ":INT in [41, 42, 43]"));
        assertFalse(matchesWhere("$." + BIN_INT + ":INT in [1, 2]"));
    }

    // --- logical ---

    @Test
    public void notNegatesPredicate() {
        assertTrue(selectBoolean("not($." + BIN_INT + ":INT == 1)"));
        assertFalse(selectBoolean("not($." + BIN_INT + ":INT == 42)"));
    }

    @Test
    public void notRequiresParenthesisedCallForm() {
        assertThrows(AerospikeException.class,
            () -> selectValue("!($." + BIN_INT + ":INT == 1)"));
    }

    @Test
    public void andOrShortCircuitToExpectedTruth() {
        assertTrue(selectBoolean("$." + BIN_INT + ":INT == 42 or $." + BIN_INT + ":INT == 1"));
        assertFalse(selectBoolean("$." + BIN_INT + ":INT == 42 and $." + BIN_INT + ":INT == 1"));
    }

    // --- trilean / absent handling ---

    @Test
    public void unknownIsAbsorbedByOr() {
        assertTrue(selectBoolean("$." + BIN_INT + ":INT == 42 or unknown"));
    }

    @Test
    public void existsDistinguishesPresentFromAbsentBin() {
        assertTrue(selectBoolean("$." + BIN_MAP + ".exists()"));
        assertFalse(selectBoolean("$." + BIN_ABSENT + ".exists()"));
    }

    @Test
    public void existsUsableAsWhereFilter() {
        assertTrue(matchesWhere("$." + BIN_MAP + ".exists()"));
        assertFalse(matchesWhere("$." + BIN_ABSENT + ".exists()"));
    }

    /** A bare bin reference has no resolved type, so it cannot be a projection on its own. */
    @Test
    public void bareUntypedBinReferenceRejected() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_ABSENT));
        assertEquals(ResultCode.PARAMETER_ERROR, ex.getResultCode());
    }

    /** A type-pinned read of an absent bin compiles, then faults at evaluation. */
    @Test
    public void typedReadOfAbsentBinIsNotApplicable() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_ABSENT + ":INT"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    @Test
    public void absentBinDoesNotMatchWhereFilter() {
        assertFalse(matchesWhere("$." + BIN_ABSENT + ":INT == 1"));
    }

    // --- bin type introspection ---

    @Test
    public void typeReportsBinParticleType() {
        assertTrue(matchesWhere("$." + BIN_INT + ".type() == INT"));
        assertTrue(matchesWhere("$." + BIN_STR + ".type() == STRING"));
        assertFalse(matchesWhere("$." + BIN_INT + ".type() == STRING"));
    }

    // --- quoted bin names ---

    @Test
    public void quotedBinNameResolvesSameAsBare() {
        assertTrue(matchesWhere("$.\"" + BIN_INT + "\":INT == 42"));
        assertTrue(matchesWhere("$.'" + BIN_INT + "':INT == 42"));
    }

    // --- comments ---

    @Test
    public void blockCommentsAreIgnored() {
        assertTrue(matchesWhere("$." + BIN_INT + ":INT /* inline */ == 42"));
    }

    /** AEL has block comments only; a // sequence is a parse error, not a line comment. */
    @Test
    public void lineCommentsAreNotSupported() {
        assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_INT + ":INT // trailing\n"));
    }

    // --- helpers ---

    private long selectLong(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Number.class, value, () -> "expected number for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private List<Long> selectLongs(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(List.class, value, () -> "expected list for AEL: " + ael);
        return ((List<?>) value).stream().map(v -> ((Number) v).longValue()).toList();
    }

    private boolean selectBoolean(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Boolean.class, value, () -> "expected boolean for AEL: " + ael);
        return (Boolean) value;
    }

    private Object selectValue(String ael) {
        Object value = rawValue(ael);
        assertTrue(value != null, () -> "null result for AEL: " + ael);
        return value;
    }

    private Object rawValue(String ael) {
        try (RecordStream rs = session.query(key)
            .bin("out")
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            return rec.getValue("out");
        }
    }

    private boolean matchesWhere(String whereAel) {
        try (RecordStream rs = session.query(key).where(whereAel).execute()) {
            return rs.hasNext();
        }
    }
}
