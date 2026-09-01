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
 *
 * <p>Fixture — one record with these bins:
 * <pre>
 *   num  42
 *   s    "hello"
 *   l    [100, 200, 300]
 *   m    {alpha: 10, beta: 20}
 * </pre>
 * Bin {@code nosuchbin} is deliberately never written.
 */
public class AelLiteralTest extends ClusterTest {
    private static final String KEY = "ael_literal";

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
            .bin("num").setTo(42)
            .bin("s").setTo("hello")
            .bin("l").setTo(List.of(100, 200, 300))
            .bin("m").setTo(map)
            .execute();
    }

    // --- scalar literals ---

    @Test
    public void nilLiteralYieldsNull() {
        assertNull(rawValue("NIL"));
    }

    @Test
    public void booleanLiteralsCombineWithPredicates() {
        assertTrue(selectBoolean("$.num:INT == 42 and true"));
        assertFalse(selectBoolean("$.num:INT == 42 and false"));
    }

    @Test
    public void integerLiteralRadixForms() {
        assertTrue(matchesWhere("$.num:INT == 0x2A"));
        assertTrue(matchesWhere("$.num:INT == 0b101010"));
    }

    @Test
    public void negativeIntegerLiteral() {
        assertEquals(-42L, selectLong("0 - $.num:INT"));
        assertTrue(matchesWhere("$.num:INT > -1"));
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
        assertTrue(matchesWhere("$.s:STRING == 'hello'"));
        assertTrue(matchesWhere("$.s:STRING == \"hello\""));
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
        assertRejected("[3, 1, 2]:SORTED", ResultCode.PARAMETER_ERROR);
    }

    // --- membership ---

    @Test
    public void inMembershipOverListLiteral() {
        assertTrue(selectBoolean("$.num:INT in [1, 42, 3]"));
        assertFalse(selectBoolean("$.num:INT in [1, 2, 3]"));
    }

    @Test
    public void inMembershipOverStrings() {
        assertTrue(selectBoolean("$.s:STRING in ['hello', 'world']"));
        assertFalse(selectBoolean("$.s:STRING in ['nope']"));
    }

    @Test
    public void inMembershipUsableAsWhereFilter() {
        assertTrue(matchesWhere("$.num:INT in [41, 42, 43]"));
        assertFalse(matchesWhere("$.num:INT in [1, 2]"));
    }

    // --- logical ---

    @Test
    public void notNegatesPredicate() {
        assertTrue(selectBoolean("not($.num:INT == 1)"));
        assertFalse(selectBoolean("not($.num:INT == 42)"));
    }

    @Test
    public void notRequiresParenthesisedCallForm() {
        assertThrows(AerospikeException.class, () -> selectValue("!($.num:INT == 1)"));
    }

    @Test
    public void andOrShortCircuitToExpectedTruth() {
        assertTrue(selectBoolean("$.num:INT == 42 or $.num:INT == 1"));
        assertFalse(selectBoolean("$.num:INT == 42 and $.num:INT == 1"));
    }

    // --- trilean / absent handling ---

    @Test
    public void unknownIsAbsorbedByOr() {
        assertTrue(selectBoolean("$.num:INT == 42 or unknown"));
    }

    @Test
    public void existsDistinguishesPresentFromAbsentBin() {
        assertTrue(selectBoolean("$.m.exists()"));
        assertFalse(selectBoolean("$.nosuchbin.exists()"));
    }

    @Test
    public void existsUsableAsWhereFilter() {
        assertTrue(matchesWhere("$.m.exists()"));
        assertFalse(matchesWhere("$.nosuchbin.exists()"));
    }

    /** A bare bin reference has no resolved type, so it cannot be a projection on its own. */
    @Test
    public void bareUntypedBinReferenceRejected() {
        assertRejected("$.nosuchbin", ResultCode.PARAMETER_ERROR);
    }

    /** A type-pinned read of an absent bin compiles, then faults at evaluation. */
    @Test
    public void typedReadOfAbsentBinIsNotApplicable() {
        assertRejected("$.nosuchbin:INT", ResultCode.OP_NOT_APPLICABLE);
    }

    @Test
    public void absentBinDoesNotMatchWhereFilter() {
        assertFalse(matchesWhere("$.nosuchbin:INT == 1"));
    }

    // --- bin type introspection ---

    @Test
    public void typeReportsBinParticleType() {
        assertTrue(matchesWhere("$.num.type() == INT"));
        assertTrue(matchesWhere("$.s.type() == STRING"));
        assertFalse(matchesWhere("$.num.type() == STRING"));
    }

    // --- quoted bin names ---

    @Test
    public void quotedBinNameResolvesSameAsBare() {
        assertTrue(matchesWhere("$.\"num\":INT == 42"));
        assertTrue(matchesWhere("$.'num':INT == 42"));
    }

    // --- comments ---

    @Test
    public void blockCommentsAreIgnored() {
        assertTrue(matchesWhere("$.num:INT /* inline */ == 42"));
    }

    /** AEL has block comments only; a // sequence is a parse error, not a line comment. */
    @Test
    public void lineCommentsAreNotSupported() {
        assertThrows(AerospikeException.class, () -> selectValue("$.num:INT // trailing\n"));
    }

    // --- helpers ---

    private void assertRejected(String ael, int expectedResultCode) {
        AerospikeException ex = assertThrows(AerospikeException.class, () -> selectValue(ael),
            () -> "expected server to reject AEL: " + ael);
        assertEquals(expectedResultCode, ex.getResultCode(),
            () -> "unexpected result code for AEL: " + ael);
    }

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
