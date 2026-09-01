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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

/**
 * Server-backed AEL integration tests for the STRING builtin family and the {@code =~}
 * regex operator.
 *
 * <p>These exercise AEL source text end to end — the server parses and executes it. The
 * equivalent msgpack {@code Exp} builders are covered separately by
 * {@code StringApiPackagingTest}; this suite is about the language surface.
 *
 * <p>String methods take a STRING receiver, so paths are pinned {@code :STRING}. Index
 * arguments are codepoint offsets and negative values count from the end.
 *
 * <p>Fixture — one record with these bins:
 * <pre>
 *   s       "Hello World"
 *   uni     "héllo"          5 codepoints, 6 UTF-8 bytes
 *   numstr  "1234"
 *   b64     "aGVsbG8="       base64 of "hello"
 *   padded  "  trim me  "
 *   upper   "HELLO"
 *   lower   "hello"
 *   num     42
 *   f       2.5
 * </pre>
 */
public class AelStringTest extends ClusterTest {
    private static final String KEY = "ael_string";

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
            .bin("s").setTo("Hello World")
            .bin("uni").setTo("héllo")
            .bin("numstr").setTo("1234")
            .bin("b64").setTo("aGVsbG8=")
            .bin("padded").setTo("  trim me  ")
            .bin("upper").setTo("HELLO")
            .bin("lower").setTo("hello")
            .bin("num").setTo(42)
            .bin("f").setTo(2.5)
            .execute();
    }

    // --- length ---

    @Test
    public void strlenCountsCodepoints() {
        assertEquals(11L, selectLong("$.s:STRING.strlen()"));
        assertEquals(5L, selectLong("$.uni:STRING.strlen()"));
    }

    @Test
    public void bytesLengthCountsUtf8Bytes() {
        assertEquals(11L, selectLong("$.s:STRING.bytesLength()"));
        // "héllo" is where the two lengths part ways: é is one codepoint, two bytes.
        assertEquals(6L, selectLong("$.uni:STRING.bytesLength()"));
    }

    // --- substring / indexing ---

    @Test
    public void substrWithFromAndTo() {
        assertEquals("Hello", selectString("$.s:STRING.substr(from: 0, to: 5)"));
    }

    @Test
    public void substrWithFromOnlyRunsToEnd() {
        assertEquals("World", selectString("$.s:STRING.substr(from: 6)"));
    }

    @Test
    public void substrWithNegativeFromCountsFromEnd() {
        assertEquals("World", selectString("$.s:STRING.substr(from: -5)"));
    }

    @Test
    public void charAtReturnsSingleCodepointString() {
        assertEquals("H", selectString("$.s:STRING.charAt(index: 0)"));
    }

    @Test
    public void charAtWithNegativeIndexCountsFromEnd() {
        assertEquals("d", selectString("$.s:STRING.charAt(index: -1)"));
    }

    // --- search ---

    @Test
    public void findReturnsFirstOccurrenceIndex() {
        assertEquals(6L, selectLong("$.s:STRING.find(needle: 'World')"));
    }

    @Test
    public void findWithOccurrenceSelectsNthMatch() {
        // "Hello World" — 'l' at codepoints 2, 3, 9; the 2nd occurrence is index 3.
        assertEquals(3L, selectLong("$.s:STRING.find(needle: 'l', occurrence: 2)"));
    }

    @Test
    public void findReturnsMinusOneWhenAbsent() {
        assertEquals(-1L, selectLong("$.s:STRING.find(needle: 'zzz')"));
    }

    @Test
    public void containsUsesNamedNeedle() {
        assertTrue(selectBoolean("$.s:STRING.contains(needle: 'World')"));
        assertFalse(selectBoolean("$.s:STRING.contains(needle: 'zzz')"));
    }

    /**
     * startsWith / endsWith take their argument positionally while contains / find label
     * theirs — the server's function table deliberately differs per op, so a named
     * argument here is a parse error rather than a synonym.
     */
    @Test
    public void startsWithAndEndsWithTakePositionalArgument() {
        assertTrue(selectBoolean("$.s:STRING.startsWith('Hello')"));
        assertFalse(selectBoolean("$.s:STRING.startsWith('World')"));
        assertTrue(selectBoolean("$.s:STRING.endsWith('World')"));
        assertFalse(selectBoolean("$.s:STRING.endsWith('Hello')"));
    }

    @Test
    public void startsWithRejectsNamedArgument() {
        assertThrows(AerospikeException.class,
            () -> selectValue("$.s:STRING.startsWith(needle: 'Hello')"));
    }

    // --- classification ---

    @Test
    public void isNumericDistinguishesNumericText() {
        assertTrue(selectBoolean("$.numstr:STRING.isNumeric()"));
        assertFalse(selectBoolean("$.s:STRING.isNumeric()"));
    }

    @Test
    public void isUpperAndIsLowerOnStoredText() {
        assertFalse(selectBoolean("$.s:STRING.isUpper()"));
        assertFalse(selectBoolean("$.s:STRING.isLower()"));
        assertTrue(selectBoolean("$.upper:STRING.isUpper()"));
        assertTrue(selectBoolean("$.lower:STRING.isLower()"));
    }
    
    @Disabled("server: isUpper/isLower ignore a chained case op; contains/strlen on the same receiver do not")
    @Test
    public void isUpperAndIsLowerObserveChainedCaseOp() {
        assertTrue(selectBoolean("$.s:STRING.upper().isUpper()"));
        assertTrue(selectBoolean("$.s:STRING.lower().isLower()"));
    }

    /** Guards the premise of the disabled test above: other reads do see the case op. */
    @Test
    public void otherReadsObserveChainedCaseOp() {
        assertTrue(selectBoolean("$.s:STRING.upper().contains(needle: 'HELLO')"));
        assertEquals("HELLO WORLD", selectString("$.s:STRING.upper()"));
    }

    // --- case and normalization ---

    @Test
    public void upperAndLower() {
        assertEquals("HELLO WORLD", selectString("$.s:STRING.upper()"));
        assertEquals("hello world", selectString("$.s:STRING.lower()"));
    }

    @Test
    public void caseFoldProducesComparisonKey() {
        assertEquals("hello world", selectString("$.s:STRING.caseFold()"));
    }

    @Test
    public void normalizeNfcLeavesAlreadyNormalizedTextAlone() {
        assertEquals("Hello World", selectString("$.s:STRING.normalizeNFC()"));
    }

    // --- trimming and padding ---

    @Test
    public void trimVariantsRemoveSurroundingWhitespace() {
        assertEquals("trim me", selectString("$.padded:STRING.trim()"));
        assertEquals("trim me  ", selectString("$.padded:STRING.trimStart()"));
        assertEquals("  trim me", selectString("$.padded:STRING.trimEnd()"));
    }

    @Test
    public void padStartAndPadEndFillToTargetLength() {
        assertEquals("****Hello World", selectString("$.s:STRING.padStart(length: 15, pad: '*')"));
        assertEquals("Hello World****", selectString("$.s:STRING.padEnd(length: 15, pad: '*')"));
    }

    @Test
    public void padIsNoOpWhenAlreadyAtTargetLength() {
        assertEquals("Hello World", selectString("$.s:STRING.padStart(length: 5, pad: '*')"));
    }

    // --- construction ---

    @Test
    public void repeatConcatenatesCopies() {
        assertEquals("Hello WorldHello World", selectString("$.s:STRING.repeat(2)"));
    }

    @Test
    public void spliceInsertsAtCodepointOffset() {
        assertEquals("Hello, World", selectString("$.s:STRING.splice(offset: 5, value: ',')"));
    }

    @Test
    public void overwriteReplacesInPlace() {
        assertEquals("Jello World", selectString("$.s:STRING.overwrite(offset: 0, value: 'J')"));
    }

    @Test
    public void snipRemovesRange() {
        assertEquals("World", selectString("$.s:STRING.snip(from: 0, to: 6)"));
    }

    // --- replacement ---

    @Test
    public void replaceSubstitutesFirstMatchOnly() {
        assertEquals("Hello There",
            selectString("$.s:STRING.replace(find: 'World', replace: 'There')"));
    }

    @Test
    public void replaceAllSubstitutesEveryMatch() {
        assertEquals("HeLLo WorLd",
            selectString("$.s:STRING.replaceAll(find: 'l', replace: 'L')"));
    }

    @Test
    public void regexReplaceUsesRegexLiteral() {
        assertEquals("Hell0 W0rld",
            selectString("$.s:STRING.regexReplace(pattern: /o/, replace: '0')"));
    }

    @Test
    public void regexReplaceHonoursCaseInsensitiveFlag() {
        assertEquals("He__o Wor_d",
            selectString("$.s:STRING.regexReplace(pattern: /L/i, replace: '_')"));
    }

    // --- regex match operator ---

    @Test
    public void regexOperatorMatchesLiteralPattern() {
        assertTrue(selectBoolean("$.s:STRING =~ /Hello/"));
        assertFalse(selectBoolean("$.s:STRING =~ /zzz/"));
    }

    @Test
    public void regexOperatorHonoursCaseInsensitiveFlag() {
        assertFalse(selectBoolean("$.s:STRING =~ /hello/"));
        assertTrue(selectBoolean("$.s:STRING =~ /hello/i"));
    }

    @Test
    public void regexOperatorSupportsAnchors() {
        assertTrue(selectBoolean("$.s:STRING =~ /^Hello/"));
        assertTrue(selectBoolean("$.s:STRING =~ /World$/"));
        assertFalse(selectBoolean("$.s:STRING =~ /^World/"));
    }

    @Test
    public void regexOperatorUsableAsWhereFilter() {
        assertTrue(matchesWhere("$.s:STRING =~ /^Hello W/"));
        assertFalse(matchesWhere("$.s:STRING =~ /^Goodbye/"));
    }

    /**
     * The regex contract is ICU syntax. PCRE2-only spellings are rejected rather than
     * executed under a different dialect, so the same pattern cannot mean two things
     * depending on whether the subject data happens to be ASCII.
     */
    @Test
    public void regexRejectsNonIcuNamedGroupSpelling() {
        assertNotApplicable("$.s:STRING =~ /(?P<name>Hello)/");
    }

    @Test
    public void regexAcceptsIcuNamedGroupSpelling() {
        assertTrue(selectBoolean("$.s:STRING =~ /(?<name>Hello)/"));
    }

    @Test
    public void regexRejectsNonIcuOpenEndedInterval() {
        assertNotApplicable("$.s:STRING =~ /a{,3}/");
    }

    @Test
    public void regexRejectsMalformedPattern() {
        assertNotApplicable("$.s:STRING =~ /[/");
    }

    // --- conversions ---

    @Test
    public void splitBreaksOnSeparator() {
        assertEquals(List.of("Hello", "World"), selectList("$.s:STRING.split(' ')"));
    }

    @Test
    public void toBlobReturnsUtf8Bytes() {
        assertBlobEquals("Hello World", "$.s:STRING.toBlob()");
    }

    @Test
    public void b64DecodeReturnsDecodedBytes() {
        assertBlobEquals("hello", "$.b64:STRING.b64Decode()");
    }

    @Test
    public void b64DecodeRejectsNonBase64Text() {
        assertNotApplicable("$.s:STRING.b64Decode()");
    }

    @Test
    public void toIntParsesNumericString() {
        assertEquals(1234L, selectLong("$.numstr:STRING.toInt()"));
    }

    @Test
    public void toFloatParsesNumericString() {
        assertEquals(1234.0, selectDouble("$.numstr:STRING.toFloat()"));
    }

    @Test
    public void toIntRejectsNonNumericString() {
        assertNotApplicable("$.s:STRING.toInt()");
    }

    @Test
    public void toStringRendersIntAndFloatBins() {
        assertEquals("42", selectString("$.num:INT.toString()"));
        assertEquals("2.5", selectString("$.f:FLOAT.toString()"));
    }

    // --- composition ---

    @Test
    public void stringOpsChainLeftToRight() {
        assertEquals("HELLO", selectString("$.s:STRING.substr(from: 0, to: 5).upper()"));
    }

    @Test
    public void stringOpResultUsableInArithmeticComparison() {
        assertTrue(matchesWhere("$.s:STRING.strlen() == 11"));
        assertFalse(matchesWhere("$.s:STRING.strlen() == 12"));
    }

    @Test
    public void stringPredicateCombinesWithLogicalOperators() {
        assertTrue(matchesWhere("$.s:STRING.startsWith('Hello') and $.num:INT == 42"));
        assertFalse(matchesWhere("$.s:STRING.startsWith('Hello') and $.num:INT == 1"));
    }

    // --- helpers ---

    private void assertNotApplicable(String ael) {
        AerospikeException ex = assertThrows(AerospikeException.class, () -> selectValue(ael),
            () -> "expected server to reject AEL: " + ael);
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode(),
            () -> "unexpected result code for AEL: " + ael);
    }

    private void assertBlobEquals(String expectedText, String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(byte[].class, value, () -> "expected blob for AEL: " + ael);
        assertArrayEquals(expectedText.getBytes(StandardCharsets.UTF_8), (byte[]) value,
            () -> "unexpected bytes for AEL: " + ael);
    }

    private String selectString(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(String.class, value, () -> "expected string for AEL: " + ael);
        return (String) value;
    }

    private long selectLong(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Number.class, value, () -> "expected number for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private double selectDouble(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Number.class, value, () -> "expected number for AEL: " + ael);
        return ((Number) value).doubleValue();
    }

    private boolean selectBoolean(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Boolean.class, value, () -> "expected boolean for AEL: " + ael);
        return (Boolean) value;
    }

    private List<?> selectList(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(List.class, value, () -> "expected list for AEL: " + ael);
        return (List<?>) value;
    }

    private Object selectValue(String ael) {
        try (RecordStream rs = session.query(key)
            .bin("out")
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            Object value = rec.getValue("out");
            assertNotNull(value, () -> "null result for AEL: " + ael);
            return value;
        }
    }

    private boolean matchesWhere(String whereAel) {
        try (RecordStream rs = session.query(key).where(whereAel).execute()) {
            return rs.hasNext();
        }
    }
}
