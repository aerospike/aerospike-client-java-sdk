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
 */
public class AelStringTest extends ClusterTest {
    private static final String KEY = "ael_string";
    private static final String BIN_TEXT = "s";
    private static final String BIN_NUMERIC = "numstr";
    private static final String BIN_BASE64 = "b64";
    private static final String BIN_PADDED = "padded";
    private static final String BIN_UPPER = "upper";
    private static final String BIN_LOWER = "lower";
    private static final String BIN_INT = "num";
    private static final String BIN_FLOAT = "f";

    private static final String TEXT = "Hello World";

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
            .bin(BIN_TEXT).setTo(TEXT)
            .bin(BIN_NUMERIC).setTo("1234")
            .bin(BIN_BASE64).setTo("aGVsbG8=")
            .bin(BIN_PADDED).setTo("  trim me  ")
            .bin(BIN_UPPER).setTo("HELLO")
            .bin(BIN_LOWER).setTo("hello")
            .bin(BIN_INT).setTo(42)
            .bin(BIN_FLOAT).setTo(2.5)
            .execute();
    }

    // --- length ---

    @Test
    public void strlenCountsCodepoints() {
        assertEquals(TEXT.length(), selectLong("$." + BIN_TEXT + ":STRING.strlen()"));
    }

    @Test
    public void bytesLengthCountsUtf8Bytes() {
        assertEquals(TEXT.getBytes(StandardCharsets.UTF_8).length,
            selectLong("$." + BIN_TEXT + ":STRING.bytesLength()"));
    }

    // --- substring / indexing ---

    @Test
    public void substrWithFromAndTo() {
        assertEquals("Hello", selectString("$." + BIN_TEXT + ":STRING.substr(from: 0, to: 5)"));
    }

    @Test
    public void substrWithFromOnlyRunsToEnd() {
        assertEquals("World", selectString("$." + BIN_TEXT + ":STRING.substr(from: 6)"));
    }

    @Test
    public void substrWithNegativeFromCountsFromEnd() {
        assertEquals("World", selectString("$." + BIN_TEXT + ":STRING.substr(from: -5)"));
    }

    @Test
    public void charAtReturnsSingleCodepointString() {
        assertEquals("H", selectString("$." + BIN_TEXT + ":STRING.charAt(index: 0)"));
    }

    @Test
    public void charAtWithNegativeIndexCountsFromEnd() {
        assertEquals("d", selectString("$." + BIN_TEXT + ":STRING.charAt(index: -1)"));
    }

    // --- search ---

    @Test
    public void findReturnsFirstOccurrenceIndex() {
        assertEquals(6L, selectLong("$." + BIN_TEXT + ":STRING.find(needle: 'World')"));
    }

    @Test
    public void findWithOccurrenceSelectsNthMatch() {
        // "Hello World" — 'l' at codepoints 2, 3, 9; the 2nd occurrence is index 3.
        assertEquals(3L, selectLong("$." + BIN_TEXT + ":STRING.find(needle: 'l', occurrence: 2)"));
    }

    @Test
    public void findReturnsMinusOneWhenAbsent() {
        assertEquals(-1L, selectLong("$." + BIN_TEXT + ":STRING.find(needle: 'zzz')"));
    }

    @Test
    public void containsUsesNamedNeedle() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING.contains(needle: 'World')"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING.contains(needle: 'zzz')"));
    }

    /**
     * startsWith / endsWith take their argument positionally while contains / find label
     * theirs — the server's function table deliberately differs per op, so a named
     * argument here is a parse error rather than a synonym.
     */
    @Test
    public void startsWithAndEndsWithTakePositionalArgument() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING.startsWith('Hello')"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING.startsWith('World')"));
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING.endsWith('World')"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING.endsWith('Hello')"));
    }

    @Test
    public void startsWithRejectsNamedArgument() {
        assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_TEXT + ":STRING.startsWith(needle: 'Hello')"));
    }

    // --- classification ---

    @Test
    public void isNumericDistinguishesNumericText() {
        assertTrue(selectBoolean("$." + BIN_NUMERIC + ":STRING.isNumeric()"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING.isNumeric()"));
    }

    @Test
    public void isUpperAndIsLowerOnStoredText() {
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING.isUpper()"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING.isLower()"));
        assertTrue(selectBoolean("$." + BIN_UPPER + ":STRING.isUpper()"));
        assertTrue(selectBoolean("$." + BIN_LOWER + ":STRING.isLower()"));
    }

    /**
     * Server bug: {@code isUpper} / {@code isLower} do not observe the result of a
     * preceding case op, though every other read op does.
     *
     * <p>{@code $.s:STRING.upper()} yields {@code "HELLO WORLD"} and
     * {@code $.s:STRING.upper().contains(needle: 'HELLO')} is {@code true} — so the chained
     * receiver is the modified value. But {@code .upper().isUpper()} returns {@code false},
     * and symmetrically for {@code .lower().isLower()}. Enable once the server agrees.
     */
    @Disabled("server: isUpper/isLower ignore a chained case op; contains/strlen on the same receiver do not")
    @Test
    public void isUpperAndIsLowerObserveChainedCaseOp() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING.upper().isUpper()"));
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING.lower().isLower()"));
    }

    /** Guards the premise of the disabled test above: other reads do see the case op. */
    @Test
    public void otherReadsObserveChainedCaseOp() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING.upper().contains(needle: 'HELLO')"));
        assertEquals("HELLO WORLD", selectString("$." + BIN_TEXT + ":STRING.upper()"));
    }

    // --- case and normalization ---

    @Test
    public void upperAndLower() {
        assertEquals("HELLO WORLD", selectString("$." + BIN_TEXT + ":STRING.upper()"));
        assertEquals("hello world", selectString("$." + BIN_TEXT + ":STRING.lower()"));
    }

    @Test
    public void caseFoldProducesComparisonKey() {
        assertEquals("hello world", selectString("$." + BIN_TEXT + ":STRING.caseFold()"));
    }

    @Test
    public void normalizeNfcLeavesAlreadyNormalizedTextAlone() {
        assertEquals(TEXT, selectString("$." + BIN_TEXT + ":STRING.normalizeNFC()"));
    }

    // --- trimming and padding ---

    @Test
    public void trimVariantsRemoveSurroundingWhitespace() {
        assertEquals("trim me", selectString("$." + BIN_PADDED + ":STRING.trim()"));
        assertEquals("trim me  ", selectString("$." + BIN_PADDED + ":STRING.trimStart()"));
        assertEquals("  trim me", selectString("$." + BIN_PADDED + ":STRING.trimEnd()"));
    }

    @Test
    public void padStartAndPadEndFillToTargetLength() {
        assertEquals("****Hello World",
            selectString("$." + BIN_TEXT + ":STRING.padStart(length: 15, pad: '*')"));
        assertEquals("Hello World****",
            selectString("$." + BIN_TEXT + ":STRING.padEnd(length: 15, pad: '*')"));
    }

    @Test
    public void padIsNoOpWhenAlreadyAtTargetLength() {
        assertEquals(TEXT, selectString("$." + BIN_TEXT + ":STRING.padStart(length: 5, pad: '*')"));
    }

    // --- construction ---

    @Test
    public void repeatConcatenatesCopies() {
        assertEquals(TEXT + TEXT, selectString("$." + BIN_TEXT + ":STRING.repeat(2)"));
    }

    @Test
    public void spliceInsertsAtCodepointOffset() {
        assertEquals("Hello, World",
            selectString("$." + BIN_TEXT + ":STRING.splice(offset: 5, value: ',')"));
    }

    @Test
    public void overwriteReplacesInPlace() {
        assertEquals("Jello World",
            selectString("$." + BIN_TEXT + ":STRING.overwrite(offset: 0, value: 'J')"));
    }

    @Test
    public void snipRemovesRange() {
        assertEquals("World", selectString("$." + BIN_TEXT + ":STRING.snip(from: 0, to: 6)"));
    }

    // --- replacement ---

    @Test
    public void replaceSubstitutesFirstMatchOnly() {
        assertEquals("Hello There",
            selectString("$." + BIN_TEXT + ":STRING.replace(find: 'World', replace: 'There')"));
    }

    @Test
    public void replaceAllSubstitutesEveryMatch() {
        assertEquals("HeLLo WorLd",
            selectString("$." + BIN_TEXT + ":STRING.replaceAll(find: 'l', replace: 'L')"));
    }

    @Test
    public void regexReplaceUsesRegexLiteral() {
        assertEquals("Hell0 W0rld",
            selectString("$." + BIN_TEXT + ":STRING.regexReplace(pattern: /o/, replace: '0')"));
    }

    @Test
    public void regexReplaceHonoursCaseInsensitiveFlag() {
        assertEquals("He__o Wor_d",
            selectString("$." + BIN_TEXT + ":STRING.regexReplace(pattern: /L/i, replace: '_')"));
    }

    // --- regex match operator ---

    @Test
    public void regexOperatorMatchesLiteralPattern() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING =~ /Hello/"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING =~ /zzz/"));
    }

    @Test
    public void regexOperatorHonoursCaseInsensitiveFlag() {
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING =~ /hello/"));
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING =~ /hello/i"));
    }

    @Test
    public void regexOperatorSupportsAnchors() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING =~ /^Hello/"));
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING =~ /World$/"));
        assertFalse(selectBoolean("$." + BIN_TEXT + ":STRING =~ /^World/"));
    }

    @Test
    public void regexOperatorUsableAsWhereFilter() {
        assertTrue(matchesWhere("$." + BIN_TEXT + ":STRING =~ /^Hello W/"));
        assertFalse(matchesWhere("$." + BIN_TEXT + ":STRING =~ /^Goodbye/"));
    }

    /**
     * The regex contract is ICU syntax. PCRE2-only spellings are rejected rather than
     * executed under a different dialect, so the same pattern cannot mean two things
     * depending on whether the subject data happens to be ASCII.
     */
    @Test
    public void regexRejectsNonIcuNamedGroupSpelling() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_TEXT + ":STRING =~ /(?P<name>Hello)/"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    @Test
    public void regexAcceptsIcuNamedGroupSpelling() {
        assertTrue(selectBoolean("$." + BIN_TEXT + ":STRING =~ /(?<name>Hello)/"));
    }

    @Test
    public void regexRejectsNonIcuOpenEndedInterval() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_TEXT + ":STRING =~ /a{,3}/"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    @Test
    public void regexRejectsMalformedPattern() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_TEXT + ":STRING =~ /[/"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    // --- conversions ---

    @Test
    public void splitBreaksOnSeparator() {
        assertEquals(List.of("Hello", "World"), selectList("$." + BIN_TEXT + ":STRING.split(' ')"));
    }

    @Test
    public void toBlobReturnsUtf8Bytes() {
        Object value = selectValue("$." + BIN_TEXT + ":STRING.toBlob()");
        assertInstanceOf(byte[].class, value);
        assertArrayEquals(TEXT.getBytes(StandardCharsets.UTF_8), (byte[]) value);
    }

    @Test
    public void b64DecodeReturnsDecodedBytes() {
        Object value = selectValue("$." + BIN_BASE64 + ":STRING.b64Decode()");
        assertInstanceOf(byte[].class, value);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), (byte[]) value);
    }

    @Test
    public void b64DecodeRejectsNonBase64Text() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_TEXT + ":STRING.b64Decode()"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    @Test
    public void toIntParsesNumericString() {
        assertEquals(1234L, selectLong("$." + BIN_NUMERIC + ":STRING.toInt()"));
    }

    @Test
    public void toFloatParsesNumericString() {
        assertEquals(1234.0, selectDouble("$." + BIN_NUMERIC + ":STRING.toFloat()"));
    }

    @Test
    public void toIntRejectsNonNumericString() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$." + BIN_TEXT + ":STRING.toInt()"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    @Test
    public void toStringRendersIntAndFloatBins() {
        assertEquals("42", selectString("$." + BIN_INT + ":INT.toString()"));
        assertEquals("2.5", selectString("$." + BIN_FLOAT + ":FLOAT.toString()"));
    }

    // --- composition ---

    @Test
    public void stringOpsChainLeftToRight() {
        assertEquals("HELLO", selectString("$." + BIN_TEXT + ":STRING.substr(from: 0, to: 5).upper()"));
    }

    @Test
    public void stringOpResultUsableInArithmeticComparison() {
        assertTrue(matchesWhere("$." + BIN_TEXT + ":STRING.strlen() == 11"));
        assertFalse(matchesWhere("$." + BIN_TEXT + ":STRING.strlen() == 12"));
    }

    @Test
    public void stringPredicateCombinesWithLogicalOperators() {
        assertTrue(matchesWhere(
            "$." + BIN_TEXT + ":STRING.startsWith('Hello') and $." + BIN_INT + ":INT == 42"));
        assertFalse(matchesWhere(
            "$." + BIN_TEXT + ":STRING.startsWith('Hello') and $." + BIN_INT + ":INT == 1"));
    }

    // --- helpers ---

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
