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
package com.aerospike.client.sdk;

import static com.aerospike.client.sdk.ExpProjectionTestSupport.assertProjection;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.StringExp;
import com.aerospike.client.sdk.junit.RequiresServerFeature;
import com.aerospike.client.sdk.junit.ServerFeature;
import com.aerospike.client.sdk.operation.StringNumericType;
import com.aerospike.client.sdk.operation.StringOperation;
import com.aerospike.client.sdk.operation.StringRegexFlags;
import com.aerospike.client.sdk.operation.StringWriteFlags;

/**
 * Integration tests for string expressions: {@link BinBuilder} / {@link StringOperation}
 * (always run on 8.1.3+), client {@link Exp} API via {@link StringExp}, and string AEL
 * equivalents (disabled until the server validates them in selectFrom/filter).
 */
@RequiresServerFeature(ServerFeature.STRING_OPS)
public class OperateStringTest extends ClusterTest {

    private static final String STRING_BIN = "s";
    private static final String NUM_BIN = "num";
    private static final String FLT_BIN = "flt";
    private static final String B64_BIN = "b64";
    private static final String INT_BIN = "n";
    private static final String DIGITS_BIN = "digits";
    /** Decomposed e-acute for {@link StringExp#normalizeNFC} coverage. */
    private static final String NFC_BIN = "nfc";
    private static final Key KEY = args.set.id("stringop-key");
    private static final String BIN = "sbin";

    @Nested
    @DisplayName("reads")
    class Reads {
        @Nested
        @DisplayName("BinBuilder")
        class Fluent {
            Key key;

            @BeforeEach
            void seedHelloString() {
                key = freshKey("stringFluentReads");
                seed(key, b -> b.bin(STRING_BIN).setTo("hello"));
            }

            @Test
            @DisplayName("BinBuilder strlen, substr, find")
            public void binBuilderStringReads() {
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).strlen()
                    .bin(STRING_BIN).substr(1, 4)
                    .bin(STRING_BIN).find("ll")
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertEquals(5L, rec.operationResult(0).getLong(), "BinBuilder.strlen(stringBin s)");
                    assertEquals("ell", rec.operationResult(1).getString(),
                        "BinBuilder.substr(offset=1, count=4)");
                    assertEquals(2L, rec.operationResult(2).getLong(), "BinBuilder.find(needle=ll)");
                }
            }
        }

        @Nested
        @DisplayName("StringOperation / appendOperations")
        class OperationReads {
            Key key;

            @BeforeEach
            void seedReadRecord() {
                key = freshKey("stringOpReadSweep");
                seed(key, b -> b
                    .bin(STRING_BIN).setTo("Hello,42")
                    .bin(NUM_BIN).setTo("12345")
                    .bin(FLT_BIN).setTo("3.14")
                    .bin(B64_BIN).setTo("aGVsbG8=")
                    .bin(INT_BIN).setTo(42));
            }

            @Test
            @DisplayName("StringOperation read API sweep")
            public void stringOperationReadSweep() {
                try (RecordStream rs = session.query(key)
                    .appendOperations(
                        StringOperation.strlen(STRING_BIN),
                        StringOperation.substr(STRING_BIN, 1, 4),
                        StringOperation.substr(STRING_BIN, 3),
                        StringOperation.charAt(STRING_BIN, 0),
                        StringOperation.find(STRING_BIN, "ll"),
                        StringOperation.find(STRING_BIN, "l", -1),
                        StringOperation.byteLength(STRING_BIN),
                        StringOperation.contains(STRING_BIN, ","),
                        StringOperation.startsWith(STRING_BIN, "Hel"),
                        StringOperation.endsWith(STRING_BIN, "42"),
                        StringOperation.isNumeric(NUM_BIN),
                        StringOperation.isNumeric(NUM_BIN, StringNumericType.INT),
                        StringOperation.isUpper(STRING_BIN),
                        StringOperation.isLower(STRING_BIN),
                        StringOperation.toInteger(NUM_BIN),
                        StringOperation.toDouble(FLT_BIN),
                        StringOperation.split(STRING_BIN),
                        StringOperation.split(STRING_BIN, ","),
                        StringOperation.regexCompare(STRING_BIN, "Hel.*"),
                        StringOperation.regexCompare(STRING_BIN, "hel.*", StringRegexFlags.CASE_INSENSITIVE),
                        StringOperation.b64Decode(B64_BIN),
                        StringOperation.toBlob(STRING_BIN),
                        StringOperation.toString(INT_BIN))
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertAll("StringOperation read sweep",
                        () -> assertEquals(8L, rec.operationResult(0).getLong(),
                            "StringOperation.strlen(stringBin s)"),
                        () -> assertEquals("ell", rec.operationResult(1).getString(),
                            "StringOperation.substr(offset=1, count=4)"),
                        () -> assertEquals("lo,42", rec.operationResult(2).getString(),
                            "StringOperation.substr(offset=3)"),
                        () -> assertEquals("H", rec.operationResult(3).getString(),
                            "StringOperation.charAt(index=0)"),
                        () -> assertEquals(2L, rec.operationResult(4).getLong(),
                            "StringOperation.find(needle=ll)"),
                        () -> assertEquals(3L, rec.operationResult(5).getLong(),
                            "StringOperation.find(needle=l, occurrence=-1)"),
                        () -> assertEquals(8L, rec.operationResult(6).getLong(),
                            "StringOperation.byteLength(stringBin s)"),
                        () -> assertTrue(rec.operationResult(7).getBoolean(),
                            "StringOperation.contains(needle=,)"),
                        () -> assertTrue(rec.operationResult(8).getBoolean(),
                            "StringOperation.startsWith(prefix=Hel)"),
                        () -> assertTrue(rec.operationResult(9).getBoolean(),
                            "StringOperation.endsWith(suffix=42)"),
                        () -> assertTrue(rec.operationResult(10).getBoolean(),
                            "StringOperation.isNumeric(stringBin num)"),
                        () -> assertTrue(rec.operationResult(11).getBoolean(),
                            "StringOperation.isNumeric(INT, stringBin num)"),
                        () -> assertFalse(rec.operationResult(12).getBoolean(),
                            "StringOperation.isUpper(stringBin s)"),
                        () -> assertFalse(rec.operationResult(13).getBoolean(),
                            "StringOperation.isLower(stringBin s)"),
                        () -> assertEquals(12345L, rec.operationResult(14).getLong(),
                            "StringOperation.toInteger(stringBin num)"),
                        () -> assertEquals(3.14, rec.operationResult(15).getDouble(), 0.0001,
                            "StringOperation.toDouble(stringBin flt)"),
                        () -> assertEquals(8, rec.operationResult(16).getList().size(),
                            "StringOperation.split(stringBin s)"),
                        () -> assertEquals(2, rec.operationResult(17).getList().size(),
                            "StringOperation.split(delimiter=,)"),
                        () -> assertTrue(rec.operationResult(18).getBoolean(),
                            "StringOperation.regexCompare(pattern=Hel.*)"),
                        () -> assertTrue(rec.operationResult(19).getBoolean(),
                            "StringOperation.regexCompare(pattern=hel.*, CASE_INSENSITIVE)"),
                        () -> assertEquals("hello",
                            new String(rec.operationResult(20).getBytes(), StandardCharsets.UTF_8),
                            "StringOperation.b64Decode(stringBin b64)"),
                        () -> assertEquals("Hello,42",
                            new String(rec.operationResult(21).getBytes(), StandardCharsets.UTF_8),
                            "StringOperation.toBlob(stringBin s)"),
                        () -> assertEquals("42", rec.operationResult(22).getString(),
                            "StringOperation.toString(intBin n)"));
                }
            }
        }

        @Nested
        @DisplayName("StringExp projections")
        class ExpReads {
            Key key;

            @BeforeEach
            void seedReadProjectionRecord() {
                key = freshKey("stringExpReadProj");
                seed(key, b -> b
                    .bin(STRING_BIN).setTo("Hello,42")
                    .bin(NUM_BIN).setTo("12345")
                    .bin(FLT_BIN).setTo("3.14")
                    .bin(B64_BIN).setTo("aGVsbG8=")
                    .bin(INT_BIN).setTo(42));
            }

            @Test
            @DisplayName("StringExp read API sweep (one query per call)")
            public void stringExpReadProjections() {
                Exp s = Exp.stringBin(STRING_BIN);
                assertAll("string read projections",
            () -> assertProjection(session, key, "StringExp.strlen(stringBin s)",
                StringExp.strlen(s),
                rec -> assertEquals(8L, rec.getLong("r"), "length of Hello,42")),
            () -> assertProjection(session, key,
                "StringExp.substr(offset=1, count=4, stringBin s)",
                StringExp.substr(Exp.val(1), Exp.val(4), s),
                rec -> assertEquals("ell", rec.getString("r"), "substring")),
            () -> assertProjection(session, key,
                "StringExp.substr(offset=3, stringBin s)",
                StringExp.substr(Exp.val(3), s),
                rec -> assertEquals("lo,42", rec.getString("r"), "tail from index 3")),
            () -> assertProjection(session, key,
                "StringExp.charAt(index=0, stringBin s)",
                StringExp.charAt(Exp.val(0), s),
                rec -> assertEquals("H", rec.getString("r"), "character")),
            () -> assertProjection(session, key,
                "StringExp.find(needle=ll, stringBin s)",
                StringExp.find(Exp.val("ll"), s),
                rec -> assertEquals(2L, rec.getLong("r"), "index")),
            () -> assertProjection(session, key,
                "StringExp.find(needle=l, offset=-1, stringBin s)",
                StringExp.find(Exp.val("l"), Exp.val(-1), s),
                rec -> assertEquals(3L, rec.getLong("r"), "index")),
            () -> assertProjection(session, key,
                "StringExp.byteLength(stringBin s)",
                StringExp.byteLength(s),
                rec -> assertEquals(8L, rec.getLong("r"), "UTF-8 byte length")),
            () -> assertProjection(session, key,
                "StringExp.contains(needle=,, stringBin s)",
                StringExp.contains(Exp.val(","), s),
                rec -> assertTrue(rec.getBoolean("r"), "contains comma")),
            () -> assertProjection(session, key,
                "StringExp.startsWith(prefix=Hel, stringBin s)",
                StringExp.startsWith(Exp.val("Hel"), s),
                rec -> assertTrue(rec.getBoolean("r"), "starts with Hel")),
            () -> assertProjection(session, key,
                "StringExp.endsWith(suffix=42, stringBin s)",
                StringExp.endsWith(Exp.val("42"), s),
                rec -> assertTrue(rec.getBoolean("r"), "ends with 42")),
            () -> assertProjection(session, key,
                "StringExp.isNumeric(stringBin num)",
                StringExp.isNumeric(Exp.stringBin(NUM_BIN)),
                rec -> assertTrue(rec.getBoolean("r"), "numeric")),
            () -> assertProjection(session, key,
                "StringExp.isNumeric(INT, stringBin num)",
                StringExp.isNumeric(StringNumericType.INT, Exp.stringBin(NUM_BIN)),
                rec -> assertTrue(rec.getBoolean("r"), "integer numeric")),
            () -> assertProjection(session, key,
                "StringExp.isUpper(stringBin s)",
                StringExp.isUpper(s),
                rec -> assertFalse(rec.getBoolean("r"), "not all upper")),
            () -> assertProjection(session, key,
                "StringExp.isLower(stringBin s)",
                StringExp.isLower(s),
                rec -> assertFalse(rec.getBoolean("r"), "not all lower")),
            () -> assertProjection(session, key,
                "StringExp.toInteger(stringBin num)",
                StringExp.toInteger(Exp.stringBin(NUM_BIN)),
                rec -> assertEquals(12345L, rec.getLong("r"), "parsed integer")),
            () -> assertProjection(session, key,
                "StringExp.toDouble(stringBin flt)",
                StringExp.toDouble(Exp.stringBin(FLT_BIN)),
                rec -> assertEquals(3.14, rec.getDouble("r"), 0.0001, "parsed double")),
            () -> assertProjection(session, key,
                "StringExp.split(stringBin s)",
                StringExp.split(s),
                rec -> assertEquals(8, rec.getList("r").size(), "char count")),
            () -> assertProjection(session, key,
                "StringExp.split(delimiter=,, stringBin s)",
                StringExp.split(Exp.val(","), s),
                rec -> assertEquals(2, rec.getList("r").size(), "part count")),
            () -> assertProjection(session, key,
                "StringExp.regexCompare(pattern=Hel.*, stringBin s)",
                StringExp.regexCompare(Exp.val("Hel.*"), s),
                rec -> assertTrue(rec.getBoolean("r"), "regex match")),
            () -> assertProjection(session, key,
                "StringExp.b64Decode(stringBin b64)",
                StringExp.b64Decode(Exp.stringBin(B64_BIN)),
                rec -> assertEquals("hello",
                    new String(rec.getBytes("r"), StandardCharsets.UTF_8), "decoded bytes")),
            () -> assertProjection(session, key,
                "StringExp.toBlob(stringBin s)",
                StringExp.toBlob(s),
                rec -> assertEquals("Hello,42",
                    new String(rec.getBytes("r"), StandardCharsets.UTF_8), "blob bytes")),
            () -> assertProjection(session, key,
                "StringExp.toString(intBin n)",
                StringExp.toString(Exp.intBin(INT_BIN)),
                rec -> assertEquals("42", rec.getString("r"), "string form"))
                );
            }
        }
    }

    @Nested
    @DisplayName("modifies")
    class Modifies {
        @Nested
        @DisplayName("BinBuilder")
        class Fluent {
            Key key;

            @BeforeEach
            void seedModifyString() {
                key = freshKey("stringBinFluentModify");
                seed(key, b -> b.bin(STRING_BIN).setTo("ab"));
            }

            @Test
            public void binBuilderStringModifyAndRead() {
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).upper()
                    .bin(STRING_BIN).get()
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertNull(rec.operationResult(0).getValue(), "upper modify returns no operation payload");
                    assertEquals("AB", rec.operationResult(1).getString(), "stored bin after upper");
                }
            }
        }

        @Nested
        @DisplayName("StringOperation / appendOperations")
        class OperationApi {
            private static final String UP_BIN = "mUp";
            private static final String LOW_BIN = "mLow";
            private static final String REPL_BIN = "mRepl";
            private static final String REPL_ALL_BIN = "mReplAll";
            private static final String INSERT_BIN = "mInsert";
            private static final String OVERWRITE_BIN = "mOverwrite";
            private static final String SNIP_BIN = "mSnip";
            private static final String APPEND_BIN = "mAppend";
            private static final String PREPEND_BIN = "mPrepend";
            private static final String PAD_START_BIN = "mPadStart";
            private static final String PAD_END_BIN = "mPadEnd";
            private static final String REPEAT_BIN = "mRepeat";
            private static final String FOLD_BIN = "mFold";
            private static final String CONCAT_BIN = "mConcat";
            private static final String CONCAT_LIST_BIN = "mConcatList";

            Key key;

            @BeforeEach
            void seedModifyBins() {
                key = freshKey("stringOpModifySweep");
                seed(key, b -> b
                    .bin(UP_BIN).setTo("Hello")
                    .bin(LOW_BIN).setTo("Hello")
                    .bin(REPL_BIN).setTo("Hello")
                    .bin(REPL_ALL_BIN).setTo("Hello")
                    .bin(INSERT_BIN).setTo("Hello")
                    .bin(OVERWRITE_BIN).setTo("Hello")
                    .bin(SNIP_BIN).setTo("Hello")
                    .bin(APPEND_BIN).setTo("Hello")
                    .bin(PREPEND_BIN).setTo("Hello")
                    .bin(PAD_START_BIN).setTo("Hello")
                    .bin(PAD_END_BIN).setTo("Hello")
                    .bin(REPEAT_BIN).setTo("Hello")
                    .bin(FOLD_BIN).setTo("Hello")
                    .bin(CONCAT_BIN).setTo("Hello")
                    .bin(CONCAT_LIST_BIN).setTo("Hello")
                    .bin(NFC_BIN).setTo("e\u0301"));
            }

            @Test
            @DisplayName("StringOperation modify API sweep")
            public void stringOperationModifySweep() {
                int flags = StringWriteFlags.DEFAULT;
                seed(key, b -> b.appendOperations(
                        StringOperation.upper(flags, UP_BIN),
                        StringOperation.lower(flags, LOW_BIN),
                        StringOperation.replace(flags, REPL_BIN, "lo", "LL"),
                        StringOperation.replaceAll(flags, REPL_ALL_BIN, "l", "L"),
                        StringOperation.insert(flags, INSERT_BIN, 1, "X"),
                        StringOperation.overwrite(flags, OVERWRITE_BIN, 1, "i"),
                        StringOperation.snip(flags, SNIP_BIN, 1, 4),
                        StringOperation.append(flags, APPEND_BIN, "!"),
                        StringOperation.prepend(flags, PREPEND_BIN, ">"),
                        StringOperation.padStart(flags, PAD_START_BIN, 7, "0"),
                        StringOperation.padEnd(flags, PAD_END_BIN, 10, "."),
                        StringOperation.repeat(flags, REPEAT_BIN, 2),
                        StringOperation.caseFold(flags, FOLD_BIN),
                        StringOperation.normalizeNFC(flags, NFC_BIN),
                        StringOperation.concat(flags, CONCAT_BIN, "!"),
                        StringOperation.concat(flags, CONCAT_LIST_BIN, List.of("!", "?"))));

                try (RecordStream rs = session.query(key)
                    .bin(UP_BIN).get()
                    .bin(LOW_BIN).get()
                    .bin(REPL_BIN).get()
                    .bin(REPL_ALL_BIN).get()
                    .bin(INSERT_BIN).get()
                    .bin(OVERWRITE_BIN).get()
                    .bin(SNIP_BIN).get()
                    .bin(APPEND_BIN).get()
                    .bin(PREPEND_BIN).get()
                    .bin(PAD_START_BIN).get()
                    .bin(PAD_END_BIN).get()
                    .bin(REPEAT_BIN).get()
                    .bin(FOLD_BIN).get()
                    .bin(NFC_BIN).get()
                    .bin(CONCAT_BIN).get()
                    .bin(CONCAT_LIST_BIN).get()
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertAll("StringOperation modify sweep",
                        () -> assertEquals("HELLO", rec.getString(UP_BIN),
                            "StringOperation.upper(flags, stringBin)"),
                        () -> assertEquals("hello", rec.getString(LOW_BIN),
                            "StringOperation.lower(flags, stringBin)"),
                        () -> assertEquals("HelLL", rec.getString(REPL_BIN),
                            "StringOperation.replace(flags, lo, LL, stringBin)"),
                        () -> assertEquals("HeLLo", rec.getString(REPL_ALL_BIN),
                            "StringOperation.replaceAll(flags, l, L, stringBin)"),
                        () -> assertEquals("HXello", rec.getString(INSERT_BIN),
                            "StringOperation.insert(flags, index=1, X, stringBin)"),
                        () -> assertEquals("Hillo", rec.getString(OVERWRITE_BIN),
                            "StringOperation.overwrite(flags, index=1, i, stringBin)"),
                        () -> assertEquals("Ho", rec.getString(SNIP_BIN),
                            "StringOperation.snip(flags, index=1, count=4, stringBin)"),
                        () -> assertEquals("Hello!", rec.getString(APPEND_BIN),
                            "StringOperation.append(flags, !, stringBin)"),
                        () -> assertEquals(">Hello", rec.getString(PREPEND_BIN),
                            "StringOperation.prepend(flags, >, stringBin)"),
                        () -> assertEquals("00Hello", rec.getString(PAD_START_BIN),
                            "StringOperation.padStart(flags, width=7, pad=0, stringBin)"),
                        () -> assertEquals("Hello.....", rec.getString(PAD_END_BIN),
                            "StringOperation.padEnd(flags, width=10, pad=., stringBin)"),
                        () -> assertEquals("HelloHello", rec.getString(REPEAT_BIN),
                            "StringOperation.repeat(flags, count=2, stringBin)"),
                        () -> assertEquals("hello", rec.getString(FOLD_BIN),
                            "StringOperation.caseFold(flags, stringBin)"),
                        () -> assertEquals("\u00e9", rec.getString(NFC_BIN),
                            "StringOperation.normalizeNFC(flags, nfcBin)"),
                        () -> assertEquals("Hello!", rec.getString(CONCAT_BIN),
                            "StringOperation.concat(flags, !, stringBin)"),
                        () -> assertEquals("Hello!?", rec.getString(CONCAT_LIST_BIN),
                            "StringOperation.concat(flags, [!,?], stringBin)"));
                }
            }
        }

        @Nested
        @DisplayName("StringOperation trim")
        class OperationTrim {
            Key key;

            @BeforeEach
            void seedTrimBins() {
                key = freshKey("stringOpTrim");
                seed(key, b -> b
                    .bin("trimAll").setTo("  hello  ")
                    .bin("trimStart").setTo("  hello  ")
                    .bin("trimEnd").setTo("  hello  "));
            }

            @Test
            @DisplayName("StringOperation trim API sweep")
            public void stringOperationTrimSweep() {
                int flags = StringWriteFlags.DEFAULT;
                seed(key, b -> b.appendOperations(
                        StringOperation.trim(flags, "trimAll"),
                        StringOperation.trimStart(flags, "trimStart"),
                        StringOperation.trimEnd(flags, "trimEnd")));

                try (RecordStream rs = session.query(key)
                    .bin("trimAll").get()
                    .bin("trimStart").get()
                    .bin("trimEnd").get()
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertAll("StringOperation trim sweep",
                        () -> assertEquals("hello", rec.getString("trimAll"),
                            "StringOperation.trim(flags, stringBin)"),
                        () -> assertEquals("hello  ", rec.getString("trimStart"),
                            "StringOperation.trimStart(flags, stringBin)"),
                        () -> assertEquals("  hello", rec.getString("trimEnd"),
                            "StringOperation.trimEnd(flags, stringBin)"));
                }
            }
        }

        @Nested
        @DisplayName("StringOperation regexReplace")
        class OperationRegexReplace {
            Key key;

            @BeforeEach
            void seedRegexReplaceBin() {
                key = freshKey("stringOpRegexReplace");
                seed(key, b -> b.bin(DIGITS_BIN).setTo("abc123def456"));
            }

            @Test
            @DisplayName("StringOperation.regexReplace(pattern=[0-9]+, replacement=NUM, GLOBAL)")
            public void stringOperationRegexReplace() {
                seed(key, b -> b.appendOperations(StringOperation.regexReplace(
                        StringWriteFlags.DEFAULT,
                        DIGITS_BIN,
                        "[0-9]+",
                        "NUM",
                        StringRegexFlags.GLOBAL)));

                try (RecordStream rs = session.query(key)
                    .bin(DIGITS_BIN).get()
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertEquals("abcNUMdefNUM", rec.getString(DIGITS_BIN),
                        "StringOperation.regexReplace(flags, [0-9]+, NUM, GLOBAL, digitsBin)");
                }
            }
        }

        @Nested
        @DisplayName("StringExp modify projections")
        class Projections {
            Key key;

            @BeforeEach
            void seedModifyProjectionRecord() {
                key = freshKey("stringExpModifyProj");
                seed(key, b -> b
                    .bin(STRING_BIN).setTo("Hello")
                    .bin(NFC_BIN).setTo("e\u0301"));
            }

            @Test
            @DisplayName("StringExp modify API sweep (single query)")
            public void stringExpModifyProjections() {
                Exp s = Exp.stringBin(STRING_BIN);
                Exp nfc = Exp.stringBin(NFC_BIN);
                int flags = StringWriteFlags.DEFAULT;
                try (RecordStream rs = session.query(key)
                    .bin("orig").selectFrom(s)
                    .bin("up").selectFrom(StringExp.upper(flags, s))
                    .bin("low").selectFrom(StringExp.lower(flags, s))
                    .bin("repl").selectFrom(StringExp.replace(flags, Exp.val("lo"), Exp.val("LL"), s))
                    .bin("replAll").selectFrom(StringExp.replaceAll(flags, Exp.val("l"), Exp.val("L"), s))
                    .bin("inserted").selectFrom(StringExp.insert(flags, Exp.val(1), Exp.val("X"), s))
                    .bin("overwritten").selectFrom(StringExp.overwrite(flags, Exp.val(1), Exp.val("i"), s))
                    .bin("snipped").selectFrom(StringExp.snip(flags, Exp.val(1), Exp.val(4), s))
                    .bin("appended").selectFrom(StringExp.append(flags, Exp.val("!"), s))
                    .bin("prepended").selectFrom(StringExp.prepend(flags, Exp.val(">"), s))
                    .bin("paddedStart").selectFrom(StringExp.padStart(flags, Exp.val(7), Exp.val("0"), s))
                    .bin("paddedEnd").selectFrom(StringExp.padEnd(flags, Exp.val(10), Exp.val("."), s))
                    .bin("repeated").selectFrom(StringExp.repeat(flags, Exp.val(2), s))
                    .bin("folded").selectFrom(StringExp.caseFold(flags, s))
                    .bin("normalized").selectFrom(StringExp.normalizeNFC(flags, nfc))
                    .bin("concatenated").selectFrom(StringExp.concat(flags, Exp.val(List.of("!", "?")), s))
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertAll("string modify projections",
                        () -> assertEquals("Hello", rec.getString("orig"),
                            "selectFrom(stringBin s) original value"),
                        () -> assertEquals("HELLO", rec.getString("up"),
                            "StringExp.upper(flags, stringBin s)"),
                        () -> assertEquals("hello", rec.getString("low"),
                            "StringExp.lower(flags, stringBin s)"),
                        () -> assertEquals("HelLL", rec.getString("repl"),
                            "StringExp.replace(flags, lo, LL, stringBin s)"),
                        () -> assertEquals("HeLLo", rec.getString("replAll"),
                            "StringExp.replaceAll(flags, l, L, stringBin s)"),
                        () -> assertEquals("HXello", rec.getString("inserted"),
                            "StringExp.insert(flags, index=1, X, stringBin s)"),
                        () -> assertEquals("Hillo", rec.getString("overwritten"),
                            "StringExp.overwrite(flags, index=1, i, stringBin s)"),
                        () -> assertEquals("Ho", rec.getString("snipped"),
                            "StringExp.snip(flags, index=1, count=4, stringBin s)"),
                        () -> assertEquals("Hello!", rec.getString("appended"),
                            "StringExp.append(flags, !, stringBin s)"),
                        () -> assertEquals(">Hello", rec.getString("prepended"),
                            "StringExp.prepend(flags, >, stringBin s)"),
                        () -> assertEquals("00Hello", rec.getString("paddedStart"),
                            "StringExp.padStart(flags, width=7, pad=0, stringBin s)"),
                        () -> assertEquals("Hello.....", rec.getString("paddedEnd"),
                            "StringExp.padEnd(flags, width=10, pad=., stringBin s)"),
                        () -> assertEquals("HelloHello", rec.getString("repeated"),
                            "StringExp.repeat(flags, count=2, stringBin s)"),
                        () -> assertEquals("hello", rec.getString("folded"),
                            "StringExp.caseFold(flags, stringBin s)"),
                        () -> assertEquals("\u00e9", rec.getString("normalized"),
                            "StringExp.normalizeNFC(flags, nfcBin)"),
                        () -> assertEquals("Hello!?", rec.getString("concatenated"),
                            "StringExp.concat(flags, [!,?], stringBin s)"));
                }
            }
        }

        @Nested
        @DisplayName("StringExp trim")
        class Trim {
            Key key;

            @BeforeEach
            void seedTrimRecord() {
                key = freshKey("stringExpTrimProj");
                seed(key, b -> b.bin(STRING_BIN).setTo("  hello  "));
            }

            @Test
            @DisplayName("StringExp trim API sweep (single query)")
            public void stringExpTrimProjections() {
                Exp s = Exp.stringBin(STRING_BIN);
                int flags = StringWriteFlags.DEFAULT;
                try (RecordStream rs = session.query(key)
                    .bin("trimmed").selectFrom(StringExp.trim(flags, s))
                    .bin("trimStart").selectFrom(StringExp.trimStart(flags, s))
                    .bin("trimEnd").selectFrom(StringExp.trimEnd(flags, s))
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertAll("string trim projections",
                        () -> assertEquals("hello", rec.getString("trimmed"),
                            "StringExp.trim(flags, stringBin s)"),
                        () -> assertEquals("hello  ", rec.getString("trimStart"),
                            "StringExp.trimStart(flags, stringBin s)"),
                        () -> assertEquals("  hello", rec.getString("trimEnd"),
                            "StringExp.trimEnd(flags, stringBin s)"));
                }
            }
        }

        @Nested
        @DisplayName("StringExp regexReplace")
        class RegexReplace {
            Key key;

            @BeforeEach
            void seedRegexReplaceRecord() {
                key = freshKey("stringExpModifyRegex");
                seed(key, b -> b.bin(DIGITS_BIN).setTo("abc123def456"));
            }

            @Test
            @DisplayName("StringExp.regexReplace(pattern=[0-9]+, replacement=NUM, GLOBAL)")
            public void stringExpModifyProjectionsRegexReplace() {
                try (RecordStream rs = session.query(key)
                    .bin("regexOut").selectFrom(StringExp.regexReplace(
                        StringWriteFlags.DEFAULT,
                        Exp.val("[0-9]+"),
                        Exp.val("NUM"),
                        StringRegexFlags.GLOBAL,
                        Exp.stringBin(DIGITS_BIN)))
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertEquals("abcNUMdefNUM", rec.getString("regexOut"),
                        "StringExp.regexReplace(flags, [0-9]+, NUM, GLOBAL, digitsBin)");
                }
            }
        }
    }

    @Nested
    @DisplayName("query filters")
    class Filters {
        Key match;
        Key miss;

        @BeforeEach
        void seedFilterRecords() {
            match = freshKey("stringExpFilterYes");
            miss = freshKey("stringExpFilterNo");
            seed(match, b -> b.bin(STRING_BIN).setTo("hello"));
            seed(miss, b -> b.bin(STRING_BIN).setTo("goodbye"));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("stringExpFilters")
        public void stringExpFiltersOnStringBin(String label, Function<Exp, Exp> filter, int expectedCount) {
            Exp s = Exp.stringBin(STRING_BIN);
            try (RecordStream rs = session.query(match, miss)
                .where(filter.apply(s))
                .execute()) {
                assertEquals(expectedCount, countResults(rs), label);
            }
        }

        private static Stream<Arguments> stringExpFilters() {
            return Stream.of(
                Arguments.of("StringExp.startsWith(hel)",
                    (Function<Exp, Exp>) s -> StringExp.startsWith(Exp.val("hel"), s), 1),
                Arguments.of("StringExp.contains(ell)",
                    (Function<Exp, Exp>) s -> StringExp.contains(Exp.val("ell"), s), 1),
                Arguments.of("StringExp.endsWith(lo)",
                    (Function<Exp, Exp>) s -> StringExp.endsWith(Exp.val("lo"), s), 1),
                Arguments.of("StringExp.regexCompare(^hello$)",
                    (Function<Exp, Exp>) s -> StringExp.regexCompare(Exp.val("^hello$"), 0, s), 1));
        }
    }

    /**
     * String AEL equivalents of the {@link Exp} tests above. Disabled until
     * the server accepts these forms in selectFrom/filter (currently Parameter error).
     */
    @Nested
    @DisplayName("string AEL")
    @Disabled("server-side string AEL fails (Parameter error): "
        + "string read projections in selectFrom are not validated")
    class StringAel {

        @Test
        @DisplayName("strlen, substr, find via AEL selectFrom (single query)")
        public void stringProjectionViaStringExpOnQuery() {
            Key key = freshKey("stringExpQueryAel");
            seed(key, b -> b.bin(STRING_BIN).setTo("hello"));

            String slen = "$." + STRING_BIN + ".strlen()";
            String stail = "$." + STRING_BIN + ".substr(3)";
            String sfind = "$." + STRING_BIN + ".find('ll')";

            try (RecordStream rs = session.query(key)
                .bin("slen").selectFrom(slen)
                .bin("stail").selectFrom(stail)
                .bin("sfind").selectFrom(sfind)
                .execute()) {
                assertTrue(rs.hasNext());
                Record rec = rs.next().recordOrThrow();
                assertAll("string read projections (AEL)",
                    () -> assertEquals(5L, rec.getLong("slen"),
                        "$." + STRING_BIN + ".strlen()"),
                    () -> assertEquals("lo", rec.getString("stail"),
                        "$." + STRING_BIN + ".substr(3)"),
                    () -> assertEquals(2L, rec.getLong("sfind"),
                        "$." + STRING_BIN + ".find('ll')"));
            }
        }
    }

    //=================================================================
    // CTX navigation — string nested in list/map bins
    //
    // Exercises the §2.3.1 CTX-wrapper wire envelope: when CTX is non-empty
    // the op-data becomes [0xFF, ctx_list, [sub_op, args...]] — three outer
    // elements, with the sub-op and its args in their own nested array so
    // the inner arity is self-describing (SERVER-1483). The server dispatches
    // these through as_bin_string_modify_ctx_tr / its read-side twin, which
    // is a separate code path from the top-level-bin variant exercised above.
    //=================================================================

    @Test
    public void readOpOnStringNestedInList() {
        runDelete(KEY);

        List<String> list = new ArrayList<>();
        list.add("alpha");
        list.add("beta");
        list.add("gamma");

        seed(KEY, b -> b.bin(BIN).setTo(list));

        try (RecordStream rs = session.query(KEY)
            .appendOperations(
                StringOperation.strlen(BIN, CTX.listIndex(1)),
                StringOperation.find(BIN, "et", CTX.listIndex(1)))
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(4L, rec.operationResult(0).getLong(),
                "StringOperation.strlen on nested list string");
            assertEquals(1L, rec.operationResult(1).getLong(),
                "StringOperation.find on nested list string");
        }
    }

    @Test
    public void modifyOpWithFlagsOnStringNestedInList() {
        runDelete(KEY);

        List<String> list = new ArrayList<>();
        list.add("alpha");
        list.add("beta");
        list.add("gamma");

        seed(KEY, b -> b.bin(BIN).setTo(list));

        seed(KEY, b -> b.appendOperations(StringOperation.append(
            StringWriteFlags.NO_FAIL, BIN, "!", CTX.listIndex(1))));

        try (RecordStream rs = session.query(KEY).execute()) {
            AerospikeList<?> after = rs.getFirstRecord().getList(BIN);
            assertEquals(Arrays.asList("alpha", "beta!", "gamma"), after);
        }
    }

    @Test
    public void noFailFlagDecidesOutcomeOnUnreachableCtxPath() {
        runDelete(KEY);

        List<Value> list = new ArrayList<Value>();
        list.add(Value.get("alpha"));
        list.add(Value.get("beta"));

        seed(KEY, b -> b.bin(BIN).setTo(list));

        seed(KEY, b -> b.appendOperations(StringOperation.append(
            StringWriteFlags.NO_FAIL, BIN, "!", CTX.listIndex(99))));

        try (RecordStream rs = session.query(KEY).execute()) {
            AerospikeList<?> after = rs.getFirstRecord().getList(BIN);
            assertEquals(Arrays.asList("alpha", "beta"), after);
        }

        AerospikeException ae = assertThrows(AerospikeException.class, () -> seed(KEY, b -> b.appendOperations(
            StringOperation.append(StringWriteFlags.DEFAULT, BIN, "!", CTX.listIndex(99)))));

        assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());
    }

    private void seed(Key key, Consumer<ChainableOperationBuilder> configure) {
        ChainableOperationBuilder builder = session.upsert(key);
        configure.accept(builder);
        try (RecordStream ignored = builder.execute()) {
        }
    }

    private void runDelete(Key key) {
        try (RecordStream ignored = session.delete(key).execute()) {
        }
    }
}
