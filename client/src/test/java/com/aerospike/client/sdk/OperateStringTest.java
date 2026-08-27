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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
 * equivalents (run when server-side AEL supports them).
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
        @DisplayName("BinBuilder / StringOperation")
        class Fluent {
            Key key;

            @BeforeEach
            void seedHelloString() {
                key = freshKey("stringFluentReads");
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).setTo("hello")
                    .execute()) {
                }
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

            @Test
            @DisplayName("StringOperation strlen, substr, find via appendOperations")
            public void stringReadsViaAppendOperations() {
                try (RecordStream rs = session.upsert(key)
                    .appendOperations(
                        StringOperation.strlen(STRING_BIN),
                        StringOperation.substr(STRING_BIN, 1, 4),
                        StringOperation.substr(STRING_BIN, 3),
                        StringOperation.find(STRING_BIN, "ll"))
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertEquals(5L, rec.operationResult(0).getLong(), "StringOperation.strlen(stringBin s)");
                    assertEquals("ell", rec.operationResult(1).getString(),
                        "StringOperation.substr(offset=1, count=4)");
                    assertEquals("lo", rec.operationResult(2).getString(),
                        "StringOperation.substr(offset=3)");
                    assertEquals(2L, rec.operationResult(3).getLong(), "StringOperation.find(needle=ll)");
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
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).setTo("Hello,42")
                    .bin(NUM_BIN).setTo("12345")
                    .bin(FLT_BIN).setTo("3.14")
                    .bin(B64_BIN).setTo("aGVsbG8=")
                    .bin(INT_BIN).setTo(42)
                    .execute()) {
                }
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
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).setTo("ab")
                    .execute()) {
                }
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
        @DisplayName("StringExp modify projections")
        class Projections {
            Key key;

            @BeforeEach
            void seedModifyProjectionRecord() {
                key = freshKey("stringExpModifyProj");
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).setTo("Hello")
                    .bin(NFC_BIN).setTo("e\u0301")
                    .execute()) {
                }
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
                try (RecordStream rs = session.upsert(key)
                    .bin(STRING_BIN).setTo("  hello  ")
                    .execute()) {
                }
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
                try (RecordStream rs = session.upsert(key)
                    .bin(DIGITS_BIN).setTo("abc123def456")
                    .execute()) {
                }
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
            try (RecordStream rs = session.upsert(match)
                .bin(STRING_BIN).setTo("hello")
                .execute()) {
            }
            try (RecordStream rs = session.upsert(miss)
                .bin(STRING_BIN).setTo("goodbye")
                .execute()) {
            }
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
     * String AEL equivalents of the {@link Exp} tests above. Skipped on 8.1.3+ until
     * the server accepts these forms in selectFrom/filter (currently Parameter error).
     */
    @Nested
    @RequiresServerFeature(ServerFeature.AEL)
    @DisplayName("string AEL")
    class StringAel {

        @Test
        @DisplayName("strlen, substr, find via AEL selectFrom (single query)")
        public void stringProjectionViaStringExpOnQuery() {
            assumeFalse(supportsAel(),
                "server-side string AEL fails (Parameter error): "
                    + "string read projections in selectFrom are not validated");

            Key key = freshKey("stringExpQueryAel");
            try (RecordStream rs = session.upsert(key)
                .bin(STRING_BIN).setTo("hello")
                .execute()) {
            }

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
    public void modifyOpWithFlagsOnStringNestedInList() {
        session.delete(KEY).execute();

        List<String> list = new ArrayList<>();
        list.add("alpha");
        list.add("beta");
        list.add("gamma");

        session.upsert(KEY)
            .bin(BIN).setTo(list)
            .execute();

        session.upsert(KEY)
            .bin(BIN).onListIndex(1).listAppend("!", ops -> ops.allowFailures())
            .execute();

        AerospikeList<?> after = session.query(KEY)
            .execute()
            .getFirstRecord()
            .getList(BIN);

        assertEquals(Arrays.asList("alpha", "beta!", "gamma"), after);
    }

    @Test
    public void noFailFlagDecidesOutcomeOnUnreachableCtxPath() {
        session.delete(KEY).execute();

        List<Value> list = new ArrayList<Value>();
        list.add(Value.get("alpha"));
        list.add(Value.get("beta"));

        session.upsert(KEY)
            .bin(BIN).setTo(list)
            .execute();

        session.upsert(KEY)
            .bin(BIN).onListIndex(99).listAppend("!", ops -> ops.allowFailures())
            .execute();

        AerospikeList<?> after = session.query(KEY)
            .execute()
            .getFirstRecord()
            .getList(BIN);

        assertEquals(Arrays.asList("alpha", "beta"), after);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session.upsert(KEY)
                .bin(BIN).onListIndex(99).listAppend("!")
                .execute();
        });

        assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());
    }
}
