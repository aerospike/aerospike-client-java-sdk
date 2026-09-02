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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListPolicy;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.ListSortFlags;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.exp.MapExp;

/**
 * Integration tests for list expressions: client {@link Exp} API (always run) and
 * string AEL equivalents (run when server-side AEL supports them).
 */
public class ListExpTest extends ClusterTest {
    private static final String LIST_BIN = "A";
    private static final String LIST_BIN_B = "B";
    private static final String LIST_BIN_C = "C";

    private static List<Integer> sampleList() {
        return List.of(10, 20, 30, 40);
    }

    /** Unsorted list used to verify {@link ListExp#sort} reorders values. */
    private static List<Integer> unsortedList() {
        return List.of(30, 10, 40, 20);
    }

    /** Relative rank/range list ops require {@link ListOrder#ORDERED}. */
    private void seedOrderedList(Key key, List<Integer> values) {
        try (RecordStream rs = session.upsert(key)
            .bin(LIST_BIN).listSetOrder(ListOrder.ORDERED)
            .bin(LIST_BIN).listAppendItems(values)
            .execute()) {
        }
    }

    @Test
    public void modifyWithContext() {
        Key key = freshKey("listCtx");

        List<String> listSubA = new ArrayList<>();
        listSubA.add("e");
        listSubA.add("d");
        listSubA.add("c");
        listSubA.add("b");
        listSubA.add("a");

        List<Object> listA = new ArrayList<>();
        listA.add("a");
        listA.add("b");
        listA.add("c");
        listA.add("d");
        listA.add(listSubA);

        List<String> listB = new ArrayList<>();
        listB.add("x");
        listB.add("y");
        listB.add("z");

        try (RecordStream rs = session.upsert(key)
            .bin(LIST_BIN).listAppendItems(listA)
            .bin(LIST_BIN_B).listAppendItems(listB)
            .bin(LIST_BIN_C).setTo("M")
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();

            // TODO Port expression to AEL.
            CTX ctx = CTX.listIndex(4);

            Expression where = Exp.build(
                Exp.eq(
                    ListExp.size(
                        // Temporarily append LIST_BIN_B/LIST_BIN_C to LIST_BIN in expression.
                        ListExp.appendItems(ListPolicy.Default, Exp.listBin(LIST_BIN_B),
                            ListExp.append(ListPolicy.Default, Exp.stringBin(LIST_BIN_C), Exp.listBin(LIST_BIN), ctx),
                            ctx),
                        ctx),
                    Exp.val(9)));

            try (RecordStream queryRs = session.query(key)
                .readingOnlyBins(LIST_BIN)
                .where(where)
                .failOnFilteredOut()
                .execute()) {
                assertTrue(queryRs.hasNext());
                rec = queryRs.next().recordOrThrow();
                List<?> result = rec.getList(LIST_BIN);
                assertEquals(5, result.size());
            }

            where = Exp.build(
                Exp.eq(
                    ListExp.size(
                        // Temporarily append local listB and local "M" string to LIST_BIN in expression.
                        ListExp.appendItems(ListPolicy.Default, Exp.val(listB),
                            ListExp.append(ListPolicy.Default, Exp.val("M"), Exp.listBin(LIST_BIN), ctx),
                            ctx),
                        ctx),
                    Exp.val(9)));

            try (RecordStream queryRs = session.query(key)
                .readingOnlyBins(LIST_BIN)
                .where(where)
                .failOnFilteredOut()
                .execute()) {
                assertTrue(queryRs.hasNext());
                rec = queryRs.next().recordOrThrow();
                List<?> result = rec.getList(LIST_BIN);
                assertEquals(5, result.size());
            }
        }
    }

    @Test
    public void listExpressionFilterMapElementInListBin() {
        Key keyMatch = freshKey("listMapFilter");
        Key keyFiltered = freshKey("listMapFilterNoMatch");

        List<Map<String, Object>> listOfMaps = List.of(
                Map.of("name", "alice", "age", 30),
                Map.of("name", "bob", "age", 25)
        );

        try (RecordStream rs = session.upsert(keyMatch)
                .bin(LIST_BIN).setTo(listOfMaps)
                .execute()) {
        }

        List<Map<String, Object>> listNoMatch = List.of(
                Map.of("name", "charlie", "age", 40),
                Map.of("name", "dave", "age", 35)
        );

        try (RecordStream rs = session.upsert(keyFiltered)
            .bin(LIST_BIN).setTo(listNoMatch)
            .execute()) {
        }

        // Filter: get "name" from map at list index 0, check if it equals "alice".
        Expression filter = Exp.build(
                Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val("name"),
                        ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.MAP, Exp.val(0), Exp.listBin(LIST_BIN))),
                        Exp.val("alice")
                ));

        try (RecordStream rs = session.query(List.of(keyMatch, keyFiltered))
            .where(filter)
            .failOnFilteredOut()
            .execute()) {
            assertTrue(rs.hasNext());
            RecordResult match = rs.next();
            assertEquals(ResultCode.OK, match.getResultCode());
            Record rec = match.recordOrThrow();
            List<?> result = rec.getList(LIST_BIN);
            assertNotNull(result);
            assertEquals(2, result.size());

            assertTrue(rs.hasNext());
            assertEquals(ResultCode.FILTERED_OUT, rs.next().getResultCode());
            assertFalse(rs.hasNext());
        }
    }

    @Test
    public void expReturnsList() {
        Key key = freshKey("expReturnsList");

        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        list.add("d");

        Expression exp = Exp.build(Exp.val(list));

        try (RecordStream rs = session.upsert(key)
            .bin(LIST_BIN_C).upsertFrom(exp)
            .bin(LIST_BIN_C).get()
            .bin("var").selectFrom(exp)
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();

            List<?> results = rec.getList(LIST_BIN_C);
            assertEquals(2, results.size());

            List<?> rlist = (List<?>)results.get(1);
            assertEquals(4, rlist.size());

            List<?> results2 = rec.getList("var");
            assertEquals(4, results2.size());
        }
    }

    @Nested
    @DisplayName("read projections")
    class Reads {
        Key key;
        Exp list;

        @BeforeEach
        void seedReadRecord() {
            key = freshKey("listExpReadProj");
            seedOrderedList(key, sampleList());
            list = Exp.listBin(LIST_BIN);
        }

        @Test
        @DisplayName("ListExp read API sweep (one query per call)")
        public void listExpReadProjections() {
            assertAll("list read projections",
            () -> assertProjection(session, key, "ListExp.size(listBin)",
                ListExp.size(list),
                rec -> assertEquals(4L, rec.getLong("r"), "size")),
            () -> assertProjection(session, key,
                "ListExp.getByValue(EXISTS, value=20)",
                ListExp.getByValue(ListReturnType.EXISTS, Exp.val(20), list),
                rec -> assertTrue(rec.getBoolean("r"), "value 20 exists")),
            () -> assertProjection(session, key,
                "ListExp.getByValue(COUNT, value=20)",
                ListExp.getByValue(ListReturnType.COUNT, Exp.val(20), list),
                rec -> assertEquals(1L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "ListExp.getByValue(INDEX, value=20)",
                ListExp.getByValue(ListReturnType.INDEX, Exp.val(20), list),
                rec -> assertTrue(rec.getList("r").contains(1L), "index of 20")),
            () -> assertProjection(session, key,
                "ListExp.getByValueRange(VALUE, values [20, 35))",
                ListExp.getByValueRange(ListReturnType.VALUE, Exp.val(20), Exp.val(35), list),
                rec -> assertEquals(List.of(20L, 30L), rec.getList("r"), "values")),
            () -> assertProjection(session, key,
                "ListExp.getByValueList(COUNT, values [20, 40])",
                ListExp.getByValueList(ListReturnType.COUNT, Exp.val(List.of(20, 40)), list),
                rec -> assertEquals(2L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "ListExp.getByValueRelativeRankRange(VALUE, value=20, rank=0)",
                ListExp.getByValueRelativeRankRange(
                    ListReturnType.VALUE, Exp.val(20), Exp.val(0), list),
                rec -> assertEquals(List.of(20L, 30L, 40L), rec.getList("r"), "values")),
            () -> assertProjection(session, key,
                "ListExp.getByValueRelativeRankRange(VALUE, value=20, rank=0, count=2)",
                ListExp.getByValueRelativeRankRange(
                    ListReturnType.VALUE, Exp.val(20), Exp.val(0), Exp.val(2), list),
                rec -> assertEquals(List.of(20L, 30L), rec.getList("r"), "values")),
            () -> assertProjection(session, key,
                "ListExp.getByValueRelativeRankRange(COUNT, value=20, rank=0, count=2)",
                ListExp.getByValueRelativeRankRange(
                    ListReturnType.COUNT, Exp.val(20), Exp.val(0), Exp.val(2), list),
                rec -> assertEquals(2L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "ListExp.getByIndex(VALUE, index=0)",
                ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0), list),
                rec -> assertEquals(10L, rec.getLong("r"), "value at index 0")),
            () -> assertProjection(session, key,
                "ListExp.getByIndexRange(COUNT, index=1)",
                ListExp.getByIndexRange(ListReturnType.COUNT, Exp.val(1), list),
                rec -> assertEquals(3L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "ListExp.getByIndexRange(COUNT, index=1, count=2)",
                ListExp.getByIndexRange(ListReturnType.COUNT, Exp.val(1), Exp.val(2), list),
                rec -> assertEquals(2L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "ListExp.getByRank(VALUE, rank=0)",
                ListExp.getByRank(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0), list),
                rec -> assertEquals(10L, rec.getLong("r"), "value at rank 0")),
            () -> assertProjection(session, key,
                "ListExp.getByRankRange(COUNT, rank=0)",
                ListExp.getByRankRange(ListReturnType.COUNT, Exp.val(0), list),
                rec -> assertEquals(4L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "ListExp.getByRankRange(COUNT, rank=0, count=2)",
                ListExp.getByRankRange(ListReturnType.COUNT, Exp.val(0), Exp.val(2), list),
                rec -> assertEquals(2L, rec.getLong("r"), "count"))
            );
        }
    }

    @Nested
    @DisplayName("modify projections")
    class Modifies {
        @Nested
        @DisplayName("put")
        class Put {
            Key key;
            Exp list;

            @BeforeEach
            void seedPutRecord() {
                key = freshKey("listExpModPut");
                try (RecordStream rs = session.upsert(key)
                    .bin(LIST_BIN).setTo(unsortedList())
                    .execute()) {
                }
                list = Exp.listBin(LIST_BIN);
            }

            @Test
            @DisplayName("ListExp modify put API sweep (single query)")
            public void listExpModifyPutProjections() {
                try (RecordStream rs = session.query(key)
                    .bin("appended").selectFrom(ListExp.append(ListPolicy.Default, Exp.val(50), list))
                    .bin("appendItems").selectFrom(ListExp.appendItems(
                        ListPolicy.Default, Exp.val(List.of(99)), list))
                    .bin("inserted").selectFrom(ListExp.insert(
                        ListPolicy.Default, Exp.val(1), Exp.val(15), list))
                    .bin("insertItems").selectFrom(ListExp.insertItems(
                        ListPolicy.Default, Exp.val(2), Exp.val(List.of(12, 13)), list))
                    .bin("incremented").selectFrom(ListExp.increment(
                        ListPolicy.Default, Exp.val(2), Exp.val(5), list))
                    .bin("setAt").selectFrom(ListExp.set(ListPolicy.Default, Exp.val(1), Exp.val(25), list))
                    .bin("sorted").selectFrom(ListExp.sort(ListSortFlags.DEFAULT, list))
                    .bin("origSize").selectFrom(ListExp.size(list))
                    .execute()) {
                    assertTrue(rs.hasNext());
                    Record rec = rs.next().recordOrThrow();
                    assertAll("list modify put projections",
                        () -> assertEquals(5, rec.getList("appended").size(),
                            "ListExp.append(value=50) result size"),
                        () -> assertEquals(50L, rec.getList("appended").get(4),
                            "ListExp.append(value=50) appended value"),
                        () -> assertEquals(5, rec.getList("appendItems").size(),
                            "ListExp.appendItems([99]) result size"),
                        () -> assertEquals(99L, rec.getList("appendItems").get(4),
                            "ListExp.appendItems([99]) appended value"),
                        () -> assertEquals(5, rec.getList("inserted").size(),
                            "ListExp.insert(index=1, value=15) result size"),
                        () -> assertEquals(15L, rec.getList("inserted").get(1),
                            "ListExp.insert(index=1, value=15) inserted value"),
                        () -> assertEquals(6, rec.getList("insertItems").size(),
                            "ListExp.insertItems(index=2, values=[12,13]) result size"),
                        () -> assertEquals(45L, rec.getList("incremented").get(2),
                            "ListExp.increment(index=2, delta=5) value at index 2"),
                        () -> assertEquals(25L, rec.getList("setAt").get(1),
                            "ListExp.set(index=1, value=25) value at index 1"),
                        () -> assertEquals(List.of(10L, 20L, 30L, 40L), rec.getList("sorted"),
                            "ListExp.sort(DEFAULT) sorted values"),
                        () -> assertEquals(4L, rec.getLong("origSize"),
                            "ListExp.size(listBin) original size unchanged"));
                }

                try (RecordStream rs = session.query(key).execute()) {
                    assertTrue(rs.hasNext());
                    assertEquals(4, rs.next().recordOrThrow().getList(LIST_BIN).size(),
                        "stored listBin size after modify put projections");
                }
            }
        }

        @Nested
        @DisplayName("remove")
        class Remove {
            Key key;
            Exp list;

            @BeforeEach
            void seedRemoveRecord() {
                key = freshKey("listExpModRemove");
                seedOrderedList(key, sampleList());
                list = Exp.listBin(LIST_BIN);
            }

            @Test
            @DisplayName("ListExp modify remove API sweep (one query per call)")
            public void listExpModifyRemoveProjections() {
                assertAll("list modify remove projections",
                    () -> assertProjection(session, key, "ListExp.clear(listBin)",
                        ListExp.clear(list),
                        rec -> assertEquals(0, rec.getList("r").size(), "result list size")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByValue(INVERTED, value=20)",
                        ListExp.removeByValue(ListReturnType.INVERTED, Exp.val(20), list),
                        rec -> assertEquals(List.of(20L), rec.getList("r"), "removed values")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByValueList(INVERTED, values [20, 40])",
                        ListExp.removeByValueList(ListReturnType.INVERTED, Exp.val(List.of(20, 40)), list),
                        rec -> assertEquals(List.of(20L, 40L), rec.getList("r"), "removed values")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByValueRange(INVERTED, values [20, 35))",
                        ListExp.removeByValueRange(
                            ListReturnType.INVERTED, Exp.val(20), Exp.val(35), list),
                        rec -> assertEquals(List.of(20L, 30L), rec.getList("r"), "removed values")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByValueRelativeRankRange(INVERTED, value=20, rank=0)",
                        ListExp.removeByValueRelativeRankRange(
                            ListReturnType.INVERTED, Exp.val(20), Exp.val(0), list),
                        rec -> assertEquals(List.of(20L, 30L, 40L), rec.getList("r"), "removed values")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByValueRelativeRankRange(INVERTED, value=20, rank=0, count=2)",
                        ListExp.removeByValueRelativeRankRange(
                            ListReturnType.INVERTED, Exp.val(20), Exp.val(0), Exp.val(2), list),
                        rec -> assertEquals(List.of(20L, 30L), rec.getList("r"), "removed values")),
                    () -> assertProjection(session, key, "ListExp.removeByIndex(index=0)",
                        ListExp.removeByIndex(Exp.val(0), list),
                        rec -> assertEquals(List.of(20L, 30L, 40L), rec.getList("r"), "remaining values")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByIndexRange(INVERTED, index=1)",
                        ListExp.removeByIndexRange(ListReturnType.INVERTED, Exp.val(1), list),
                        rec -> assertEquals(List.of(20L, 30L, 40L), rec.getList("r"), "removed slice")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByIndexRange(INVERTED, index=1, count=2)",
                        ListExp.removeByIndexRange(ListReturnType.INVERTED, Exp.val(1), Exp.val(2), list),
                        rec -> assertEquals(List.of(20L, 30L), rec.getList("r"), "removed slice")),
                    () -> assertProjection(session, key, "ListExp.removeByRank(rank=0)",
                        ListExp.removeByRank(Exp.val(0), list),
                        rec -> assertEquals(List.of(20L, 30L, 40L), rec.getList("r"), "remaining values")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByRankRange(INVERTED, rank=1)",
                        ListExp.removeByRankRange(ListReturnType.INVERTED, Exp.val(1), list),
                        rec -> assertEquals(List.of(20L, 30L, 40L), rec.getList("r"), "removed slice")),
                    () -> assertProjection(session, key,
                        "ListExp.removeByRankRange(INVERTED, rank=1, count=2)",
                        ListExp.removeByRankRange(ListReturnType.INVERTED, Exp.val(1), Exp.val(2), list),
                        rec -> assertEquals(List.of(20L, 30L), rec.getList("r"), "removed slice"))
                );
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
            match = freshKey("listExpFilterYes");
            miss = freshKey("listExpFilterNo");
            seedOrderedList(match, sampleList());
            try (RecordStream rs = session.upsert(miss)
                .bin(LIST_BIN).setTo(List.of(1, 2))
                .execute()) {
            }
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("listExpFilters")
        public void listExpFiltersOnListBin(String label, Function<Exp, Exp> filter, int expectedCount) {
            Exp list = Exp.listBin(LIST_BIN);
            try (RecordStream rs = session.query(match, miss)
                .where(filter.apply(list))
                .execute()) {
                assertEquals(expectedCount, countResults(rs), label);
            }
        }

        private static Stream<Arguments> listExpFilters() {
            return Stream.of(
                Arguments.of("ListExp.size > 3",
                    (Function<Exp, Exp>) list -> Exp.gt(ListExp.size(list), Exp.val(3)), 1),
                Arguments.of("ListExp.getByValue(EXISTS, value=30)",
                    (Function<Exp, Exp>) list ->
                        ListExp.getByValue(ListReturnType.EXISTS, Exp.val(30), list), 1));
        }
    }

    /**
     * String AEL equivalents of the {@link Exp} tests above. Skipped on 8.1.3+ until
     * the server accepts these forms in selectFrom/filter (currently Parameter error).
     */
    @Nested
    @DisplayName("string AEL")
    class StringAel {

        @Test
        @DisplayName("getByValue(return: INDEX) via AEL selectFrom")
        public void listExpressionWithReturnTypeIndex() {
            assumeSupportsAel();
            assumeFalse(supportsAel(),
                "server-side string AEL fails (Parameter error): "
                    + "list getByValue(return: INDEX) in selectFrom is not validated");

            Key key = freshKey("listRetIndexAel");
            seedOrderedList(key, sampleList());

            String readExp = "$." + LIST_BIN + ".{=20}.get(return: INDEX)";

            try (RecordStream rs = session.query(key)
                .bin(LIST_BIN).selectFrom(readExp)
                .execute()) {
                assertTrue(rs.hasNext());
                Record rec = rs.next().recordOrThrow();
                List<?> indices = rec.getList(LIST_BIN);
                assertNotNull(indices);
                assertTrue(indices.contains(1L), "Expected index 1 for value 20");
            }
        }

        @Test
        @DisplayName("getByValueRelativeRankRange via AEL selectFrom")
        public void relativeRankListExpressionOrder() {
            assumeSupportsAel();
            assumeFalse(supportsAel(),
                "server-side string AEL fails (Parameter error): "
                    + "list getByValueRelativeRankRange in selectFrom is not validated");

            Key key = freshKey("relRankAel");
            seedOrderedList(key, sampleList());

            String readExp = "$." + LIST_BIN + ".{=20:0:2}.get(return: VALUE)";

            try (RecordStream rs = session.query(key)
                .bin(LIST_BIN).selectFrom(readExp)
                .execute()) {
                assertTrue(rs.hasNext());
                Record rec = rs.next().recordOrThrow();
                List<?> result = rec.getList(LIST_BIN);
                assertNotNull(result);
                assertEquals(2, result.size());
                assertTrue(result.contains(20L));
                assertTrue(result.contains(30L));
            }
        }
    }

    @Test
    public void listJoinExp() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(8, 1, 3, 0),
            "List join requires server version 8.1.3 or later");

        Key key = args.set.id("explistjoin");

        session.delete(key).execute();

        List<Value> items = new ArrayList<Value>();
        items.add(Value.get("a"));
        items.add(Value.get("b"));
        items.add(Value.get("c"));

        session.upsert(key)
            .bin("jbin").setTo(items)
            .execute();

        Exp plainExp = ListExp.join(Exp.listBin("jbin"));
        Exp sepExp = ListExp.join(Exp.val("-"), Exp.listBin("jbin"));

        Record rec = session.query(key)
            .bin("plain").selectFrom(plainExp)
            .bin("sep").selectFrom(sepExp)
            .execute()
            .getFirstRecord();

        assertEquals("abc", rec.getString("plain"));
        assertEquals("a-b-c", rec.getString("sep"));
    }
}
