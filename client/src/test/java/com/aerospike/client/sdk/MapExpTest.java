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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapPolicy;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.MapExp;

/**
 * Integration tests for map expressions: client {@link Exp} API (always run) and
 * string AEL equivalents (run when server-side AEL supports them).
 */
public class MapExpTest extends ClusterTest {

    private static final String binName = "m";

    /** Ordered map {@code {0=17, 4=2, 5=15, 9=10}} used across read/range tests. */
    private static Map<Long, Long> sampleOrderedMap() {
        Map<Long, Long> map = new LinkedHashMap<>();
        map.put(0L, 17L);
        map.put(4L, 2L);
        map.put(5L, 15L);
        map.put(9L, 10L);
        return map;
    }

    @Test
    public void sortedMapEquality() {
        TreeMap<String,String> map = new TreeMap<>();
        map.put("key1", "e");
        map.put("key2", "d");
        map.put("key3", "c");
        map.put("key4", "b");
        map.put("key5", "a");

        Key key = freshKey("sortedMapEquality");

        try (RecordStream rs = session.upsert(key)
            .bin(binName).setTo(map)
            .execute()) {
        }

        Expression where = Exp.build(Exp.eq(Exp.mapBin(binName), Exp.val(map)));

        try (RecordStream rs = session.query(key)
            .readingOnlyBins(binName)
            .failOnFilteredOut()
            .where(where)
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            AerospikeMap<?,?> m = rec.getMap(binName);

            // A sorted map is returned as a LinkedHashMap for performance.
            // The response is ordered, so the LinkedHashMap insertion order
            // will match the sort order.
            assertEquals(MapOrder.KEY_ORDERED, m.getOrder());
        }
    }

    @Test
    public void invertedMapExp() {
        HashMap<String,Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 2);
        map.put("d", 3);

        Key key = freshKey("ime");

        try (RecordStream rs = session.upsert(key)
            .bin(binName).setTo(map)
            .execute()) {
        }

        // INVERTED remove returns the map with entries removed where value != 2.
        Expression readExp = Exp.build(
            MapExp.removeByValue(MapReturnType.INVERTED, Exp.val(2), Exp.mapBin(binName)));

        try (RecordStream rs = session.query(key)
            .bin(binName).selectFrom(readExp)
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            Map<?,?> m = rec.getMap(binName);
            assertEquals(2L, m.size());
            assertEquals(2L, m.get("b"));
            assertEquals(2L, m.get("c"));
        }
    }

    @Test
    @DisplayName("MapExp read API sweep (one query per call)")
    public void mapExpReadProjections() {
        Key key = freshKey("mapExpReadProj");
        try (RecordStream rs = session.upsert(key)
            .bin(binName).setTo(sampleOrderedMap())
            .execute()) {
        }

        Exp m = Exp.mapBin(binName);
        assertAll("map read projections",
            () -> assertProjection(session, key, "MapExp.size(mapBin)",
                MapExp.size(m),
                rec -> assertEquals(4L, rec.getLong("r"), "size")),
            () -> assertProjection(session, key,
                "MapExp.getByKey(EXISTS, key=5)",
                MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(5L), m),
                rec -> assertTrue(rec.getBoolean("r"), "key 5 exists")),
            () -> assertProjection(session, key,
                "MapExp.getByKey(VALUE, key=5)",
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(5L), m),
                rec -> assertEquals(15L, rec.getLong("r"), "value at key 5")),
            () -> assertProjection(session, key,
                "MapExp.getByKeyRange(KEY_VALUE, keys [4, 10))",
                MapExp.getByKeyRange(MapReturnType.KEY_VALUE, Exp.val(4L), Exp.val(10L), m),
                rec -> assertEquals(3, rec.getMap("r").size(), "entry count")),
            () -> assertProjection(session, key,
                "MapExp.getByKeyList(COUNT, keys [0, 9])",
                MapExp.getByKeyList(MapReturnType.COUNT, Exp.val(List.of(0L, 9L)), m),
                rec -> assertEquals(2L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByKeyRelativeIndexRange(KEY_VALUE, key=5, index=0)",
                MapExp.getByKeyRelativeIndexRange(
                    MapReturnType.KEY_VALUE, Exp.val(5L), Exp.val(0), m),
                rec -> {
                    assertEquals(2, rec.getMap("r").size(), "entry count");
                    assertEquals(15L, rec.getMap("r").get(5L), "value at key 5");
                }),
            () -> assertProjection(session, key,
                "MapExp.getByKeyRelativeIndexRange(COUNT, key=5, index=0, count=1)",
                MapExp.getByKeyRelativeIndexRange(
                    MapReturnType.COUNT, Exp.val(5L), Exp.val(0), Exp.val(1), m),
                rec -> assertEquals(1L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByValue(EXISTS, value=15)",
                MapExp.getByValue(MapReturnType.EXISTS, Exp.val(15L), m),
                rec -> assertTrue(rec.getBoolean("r"), "value 15 exists")),
            () -> assertProjection(session, key,
                "MapExp.getByValueRange(COUNT, values [2, 16))",
                MapExp.getByValueRange(MapReturnType.COUNT, Exp.val(2L), Exp.val(16L), m),
                rec -> assertEquals(3L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByValueList(COUNT, values [2, 10])",
                MapExp.getByValueList(MapReturnType.COUNT, Exp.val(List.of(2L, 10L)), m),
                rec -> assertEquals(2L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByValueRelativeRankRange(KEY_VALUE, value=11, rank=1)",
                MapExp.getByValueRelativeRankRange(
                    MapReturnType.KEY_VALUE, Exp.val(11L), Exp.val(1), m),
                rec -> {
                    assertEquals(1, rec.getMap("r").size(), "entry count");
                    assertEquals(17L, rec.getMap("r").get(0L), "value at key 0");
                }),
            () -> assertProjection(session, key,
                "MapExp.getByValueRelativeRankRange(COUNT, value=11, rank=1, count=1)",
                MapExp.getByValueRelativeRankRange(
                    MapReturnType.COUNT, Exp.val(11L), Exp.val(1), Exp.val(1), m),
                rec -> assertEquals(1L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByIndex(VALUE, index=0)",
                MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.INT, Exp.val(0), m),
                rec -> assertEquals(17L, rec.getLong("r"), "value at index 0")),
            () -> assertProjection(session, key,
                "MapExp.getByIndexRange(COUNT, index=1)",
                MapExp.getByIndexRange(MapReturnType.COUNT, Exp.val(1), m),
                rec -> assertEquals(3L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByIndexRange(COUNT, index=1, count=2)",
                MapExp.getByIndexRange(MapReturnType.COUNT, Exp.val(1), Exp.val(2), m),
                rec -> assertEquals(2L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByRank(VALUE, rank=0)",
                MapExp.getByRank(MapReturnType.VALUE, Exp.Type.INT, Exp.val(0), m),
                rec -> assertEquals(2L, rec.getLong("r"), "value at rank 0")),
            () -> assertProjection(session, key,
                "MapExp.getByRankRange(COUNT, rank=0)",
                MapExp.getByRankRange(MapReturnType.COUNT, Exp.val(0), m),
                rec -> assertEquals(4L, rec.getLong("r"), "count")),
            () -> assertProjection(session, key,
                "MapExp.getByRankRange(COUNT, rank=0, count=2)",
                MapExp.getByRankRange(MapReturnType.COUNT, Exp.val(0), Exp.val(2), m),
                rec -> assertEquals(2L, rec.getLong("r"), "count"))
        );
    }

    @Test
    @DisplayName("MapExp modify put API sweep (single query)")
    public void mapExpModifyPutProjections() {
        Key key = freshKey("mapExpModPut");
        Map<String, Long> seed = new HashMap<>();
        seed.put("a", 1L);
        seed.put("b", 10L);
        try (RecordStream rs = session.upsert(key)
            .bin(binName).setTo(seed)
            .execute()) {
        }

        Exp m = Exp.mapBin(binName);
        try (RecordStream rs = session.query(key)
            .bin("put").selectFrom(MapExp.put(MapPolicy.Default, Exp.val("c"), Exp.val(3L), m))
            .bin("items").selectFrom(MapExp.putItems(MapPolicy.Default, Exp.val(Map.of("d", 4L)), m))
            .bin("incr").selectFrom(MapExp.increment(MapPolicy.Default, Exp.val("b"), Exp.val(7L), m))
            .bin("origSize").selectFrom(MapExp.size(m))
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertAll("map modify put projections",
                () -> assertEquals(3, rec.getMap("put").size(),
                    "MapExp.put(key=c, value=3) result size"),
                () -> assertEquals(3L, rec.getMap("put").get("c"),
                    "MapExp.put(key=c, value=3) value at c"),
                () -> assertEquals(3, rec.getMap("items").size(),
                    "MapExp.putItems({d=4}) result size"),
                () -> assertEquals(4L, rec.getMap("items").get("d"),
                    "MapExp.putItems({d=4}) value at d"),
                () -> assertEquals(17L, rec.getMap("incr").get("b"),
                    "MapExp.increment(key=b, delta=7) value at b"),
                () -> assertEquals(2L, rec.getLong("origSize"),
                    "MapExp.size(mapBin) original size unchanged"));
        }

        try (RecordStream rs = session.query(key).execute()) {
            assertTrue(rs.hasNext());
            Map<?,?> stored = rs.next().recordOrThrow().getMap(binName);
            assertEquals(2, stored.size(), "stored mapBin size after modify put projections");
            assertEquals(1L, stored.get("a"), "stored mapBin value at a unchanged");
            assertEquals(10L, stored.get("b"), "stored mapBin value at b unchanged");
        }
    }

    @Test
    @DisplayName("MapExp modify remove API sweep (one query per call)")
    public void mapExpModifyRemoveProjections() {
        Key key = freshKey("mapExpModRemove");
        try (RecordStream rs = session.upsert(key)
            .bin(binName).setTo(sampleOrderedMap())
            .execute()) {
        }

        Exp m = Exp.mapBin(binName);
        assertAll("map modify remove projections",
            () -> assertProjection(session, key, "MapExp.clear(mapBin)",
                MapExp.clear(m),
                rec -> assertEquals(0, rec.getMap("r").size(), "result map size")),
            () -> assertProjection(session, key, "MapExp.removeByKey(key=9)",
                MapExp.removeByKey(Exp.val(9L), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertFalse(rec.getMap("r").containsKey(9L), "key 9 removed");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByKeyList(INVERTED, keys [5, 9])",
                MapExp.removeByKeyList(MapReturnType.INVERTED, Exp.val(List.of(5L, 9L)), m),
                rec -> {
                    assertEquals(2, rec.getMap("r").size(), "result map size");
                    assertEquals(15L, rec.getMap("r").get(5L), "value at key 5");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByKeyRange(INVERTED, keys [4, 10))",
                MapExp.removeByKeyRange(MapReturnType.INVERTED, Exp.val(4L), Exp.val(10L), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertEquals(2L, rec.getMap("r").get(4L), "value at key 4");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByKeyRelativeIndexRange(INVERTED, key=5, index=0)",
                MapExp.removeByKeyRelativeIndexRange(
                    MapReturnType.INVERTED, Exp.val(5L), Exp.val(0), m),
                rec -> {
                    assertEquals(2, rec.getMap("r").size(), "result map size");
                    assertEquals(15L, rec.getMap("r").get(5L), "value at key 5");
                    assertEquals(10L, rec.getMap("r").get(9L), "value at key 9");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByKeyRelativeIndexRange(INVERTED, key=5, index=0, count=1)",
                MapExp.removeByKeyRelativeIndexRange(
                    MapReturnType.INVERTED, Exp.val(5L), Exp.val(0), Exp.val(1), m),
                rec -> assertEquals(1, rec.getMap("r").size(), "result map size")),
            () -> assertProjection(session, key,
                "MapExp.removeByValueList(INVERTED, values [2, 10])",
                MapExp.removeByValueList(MapReturnType.INVERTED, Exp.val(List.of(2L, 10L)), m),
                rec -> {
                    assertEquals(2, rec.getMap("r").size(), "result map size");
                    assertEquals(2L, rec.getMap("r").get(4L), "value at key 4");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByValueRange(INVERTED, values [2, 16))",
                MapExp.removeByValueRange(MapReturnType.INVERTED, Exp.val(2L), Exp.val(16L), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertEquals(15L, rec.getMap("r").get(5L), "value at key 5");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByValueRelativeRankRange(INVERTED, value=11, rank=1)",
                MapExp.removeByValueRelativeRankRange(
                    MapReturnType.INVERTED, Exp.val(11L), Exp.val(1), m),
                rec -> {
                    assertEquals(1, rec.getMap("r").size(), "result map size");
                    assertEquals(17L, rec.getMap("r").get(0L), "value at key 0");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByValueRelativeRankRange(INVERTED, value=11, rank=1, count=1)",
                MapExp.removeByValueRelativeRankRange(
                    MapReturnType.INVERTED, Exp.val(11L), Exp.val(1), Exp.val(1), m),
                rec -> assertEquals(1, rec.getMap("r").size(), "result map size")),
            () -> assertProjection(session, key, "MapExp.removeByIndex(index=0)",
                MapExp.removeByIndex(Exp.val(0), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertFalse(rec.getMap("r").containsKey(0L), "key 0 removed");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByIndexRange(INVERTED, index=1)",
                MapExp.removeByIndexRange(MapReturnType.INVERTED, Exp.val(1), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertEquals(2L, rec.getMap("r").get(4L), "value at key 4");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByIndexRange(INVERTED, index=1, count=2)",
                MapExp.removeByIndexRange(MapReturnType.INVERTED, Exp.val(1), Exp.val(2), m),
                rec -> assertEquals(2, rec.getMap("r").size(), "result map size")),
            () -> assertProjection(session, key, "MapExp.removeByRank(rank=0)",
                MapExp.removeByRank(Exp.val(0), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertFalse(rec.getMap("r").containsKey(4L), "key 4 (rank 0) removed");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByRankRange(INVERTED, rank=1)",
                MapExp.removeByRankRange(MapReturnType.INVERTED, Exp.val(1), m),
                rec -> {
                    assertEquals(3, rec.getMap("r").size(), "result map size");
                    assertEquals(10L, rec.getMap("r").get(9L), "value at key 9");
                }),
            () -> assertProjection(session, key,
                "MapExp.removeByRankRange(INVERTED, rank=1, count=2)",
                MapExp.removeByRankRange(MapReturnType.INVERTED, Exp.val(1), Exp.val(2), m),
                rec -> assertEquals(2, rec.getMap("r").size(), "result map size"))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mapExpFilters")
    public void mapExpFiltersOnMapBin(String label, Function<Exp, Exp> filter, int expectedCount) {
        Key match = freshKey("mapExpFilterYes");
        Key miss = freshKey("mapExpFilterNo");
        try (RecordStream rs = session.upsert(match)
            .bin(binName).setTo(sampleOrderedMap())
            .execute()) {
        }
        try (RecordStream rs = session.upsert(miss)
            .bin(binName).setTo(Map.of(1L, 1L, 2L, 2L))
            .execute()) {
        }

        Exp m = Exp.mapBin(binName);
        try (RecordStream rs = session.query(match, miss)
            .where(filter.apply(m))
            .execute()) {
            assertEquals(expectedCount, countResults(rs), label);
        }
    }

    private static Stream<Arguments> mapExpFilters() {
        return Stream.of(
            Arguments.of("MapExp.size > 3",
                (Function<Exp, Exp>) map -> Exp.gt(MapExp.size(map), Exp.val(3)), 1),
            Arguments.of("MapExp.getByValue(EXISTS, value=15)",
                (Function<Exp, Exp>) map ->
                    MapExp.getByValue(MapReturnType.EXISTS, Exp.val(15L), map), 1));
    }

    /**
     * String AEL equivalents of the {@link Exp} tests above. Skipped on 8.1.3+ until
     * the server accepts these forms (currently Parameter error).
     */
    @Nested
    @DisplayName("string AEL")
    class StringAel {

        @Test
        public void sortedMapEquality() {
            assumeSupportsAel();
            assumeFalse(supportsAel(),
                "server-side string AEL fails (Parameter error): map equality filter "
                    + "($.m.get(type: MAP) == {...}) cannot compare KEY_ORDERED map ordering");

            TreeMap<String,String> map = new TreeMap<>();
            map.put("key1", "e");
            map.put("key2", "d");
            map.put("key3", "c");
            map.put("key4", "b");
            map.put("key5", "a");

            Key key = freshKey("sortedMapEqualityAel");

            try (RecordStream rs = session.upsert(key)
                .bin(binName).setTo(map)
                .execute()) {
            }

            String where = "$." + binName + ".get(type: MAP) == {'key1': 'e', 'key2': 'd', 'key3': 'c', 'key4': 'b', 'key5': 'a'}";

            try (RecordStream rs = session.query(key)
                .readingOnlyBins(binName)
                .failOnFilteredOut()
                .where(where)
                .execute()) {
                assertTrue(rs.hasNext());
                Record rec = rs.next().recordOrThrow();
                AerospikeMap<?,?> m = rec.getMap(binName);
                assertEquals(MapOrder.KEY_ORDERED, m.getOrder());
            }
        }

        @Test
        public void invertedMapExp() {
            assumeSupportsAel();
            assumeFalse(supportsAel(),
                "server-side string AEL fails (Parameter error): "
                    + "$.m.{=n}.get(return: ORDERED_MAP) in selectFrom is not supported");

            HashMap<String,Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 2);
            map.put("d", 3);

            Key key = freshKey("imeAel");

            try (RecordStream rs = session.upsert(key)
                .bin(binName).setTo(map)
                .execute()) {
            }

            String readExp = "$." + binName + ".{=2}.get(return: ORDERED_MAP)";

            try (RecordStream rs = session.query(key)
                .bin(binName).selectFrom(readExp)
                .execute()) {
                assertTrue(rs.hasNext());
                Record rec = rs.next().recordOrThrow();
                Map<?,?> m = rec.getMap(binName);
                assertEquals(2L, m.size());
                assertEquals(2L, m.get("b"));
                assertEquals(2L, m.get("c"));
            }
        }
    }
}
