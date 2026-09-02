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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.query.QueryBuilder;
import com.aerospike.client.sdk.util.Version;

/**
 * Exhaustive CDT builder permutation test: every map/list navigation overload on
 * {@link BinBuilder} (update), {@link QueryBinBuilder} (key query), and
 * {@link com.aerospike.client.sdk.query.QueryBuilderBinBuilder} (dataset query), at the
 * bin root and one nesting level (via {@code onMapKey} and via {@code onListIndex}).
 *
 * <p>Nested query paths return {@link CdtReadOnlyBuilder}, which carries the full navigation
 * surface; the top-level query entry builders ({@code QueryBinBuilder} and
 * {@code QueryBuilderBinBuilder}) had to grow the same overloads as {@code BinBuilder} /
 * {@code CdtReadContextBuilder} for this test to compile.</p>
 *
 * <p>The {@code client} module skips tests by default, so run this with
 * {@code mvn -pl client -DskipTests=false -Dtest=CdtBuilderPermutationTest test}.</p>
 */
public class CdtBuilderPermutationTest extends ClusterTest {

    private static final String SMAP = "cdtSMap";
    private static final String LMAP = "cdtLMap";
    private static final String BMAP = "cdtBMap";
    private static final String VMAP = "cdtVMap";
    private static final String LIST = "cdtList";
    private static final String ROOT_MAP = "cdtRootM";
    private static final String LIST_MAPS = "cdtLMaps";
    private static final String LIST_LISTS = "cdtLLists";
    private static final String FILTER = "cdtFilt";
    private static final int FILTER_VAL = 540_101;
    private static final byte[] BKEY = {1, 2, 3};
    private static final byte[] BKEY2 = {4, 5, 6};
    private static final String INNER = "str";

    private static Key key;

    @BeforeAll
    public static void seed() {
        key = args.set.id("cdt-perm-seed");
        session.delete(key).execute();

        TreeMap<String, Long> smap = new TreeMap<>();
        smap.put("a", 10L);
        smap.put("b", 20L);
        smap.put("c", 30L);
        smap.put("d", 40L);
        smap.put("e", 50L);

        TreeMap<Long, Long> lmap = new TreeMap<>();
        lmap.put(1L, 10L);
        lmap.put(2L, 20L);
        lmap.put(3L, 30L);
        lmap.put(4L, 40L);
        lmap.put(5L, 50L);

        TreeMap<byte[], Long> bmap = new TreeMap<>(Arrays::compare);
        bmap.put(BKEY, 42L);
        bmap.put(BKEY2, 99L);

        TreeMap<String, Object> vmap = new TreeMap<>();
        vmap.put("s", "hello");
        vmap.put("t", true);
        vmap.put("d", 1.5d);
        vmap.put("lst", List.of(1L, 2L));
        vmap.put("mp", Map.of("x", 1L));

        List<Long> list = List.of(10L, 20L, 30L, 40L, 50L);

        TreeMap<String, Object> root = new TreeMap<>();
        root.put("str", smap);
        root.put("long", lmap);
        root.put("blob", bmap);
        root.put("val", vmap);
        root.put("list", list);

        List<Object> listOfMaps = new ArrayList<>();
        listOfMaps.add(smap);
        List<Object> listOfLists = new ArrayList<>();
        listOfLists.add(list);

        session.upsert(key)
            .bin(FILTER).setTo(FILTER_VAL)
            .bin(SMAP).setTo(AerospikeMap.of(smap))
            .bin(LMAP).setTo(AerospikeMap.of(lmap))
            .bin(BMAP).setTo(AerospikeMap.of(MapOrder.KEY_ORDERED, bmap))
            .bin(VMAP).setTo(AerospikeMap.of(vmap))
            .bin(LIST).setTo(list)
            .bin(ROOT_MAP).setTo(AerospikeMap.of(MapOrder.KEY_ORDERED, root))
            .bin(LIST_MAPS).setTo(listOfMaps)
            .bin(LIST_LISTS).setTo(listOfLists)
            .execute();
    }

    private QueryBuilder dataset() {
        return session.query(args.set).where(Exp.eq(Exp.intBin(FILTER), Exp.val(FILTER_VAL)));
    }

    private static Record first(RecordStream rs) {
        return rs.getFirstRecord();
    }

    private static List<?> list(Record rec, String bin) {
        Object v = rec.getValue(bin);
        if (v instanceof List<?> l) {
            return l;
        }
        return List.of(v);
    }

    private static Object norm(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Double || o instanceof Float) {
            return ((Number) o).doubleValue();
        }
        if (o instanceof Number n) {
            return n.longValue();
        }
        if (o instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size());
            for (Object e : l) {
                out.add(norm(e));
            }
            return out;
        }
        if (o instanceof byte[] b) {
            return Arrays.toString(b);
        }
        return o;
    }

    private static void assertOps(String bin, Record rec, Object... expected) {
        List<?> got = list(rec, bin);
        assertEquals(expected.length, got.size(), bin + " result count");
        StringBuilder failures = new StringBuilder();
        for (int i = 0; i < expected.length; i++) {
            Object expectedValue = norm(expected[i]);
            Object actualValue = norm(got.get(i));
            if (!expectedValue.equals(actualValue)) {
                failures.append(System.lineSeparator())
                    .append(bin).append(" op ").append(i)
                    .append(": expected <").append(expectedValue)
                    .append("> but was <").append(actualValue).append('>');
            }
        }
        if (failures.length() > 0) {
            throw new AssertionError(failures.toString());
        }
    }

    /** Scalar / list results for the string-keyed map op sequence used below. */
    private static final Object[] SMAP_EXPECTED = {
        30L, // onMapKey("c")
        10L, // onMapIndex(0)
        10L, // onMapRank(0)
        List.of(30L), // onMapValue(30L)
        List.of(), // onMapValue("nope")
        List.of(), // onMapValue(byte[])
        List.of(), // onMapValue(1.5)
        List.of(), // onMapValue(false)
        List.of(), // onMapValue(List)
        List.of(), // onMapValue(Map)
        List.of(), // onMapValue(SpecialValue.NULL)
        List.of(20L, 30L), // onMapIndexRange(1, 2)
        List.of(40L, 50L), // onMapIndexRange(3)
        List.of(20L, 30L), // onMapKeyRange("b", "d")
        List.of(), // onMapKeyRange(byte[], byte[])
        List.of(), // onMapKeyRange(double, double)
        List.of(), // onMapKeyRange(long, long)
        List.of(30L, 40L, 50L), // onMapKeyRange("c", INFINITY)
        List.of(10L), // onMapKeyRange(NULL, "b")
        List.of(10L, 20L), // onMapRankRange(0, 2)
        List.of(), // onMapRankRange(3)
        List.of(20L, 30L, 40L), // onMapValueRange(15, 45)
        List.of(30L, 40L, 50L), // onMapValueRange(30, INFINITY)
        List.of(30L, 40L, 50L), // onMapKeyRelativeIndexRange("c", 0)
        List.of(40L, 50L), // onMapKeyRelativeIndexRange("c", 1)
        List.of(30L, 40L), // onMapKeyRelativeIndexRange("c", 0, 2)
        List.of(10L, 20L, 30L, 40L, 50L), // onMapKeyRelativeIndexRange(long, 0)
        List.of(), // onMapKeyRelativeIndexRange(byte[], 0)
        List.of(10L), // onMapKeyRelativeIndexRange(long, 0, 1)
        List.of(), // onMapKeyRelativeIndexRange(byte[], 0, 1)
        List.of(30L, 40L, 50L), // onMapValueRelativeRankRange(25L, 0)
        List.of(30L, 40L), // onMapValueRelativeRankRange(25L, 0, 2)
        List.of(), // string value rel
        List.of(), // byte[] value rel
        List.of(), // double value rel
        List.of(10L, 20L, 30L, 40L, 50L), // boolean value rel
        List.of(), // list value rel
        List.of(), // map value rel
        List.of(10L, 20L, 30L, 40L, 50L), // SpecialValue value rel
        List.of(), // string value rel count
        List.of(), // byte[] value rel count
        List.of(), // double value rel count
        List.of(10L), // boolean value rel count
        List.of(), // list value rel count
        List.of(), // map value rel count
        List.of(10L), // SpecialValue value rel count
        List.of(10L, 50L), // onMapKeyList a,e
        List.of(20L, 40L), // onMapValueList 20,40
        5L // mapSize
    };

    private static final Object[] LIST_EXPECTED = {
        30L, // onListIndex(2)
        30L, // onListIndex(2, order, pad)
        10L, // onListRank(0)
        List.of(30L), // onListValue(30L)
        List.of(), // onListValue(String)
        List.of(), // onListValue(byte[])
        List.of(), // onListValue(SpecialValue)
        List.of(), // onListValue(double)
        List.of(), // onListValue(boolean)
        List.of(), // onListValue(List)
        List.of(), // onListValue(Map)
        List.of(20L, 30L, 40L), // onListIndexRange(1, 3)
        List.of(40L, 50L), // onListIndexRange(3)
        List.of(20L, 10L), // onListRankRange(0, 2)
        List.of(40L, 50L), // onListRankRange(3)
        List.of(20L, 30L, 40L), // onListValueRange(15, 45)
        List.of(), // string range
        List.of(), // byte[] range
        List.of(), // double range
        List.of(30L, 40L, 50L), // onListValueRange(30, INFINITY)
        List.of(10L, 20L, 30L, 40L, 50L), // SV + String
        List.of(10L, 20L, 30L, 40L, 50L), // SV + byte[]
        List.of(10L, 20L, 30L, 40L, 50L), // SV + double
        List.of(10L, 20L, 30L, 40L, 50L), // onListValueRange(10, INFINITY)
        List.of(), // String + SV
        List.of(), // byte[] + SV
        List.of(), // double + SV
        List.of(), // boolean range
        List.of(), // List range
        List.of(), // Map range
        List.of(), // SV + boolean
        List.of(10L, 20L, 30L, 40L, 50L), // boolean + SV
        List.of(10L, 20L, 30L, 40L, 50L), // SV + List
        List.of(), // List + SV
        List.of(10L, 20L, 30L, 40L, 50L), // SV + Map
        List.of(), // Map + SV
        List.of(30L, 40L, 50L), // onListValueRelativeRankRange(25L, 0)
        List.of(30L, 40L), // count 2
        List.of(), // string rel
        List.of(), // byte[] rel
        List.of(), // double rel
        List.of(50L, 40L, 30L, 20L, 10L), // SV rel
        List.of(), // string rel count
        List.of(), // byte[] rel count
        List.of(), // double rel count
        List.of(10L), // SV rel count
        List.of(50L, 40L, 30L, 20L, 10L), // boolean rel
        List.of(), // list rel
        List.of(), // map rel
        List.of(10L), // boolean rel count
        List.of(), // list rel count
        List.of(), // map rel count
        List.of(10L, 50L), // onListValueList
        List.of(30L, 40L, 50L) // onEachChild > 25 collectValues
    };

    @Nested
    class UpdateTopLevel {
        @Test
        void mapAndListNavigations() {
            Record rec = first(session.update(key)
                .bin(SMAP).onMapKey("c").getValues()
                .bin(SMAP).onMapIndex(0).getValues()
                .bin(SMAP).onMapRank(0).getValues()
                .bin(SMAP).onMapValue(30L).getValues()
                .bin(SMAP).onMapValue("nope").getValues()
                .bin(SMAP).onMapValue(new byte[] {9}).getValues()
                .bin(SMAP).onMapValue(1.5d).getValues()
                .bin(SMAP).onMapValue(false).getValues()
                .bin(SMAP).onMapValue(List.of(1)).getValues()
                .bin(SMAP).onMapValue(Map.of("z", 1)).getValues()
                .bin(SMAP).onMapValue(SpecialValue.NULL).getValues()
                .bin(SMAP).onMapIndexRange(1, 2).getValues()
                .bin(SMAP).onMapIndexRange(3).getValues()
                .bin(SMAP).onMapKeyRange("b", "d").getValues()
                .bin(SMAP).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(SMAP).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(SMAP).onMapKeyRange(1L, 9L).getValues()
                .bin(SMAP).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(SMAP).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(SMAP).onMapRankRange(0, 2).getValues()
                .bin(SMAP).onMapRankRange(3).getValues()
                .bin(SMAP).onMapValueRange(15L, 45L).getValues()
                .bin(SMAP).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(SMAP).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(SMAP).onMapKeyList(List.of("a", "e")).getValues()
                .bin(SMAP).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(SMAP).mapSize()
                .bin(LIST).onListIndex(2).getValues()
                .bin(LIST).onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(LIST).onListRank(0).getValues()
                .bin(LIST).onListValue(30L).getValues()
                .bin(LIST).onListValue("x").getValues()
                .bin(LIST).onListValue(BKEY).getValues()
                .bin(LIST).onListValue(SpecialValue.NULL).getValues()
                .bin(LIST).onListValue(1.5d).getValues()
                .bin(LIST).onListValue(true).getValues()
                .bin(LIST).onListValue(List.of(1)).getValues()
                .bin(LIST).onListValue(Map.of("z", 1)).getValues()
                .bin(LIST).onListIndexRange(1, 3).getValues()
                .bin(LIST).onListIndexRange(3).getValues()
                .bin(LIST).onListRankRange(0, 2).getValues()
                .bin(LIST).onListRankRange(3).getValues()
                .bin(LIST).onListValueRange(15L, 45L).getValues()
                .bin(LIST).onListValueRange("a", "z").getValues()
                .bin(LIST).onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST).onListValueRange(0.5d, 1.5d).getValues()
                .bin(LIST).onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(LIST).onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(false, true).getValues()
                .bin(LIST).onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(LIST).onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(LIST).onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(LIST).onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(LIST).onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST).onListValueRelativeRankRange("x", 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(true, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST).onListValueList(List.of(10L, 50L)).getValues()
                .bin(LIST).onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .bin(LMAP).onMapKey(3L).getValues()
                .bin(BMAP).onMapKey(BKEY).getValues()
                .bin(VMAP).onMapValue("hello").getValues()
                .execute());
            assertOps(SMAP, rec, SMAP_EXPECTED);
            assertOps(LIST, rec, LIST_EXPECTED);
            assertEquals(30L, rec.getLong(LMAP));
            assertEquals(42L, rec.getLong(BMAP));
            assertEquals(List.of("hello"), list(rec, VMAP));
        }
    }

    @Nested
    class QueryTopLevel {
        @Test
        void mapAndListNavigations() {
            Record rec = first(session.query(key)
                .bin(SMAP).onMapKey("c").getValues()
                .bin(SMAP).onMapIndex(0).getValues()
                .bin(SMAP).onMapRank(0).getValues()
                .bin(SMAP).onMapValue(30L).getValues()
                .bin(SMAP).onMapValue("nope").getValues()
                .bin(SMAP).onMapValue(new byte[] {9}).getValues()
                .bin(SMAP).onMapValue(1.5d).getValues()
                .bin(SMAP).onMapValue(false).getValues()
                .bin(SMAP).onMapValue(List.of(1)).getValues()
                .bin(SMAP).onMapValue(Map.of("z", 1)).getValues()
                .bin(SMAP).onMapValue(SpecialValue.NULL).getValues()
                .bin(SMAP).onMapIndexRange(1, 2).getValues()
                .bin(SMAP).onMapIndexRange(3).getValues()
                .bin(SMAP).onMapKeyRange("b", "d").getValues()
                .bin(SMAP).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(SMAP).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(SMAP).onMapKeyRange(1L, 9L).getValues()
                .bin(SMAP).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(SMAP).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(SMAP).onMapRankRange(0, 2).getValues()
                .bin(SMAP).onMapRankRange(3).getValues()
                .bin(SMAP).onMapValueRange(15L, 45L).getValues()
                .bin(SMAP).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(SMAP).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(SMAP).onMapKeyList(List.of("a", "e")).getValues()
                .bin(SMAP).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(SMAP).mapSize()
                .bin(LIST).onListIndex(2).getValues()
                .bin(LIST).onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(LIST).onListRank(0).getValues()
                .bin(LIST).onListValue(30L).getValues()
                .bin(LIST).onListValue("x").getValues()
                .bin(LIST).onListValue(BKEY).getValues()
                .bin(LIST).onListValue(SpecialValue.NULL).getValues()
                .bin(LIST).onListValue(1.5d).getValues()
                .bin(LIST).onListValue(true).getValues()
                .bin(LIST).onListValue(List.of(1)).getValues()
                .bin(LIST).onListValue(Map.of("z", 1)).getValues()
                .bin(LIST).onListIndexRange(1, 3).getValues()
                .bin(LIST).onListIndexRange(3).getValues()
                .bin(LIST).onListRankRange(0, 2).getValues()
                .bin(LIST).onListRankRange(3).getValues()
                .bin(LIST).onListValueRange(15L, 45L).getValues()
                .bin(LIST).onListValueRange("a", "z").getValues()
                .bin(LIST).onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST).onListValueRange(0.5d, 1.5d).getValues()
                .bin(LIST).onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(LIST).onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(false, true).getValues()
                .bin(LIST).onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(LIST).onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(LIST).onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(LIST).onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(LIST).onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST).onListValueRelativeRankRange("x", 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(true, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST).onListValueList(List.of(10L, 50L)).getValues()
                .bin(LIST).onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .execute());
            assertOps(SMAP, rec, SMAP_EXPECTED);
            assertOps(LIST, rec, LIST_EXPECTED);
        }
    }

    @Nested
    class DatasetQueryTopLevel {
        @Test
        void mapAndListNavigations() {
            Assumptions.assumeTrue(
                cluster.getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_2),
                "dataset query read operations require server 8.1.2+");
            Record rec = first(dataset()
                .bin(SMAP).onMapKey("c").getValues()
                .bin(SMAP).onMapIndex(0).getValues()
                .bin(SMAP).onMapRank(0).getValues()
                .bin(SMAP).onMapValue(30L).getValues()
                .bin(SMAP).onMapValue("nope").getValues()
                .bin(SMAP).onMapValue(new byte[] {9}).getValues()
                .bin(SMAP).onMapValue(1.5d).getValues()
                .bin(SMAP).onMapValue(false).getValues()
                .bin(SMAP).onMapValue(List.of(1)).getValues()
                .bin(SMAP).onMapValue(Map.of("z", 1)).getValues()
                .bin(SMAP).onMapValue(SpecialValue.NULL).getValues()
                .bin(SMAP).onMapIndexRange(1, 2).getValues()
                .bin(SMAP).onMapIndexRange(3).getValues()
                .bin(SMAP).onMapKeyRange("b", "d").getValues()
                .bin(SMAP).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(SMAP).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(SMAP).onMapKeyRange(1L, 9L).getValues()
                .bin(SMAP).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(SMAP).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(SMAP).onMapRankRange(0, 2).getValues()
                .bin(SMAP).onMapRankRange(3).getValues()
                .bin(SMAP).onMapValueRange(15L, 45L).getValues()
                .bin(SMAP).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(SMAP).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(SMAP).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(SMAP).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(SMAP).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(SMAP).onMapKeyList(List.of("a", "e")).getValues()
                .bin(SMAP).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(SMAP).mapSize()
                .bin(LIST).onListIndex(2).getValues()
                .bin(LIST).onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(LIST).onListRank(0).getValues()
                .bin(LIST).onListValue(30L).getValues()
                .bin(LIST).onListValue("x").getValues()
                .bin(LIST).onListValue(BKEY).getValues()
                .bin(LIST).onListValue(SpecialValue.NULL).getValues()
                .bin(LIST).onListValue(1.5d).getValues()
                .bin(LIST).onListValue(true).getValues()
                .bin(LIST).onListValue(List.of(1)).getValues()
                .bin(LIST).onListValue(Map.of("z", 1)).getValues()
                .bin(LIST).onListIndexRange(1, 3).getValues()
                .bin(LIST).onListIndexRange(3).getValues()
                .bin(LIST).onListRankRange(0, 2).getValues()
                .bin(LIST).onListRankRange(3).getValues()
                .bin(LIST).onListValueRange(15L, 45L).getValues()
                .bin(LIST).onListValueRange("a", "z").getValues()
                .bin(LIST).onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST).onListValueRange(0.5d, 1.5d).getValues()
                .bin(LIST).onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(LIST).onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(false, true).getValues()
                .bin(LIST).onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(LIST).onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(LIST).onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(LIST).onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(LIST).onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(LIST).onListValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST).onListValueRelativeRankRange("x", 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(true, 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST).onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST).onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST).onListValueList(List.of(10L, 50L)).getValues()
                .bin(LIST).onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .execute());
            assertOps(SMAP, rec, SMAP_EXPECTED);
            assertOps(LIST, rec, LIST_EXPECTED);
        }
    }

    @Nested
    class UpdateOneLevelViaMap {
        @Test
        void mapAndListNavigations() {
            Record rec = first(session.update(key)
                .bin(ROOT_MAP).onMapKey(INNER).onMapKey("c").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapIndex(0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapRank(0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(30L).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue("nope").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(new byte[] {9}).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(1.5d).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(false).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(List.of(1)).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(Map.of("z", 1)).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(SpecialValue.NULL).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapIndexRange(1, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapIndexRange(3).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange("b", "d").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(1L, 9L).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapRankRange(0, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapRankRange(3).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRange(15L, 45L).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyList(List.of("a", "e")).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).mapSize()
                .bin(ROOT_MAP).onMapKey("list").onListIndex(2).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListRank(0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(30L).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue("x").getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(BKEY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(SpecialValue.NULL).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(1.5d).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(true).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(List.of(1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(Map.of("z", 1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListIndexRange(1, 3).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListIndexRange(3).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListRankRange(0, 2).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListRankRange(3).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(15L, 45L).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange("a", "z").getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(0.5d, 1.5d).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(false, true).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(25L, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange("x", 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(true, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueList(List.of(10L, 50L)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .execute());
            assertOps(ROOT_MAP, rec, concat(SMAP_EXPECTED, LIST_EXPECTED));
        }
    }

    @Nested
    class QueryOneLevelViaMap {
        @Test
        void mapAndListNavigations() {
            Record rec = first(session.query(key)
                .bin(ROOT_MAP).onMapKey(INNER).onMapKey("c").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapIndex(0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapRank(0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(30L).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue("nope").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(new byte[] {9}).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(1.5d).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(false).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(List.of(1)).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(Map.of("z", 1)).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValue(SpecialValue.NULL).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapIndexRange(1, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapIndexRange(3).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange("b", "d").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(1L, 9L).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapRankRange(0, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapRankRange(3).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRange(15L, 45L).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapKeyList(List.of("a", "e")).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(ROOT_MAP).onMapKey(INNER).mapSize()
                .bin(ROOT_MAP).onMapKey("list").onListIndex(2).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListRank(0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(30L).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue("x").getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(BKEY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(SpecialValue.NULL).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(1.5d).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(true).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(List.of(1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValue(Map.of("z", 1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListIndexRange(1, 3).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListIndexRange(3).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListRankRange(0, 2).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListRankRange(3).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(15L, 45L).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange("a", "z").getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(0.5d, 1.5d).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(false, true).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(25L, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange("x", 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(true, 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(ROOT_MAP).onMapKey("list").onListValueList(List.of(10L, 50L)).getValues()
                .bin(ROOT_MAP).onMapKey("list").onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .execute());
            assertOps(ROOT_MAP, rec, concat(SMAP_EXPECTED, LIST_EXPECTED));
        }
    }

    @Nested
    class UpdateOneLevelViaList {
        @Test
        void mapAndListNavigations() {
            Record rec = first(session.update(key)
                .bin(LIST_MAPS).onListIndex(0).onMapKey("c").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapIndex(0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapRank(0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(30L).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue("nope").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(new byte[] {9}).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(1.5d).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(false).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(List.of(1)).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(Map.of("z", 1)).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(SpecialValue.NULL).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapIndexRange(1, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapIndexRange(3).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange("b", "d").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(1L, 9L).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapRankRange(0, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapRankRange(3).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRange(15L, 45L).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyList(List.of("a", "e")).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(LIST_MAPS).onListIndex(0).mapSize()
                .bin(LIST_LISTS).onListIndex(0).onListIndex(2).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListRank(0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(30L).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue("x").getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(BKEY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(SpecialValue.NULL).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(1.5d).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(true).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(List.of(1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(Map.of("z", 1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListIndexRange(1, 3).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListIndexRange(3).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListRankRange(0, 2).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListRankRange(3).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(15L, 45L).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange("a", "z").getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(0.5d, 1.5d).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(false, true).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange("x", 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(true, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueList(List.of(10L, 50L)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .execute());
            assertOps(LIST_MAPS, rec, SMAP_EXPECTED);
            assertOps(LIST_LISTS, rec, LIST_EXPECTED);
        }
    }

    @Nested
    class QueryOneLevelViaList {
        @Test
        void mapAndListNavigations() {
            Record rec = first(session.query(key)
                .bin(LIST_MAPS).onListIndex(0).onMapKey("c").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapIndex(0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapRank(0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(30L).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue("nope").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(new byte[] {9}).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(1.5d).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(false).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(List.of(1)).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(Map.of("z", 1)).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValue(SpecialValue.NULL).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapIndexRange(1, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapIndexRange(3).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange("b", "d").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(0.5d, 1.5d).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(1L, 9L).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange("c", SpecialValue.INFINITY).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRange(SpecialValue.NULL, "b").getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapRankRange(0, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapRankRange(3).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRange(15L, 45L).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange("c", 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange("c", 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange("c", 0, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(3L, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(BKEY, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(3L, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyRelativeIndexRange(BKEY, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange("x", 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(true, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapKeyList(List.of("a", "e")).getValues()
                .bin(LIST_MAPS).onListIndex(0).onMapValueList(List.of(20L, 40L)).getValues()
                .bin(LIST_MAPS).onListIndex(0).mapSize()
                .bin(LIST_LISTS).onListIndex(0).onListIndex(2).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListIndex(2, ListOrder.UNORDERED, false).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListRank(0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(30L).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue("x").getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(BKEY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(SpecialValue.NULL).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(1.5d).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(true).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(List.of(1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValue(Map.of("z", 1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListIndexRange(1, 3).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListIndexRange(3).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListRankRange(0, 2).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListRankRange(3).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(15L, 45L).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange("a", "z").getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(new byte[] {0}, new byte[] {1}).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(0.5d, 1.5d).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(30L, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, "z").getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, BKEY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, 1.5d).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(10L, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange("a", SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(BKEY, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(0.5d, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(false, true).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(List.of(1), List.of(2)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(Map.of("a", 1), Map.of("b", 2)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, false).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(false, SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, List.of(1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(List.of(1), SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(SpecialValue.NULL, Map.of("a", 1)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRange(Map.of("a", 1), SpecialValue.INFINITY).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(25L, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(25L, 0, 2).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange("x", 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(BKEY, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(1.5d, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(SpecialValue.NULL, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange("x", 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(BKEY, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(1.5d, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(SpecialValue.NULL, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(true, 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(List.of(1), 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(Map.of("z", 1), 0).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(true, 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(List.of(1), 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueRelativeRankRange(Map.of("z", 1), 0, 1).getValues()
                .bin(LIST_LISTS).onListIndex(0).onListValueList(List.of(10L, 50L)).getValues()
                .bin(LIST_LISTS).onListIndex(0).onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(25))).collectValues()
                .execute());
            assertOps(LIST_MAPS, rec, SMAP_EXPECTED);
            assertOps(LIST_LISTS, rec, LIST_EXPECTED);
        }
    }

    private static Object[] concat(Object[] a, Object[] b) {
        Object[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
