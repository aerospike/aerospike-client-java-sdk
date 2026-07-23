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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.exp.MapExp;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.util.Version;

/**
 * Demonstrates fluent CDT path helpers: {@code onEachChild}, {@code collectValues},
 * {@code modifyBy}, {@code removeMatches}, and {@code collectValuesAsExpressionRead}.
 *
 * <p>These features require Aerospike Server 8.1.1 or later ({@code selectByPath} /
 * {@code modifyByPath}). If the cluster is older, this example is skipped.</p>
 */
public class CdtPathExpressionExample extends Example {

    @Override
    public void runExample() throws Exception {
        Cluster cluster = cluster();
        Version version = cluster.getRandomNode().getVersion();
        if (!version.isGreaterOrEqual(8, 1, 1, 0)) {
            throw new ExampleSkipException(
                "server is " + version + "; CDT path expressions require 8.1.1+");
        }

        Session session = cluster.createSession(Behavior.DEFAULT);
        DataSet set = dataSet("cdt-path-demo");

        boolean allOk = true;

        console.info("--- 1) Bin-root list: onEachChild + modifyBy (increment each element) ---");
        List<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(2);
        nums.add(3);
        session.upsert(set.id(1)).bin("nums").setTo(nums).execute();
        console.info("initial nums (before +10 to each): " + nums);

        session.upsert(set.id(1))
                .bin("nums").onEachChild()
                .modifyBy(Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)))
                .execute();

        Record rec = session.query(set.id(1)).bin("nums").get().execute().getFirst().orElseThrow().recordOrThrow();
        console.info("nums after +10 to each: " + rec.bins.get("nums"));
        allOk &= reportCheck(console, 1, intListEquals(rec.bins.get("nums"), List.of(11, 12, 13)));

        console.info("--- 2) Bin-root list: onEachChild(filter) + removeMatches ---");
        List<Integer> nums2 = new ArrayList<>();
        nums2.add(3);
        nums2.add(7);
        nums2.add(2);
        nums2.add(9);
        session.upsert(set.id(2)).bin("nums").setTo(nums2).execute();
        console.info("initial nums (before removeMatches where value > 5): " + nums2);

        session.upsert(set.id(2))
                .bin("nums").onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5)))
                .removeMatches()
                .execute();

        rec = session.query(set.id(2)).bin("nums").get().execute().getFirst().orElseThrow().recordOrThrow();
        console.info("nums after removing values > 5: " + rec.bins.get("nums"));
        allOk &= reportCheck(console, 2, intListEquals(rec.bins.get("nums"), List.of(3, 2)));

        console.info("--- 3) Nested map/list: titles of books with price <= 10 (collectValues) ---");
        Map<String, Object> root = buildBookCatalog();
        session.upsert(set.id(3)).bin("catalog").setTo(root).execute();
        console.info("initial catalog (before collectValues for cheap-book titles): " + root);

        RecordStream rs = session.query(set.id(3))
                .bin("catalog")
                .onMapKey("book")
                .onEachChild(Exp.le(
                        MapExp.getByKey(
                                MapReturnType.VALUE,
                                Exp.Type.FLOAT,
                                Exp.val("price"),
                                Exp.mapLoopVar(LoopVarPart.VALUE)),
                        Exp.val(10.0)))
                .onEachChild(Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title")))
                .collectValues()
                .execute();

        rec = rs.getFirst().orElseThrow().recordOrThrow();
        console.info("collectValues (price <= 10, title leaves) returned in projection bin 'catalog': "
                + rec.bins.get("catalog"));
        Set<String> cheapTitles = Set.of("Sayings of the Century", "Moby Dick");
        allOk &= reportCheck(console, 3, stringListMatchesUnordered(rec.bins.get("catalog"), cheapTitles));

        console.info("--- 4) Nested map/list: multiply every book price by 1.10 (modifyBy) ---");
        Map<String, Object> catalogBeforeBump = buildBookCatalog();
        session.upsert(set.id(4)).bin("catalog").setTo(catalogBeforeBump).execute();
        console.info("initial catalog (before 1.10x on each price): " + catalogBeforeBump);

        session.upsert(set.id(4))
                .bin("catalog")
                .onMapKey("book")
                .onEachChild()
                .onMapKey("price")
                .modifyBy(Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(1.10)))
                .execute();

        rec = session.query(set.id(4)).bin("catalog").get().execute().getFirst().orElseThrow().recordOrThrow();
        console.info("catalog after 10% price bump: " + rec.bins.get("catalog"));
        double[] originalPrices = { 8.95, 12.99, 8.99, 22.99 };
        allOk &= reportCheck(console, 4, catalogBookPricesMatch(rec.bins.get("catalog"), originalPrices, 1.10, 0.02));

        console.info("--- 5) Expression read: collectValuesAsExpressionRead into a projection bin ---");
        Map<String, Object> catalogForRead = buildBookCatalog();
        session.upsert(set.id(5)).bin("catalog").setTo(catalogForRead).execute();
        console.info("initial catalog (before expression read of all titles): " + catalogForRead);

        rs = session.query(set.id(5))
                .bin("catalog")
                .onMapKey("book")
                .onEachChild()
                .onMapKey("title")
                .collectValuesAsExpressionRead(Exp.Type.MAP, Exp.Type.LIST)
                .execute();

        rec = rs.getFirst().orElseThrow().recordOrThrow();
        console.info("collectValuesAsExpressionRead (all titles) in bin 'catalog': " + rec.bins.get("catalog"));
        Set<String> allTitles = Set.of(
                "Sayings of the Century",
                "Sword of Honour",
                "Moby Dick",
                "The Lord of the Rings");
        allOk &= reportCheck(console, 5, stringListMatchesUnordered(rec.bins.get("catalog"), allTitles));

        if (!allOk) {
            throw new AssertionError("One or more CDT path expression checks failed");
        }
        console.info("Overall: SUCCESS");
    }

    private static boolean reportCheck(Console console, int step, boolean ok) {
        console.info("Step " + step + ": " + (ok ? "SUCCESS" : "*** FAILURE ***"));
        return ok;
    }

    /**
     * Compare a bin value (list of numbers) to expected integers, in order.
     */
    private static boolean intListEquals(Object binValue, List<Integer> expected) {
        if (!(binValue instanceof List<?> list)) {
            return false;
        }
        if (list.size() != expected.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            if (!(list.get(i) instanceof Number)) {
                return false;
            }
            long got = ((Number) list.get(i)).longValue();
            if (got != expected.get(i).longValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare a bin value (list of strings) to an expected set, ignoring order and duplicates.
     */
    private static boolean stringListMatchesUnordered(Object binValue, Set<String> expected) {
        if (!(binValue instanceof List<?> list)) {
            return false;
        }
        Set<String> got = new HashSet<>();
        for (Object o : list) {
            if (!(o instanceof String)) {
                return false;
            }
            got.add((String) o);
        }
        return got.equals(expected);
    }

    /**
     * Verify {@code book} list entries have prices {@code originalPrices[i] * factor} within {@code epsilon}.
     */
    private static boolean catalogBookPricesMatch(
            Object binValue, double[] originalPrices, double factor, double epsilon) {
        if (!(binValue instanceof Map<?, ?> root)) {
            return false;
        }
        Object booksObj = root.get("book");
        if (!(booksObj instanceof List<?> books)) {
            return false;
        }
        if (books.size() != originalPrices.length) {
            return false;
        }
        for (int i = 0; i < books.size(); i++) {
            Object bookObj = books.get(i);
            if (!(bookObj instanceof Map<?, ?> book)) {
                return false;
            }
            Object priceObj = book.get("price");
            if (!(priceObj instanceof Number)) {
                return false;
            }
            double got = ((Number) priceObj).doubleValue();
            double want = originalPrices[i] * factor;
            if (Math.abs(got - want) > epsilon) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, Object> buildBookCatalog() {
        List<Map<String, Object>> books = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        books.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        books.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        books.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        books.add(book4);

        Map<String, Object> root = new HashMap<>();
        root.put("book", books);
        return root;
    }
}
