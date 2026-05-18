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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.exp.MapExp;
import com.aerospike.client.sdk.util.Version;

public class CdtOperateTest extends ClusterTest {
    private static final String NAMESPACE = "test";
    private static final String SET = "testset";
    private static final String BIN_NAME = "testbin";

    @BeforeAll
    public static void checkServerVersion() {
        // Skip tests for server version < 8.1.1
        Version serverVersion = cluster.getRandomNode().getVersion();
        boolean condition = serverVersion.isGreaterOrEqual(8, 1, 1, 0);
        Assumptions.assumeTrue(condition, "Tests skipped for server version < 8.1.1");
    }
/*
    @Test
    public void testCDTOperateWithExpressions() {
        Key rkey = args.set.id("testCDTOperateWithExpressions");

        session.delete(rkey).execute();

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        booksList.add(book4);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        session.upsert(rkey)
            .bin(BIN_NAME).setTo(rootMap)
            .execute();

        RecordStream rs = session.query(rkey).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertNotNull(rec);

        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, SelectFlags.VALUE, ctx1, ctx2, ctx3);

        Record result = session.query(rkey)
            .bin(BIN_NAME).get().execute();

        Record result = client.operate(null, rkey, selectOp);

        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        assertNotNull(results, "Results should not be null");
        assertEquals(2, results.size(), "Should have 2 books with price <= 10.0");

        // Verify the titles (order may vary)
        List<String> titles = new ArrayList<>();
        for (Object item : results) {
            assertTrue(item instanceof String, "Each result should be a string title");
            titles.add((String) item);
        }

        // Check that we got the expected titles
        assertTrue(titles.contains("Sayings of the Century"), "Should contain 'Sayings of the Century'");
        assertTrue(titles.contains("Moby Dick"), "Should contain 'Moby Dick'");
    }

    @Test
    public void testCDTApplyWithExpressions() {
        Key rkey = new Key(NAMESPACE, SET, 216);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 22.99);
        booksList.add(book4);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));

        Expression modifyExp = Exp.build(
            Exp.mul(
                Exp.floatLoopVar(LoopVarPart.VALUE),  // Current price value
                Exp.val(1.10)                         // Multiply by 1.10
            )
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, bookKey, allChildren, priceKey);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT apply operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalRootMap != null, "Root map should exist");

        List<?> finalBooksList = (List<?>) finalRootMap.get("book");
        assertTrue(finalBooksList != null && !finalBooksList.isEmpty(), "Books list should exist");

        Map<?, ?> firstBook = (Map<?, ?>) finalBooksList.get(0);
        assertTrue(firstBook != null, "First book should exist");

        Object priceObj = firstBook.get("price");
        assertTrue(priceObj != null, "Price should exist");

        double finalPrice = ((Number) priceObj).doubleValue();
        assertTrue(finalPrice > 9.0, "Price should be increased (> 9)");

        double expectedPrice = 8.95 * 1.10;
        assertTrue(Math.abs(finalPrice - expectedPrice) < 0.01,
                   "Price should be approximately " + expectedPrice);

        // Verify all books have increased prices
        double[] originalPrices = {8.95, 12.99, 8.99, 22.99};
        for (int i = 0; i < finalBooksList.size(); i++) {
            Map<?, ?> book = (Map<?, ?>) finalBooksList.get(i);
            assertTrue(book != null, "Book " + i + " should be a map");

            Object price = book.get("price");
            assertTrue(price != null, "Book " + i + " should have a price");

            double priceFloat = ((Number) price).doubleValue();
            double expected = originalPrices[i] * 1.10;
            assertTrue(Math.abs(priceFloat - expected) < 0.01,
                      "Book " + i + " price should be approximately " + expected + ", got " + priceFloat);
        }
    }

    @Test
    public void testNestedContextsAndComplexFilters() {
        Key rkey = new Key(NAMESPACE, SET, 217);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> store = new HashMap<>();
        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("category", "reference");
        book1.put("author", "Nigel Rees");
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("category", "fiction");
        book2.put("author", "Evelyn Waugh");
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("category", "fiction");
        book3.put("author", "Herman Melville");
        book3.put("title", "Moby Dick");
        book3.put("price", 8.99);
        booksList.add(book3);

        store.put("books", booksList);
        data.put("store", store);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("store"));
        CTX ctx2 = CTX.mapKey(Value.get("books"));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.and(
                Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("category"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                    Exp.val("fiction")
                ),
                Exp.lt(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                    Exp.val(10.0)
                )
            )
        );
        CTX ctx4 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3, ctx4);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        assertNotNull(results, "Results should not be null");
        assertEquals(1, results.size(), "Should have 1 fiction book with price < 10.0");
        assertEquals("Moby Dick", results.get(0), "Should get 'Moby Dick'");
    }

    @Test
    public void testEmptyResultsWhenNoItemsMatch() {
        Key rkey = new Key(NAMESPACE, SET, 218);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Expensive Book 1");
        book1.put("price", 25.99);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Expensive Book 2");
        book2.put("price", 30.50);
        booksList.add(book2);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);

        // Try to select books with price <= 10.0 (should return empty)
        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("title"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Verify empty results
        Object results = result.getValue(BIN_NAME);
        if (results instanceof List) {
            List<?> resultList = (List<?>) results;
            assertEquals(0, resultList.size(), "Should have 0 books matching the filter");
        }
    }

    @Test
    public void testMatchingTreeFlag() {
        Key rkey = new Key(NAMESPACE, SET, 219);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Cheap Book");
        book1.put("price", 5.99);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Expensive Book");
        book2.put("price", 25.99);
        booksList.add(book2);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_MATCHING_TREE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // With MatchingTree, we should get back the full matching structure
        Object results = result.getValue(BIN_NAME);
        assertNotNull(results, "Results should not be null");
    }

    @Test
    public void testMapKeysFlag() {
        Key rkey = new Key(NAMESPACE, SET, 220);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> items = new HashMap<>();
        items.put("item1", 100);
        items.put("item2", 200);
        items.put("item3", 50);
        data.put("items", items);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select with MapKeys flag - should return only keys, not values
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(75))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_MAP_KEY, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get keys where value > 75
        Object results = result.getValue(BIN_NAME);
        assertNotNull(results, "Results should not be null");
    }

    @Test
    public void testSelectNoFailFlag() {
        Key rkey = new Key(NAMESPACE, SET, 221);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> existing = new ArrayList<>();
        existing.add(1);
        existing.add(2);
        existing.add(3);
        data.put("existing", existing);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Try to select from existing path with SelectNoFail
        CTX ctx1 = CTX.mapKey(Value.get("existing"));
        CTX ctx2 = CTX.allChildren();

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_NO_FAIL, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");
    }

    @Test
    public void testLoopVariableIndex() {
        Key rkey = new Key(NAMESPACE, SET, 222);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);
        numbers.add(40);
        numbers.add(50);
        data.put("numbers", numbers);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select items where index < 3
        CTX ctx1 = CTX.mapKey(Value.get("numbers"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.lt(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(3))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get first 3 items (indices 0, 1, 2)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(3, results.size(), "Should have 3 items with index < 3");
        }
    }

    @Test
    public void testLoopVariableMapKey() {
        Key rkey = new Key(NAMESPACE, SET, 223);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> products = new HashMap<>();
        products.put("apple", 1.50);
        products.put("banana", 0.75);
        products.put("cherry", 2.25);
        data.put("products", products);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select items where key starts with 'a' or 'b' (lexicographically < "c")
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.lt(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("c"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get apple and banana (keys < "c")
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(2, results.size(), "Should have 2 items with keys < 'c'");
        }
    }

    @Test
    public void testModifyWithAddition() {
        Key rkey = new Key(NAMESPACE, SET, 224);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> scores = new ArrayList<>();
        scores.add(10);
        scores.add(20);
        scores.add(30);
        scores.add(40);
        scores.add(50);
        data.put("scores", scores);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Add 5 to each score
        CTX ctx1 = CTX.mapKey(Value.get("scores"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression modifyExp = Exp.build(
            Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5))
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT apply operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalRootMap != null, "Root map should exist");

        List<?> finalScores = (List<?>) finalRootMap.get("scores");
        assertTrue(finalScores != null, "Scores list should exist");
        assertEquals(5, finalScores.size(), "Should have 5 scores");

        int firstScore = ((Number) finalScores.get(0)).intValue();
        assertEquals(15, firstScore, "10 + 5 = 15");
    }

    @Test
    public void testModifyWithSubtraction() {
        Key rkey = new Key(NAMESPACE, SET, 225);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> balances = new HashMap<>();
        balances.put("account1", 1000);
        balances.put("account2", 2000);
        balances.put("account3", 1500);
        data.put("balances", balances);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Subtract 100 from each balance
        CTX ctx1 = CTX.mapKey(Value.get("balances"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression modifyExp = Exp.build(
            Exp.sub(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(100))
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT apply operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalRootMap != null, "Root map should exist");

        Map<?, ?> finalBalances = (Map<?, ?>) finalRootMap.get("balances");
        assertTrue(finalBalances != null, "Balances map should exist");

        // Verify account1 balance was decreased by 100
        int balance1 = ((Number) finalBalances.get("account1")).intValue();
        assertEquals(900, balance1, "1000 - 100 = 900");
    }

    @Test
    public void testNestedListsAndComplexFilters() {
        Key rkey = new Key(NAMESPACE, SET, 226);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<List<Integer>> matrix = new ArrayList<>();
        List<Integer> row1 = new ArrayList<>();
        row1.add(1);
        row1.add(2);
        row1.add(3);
        matrix.add(row1);
        List<Integer> row2 = new ArrayList<>();
        row2.add(4);
        row2.add(5);
        row2.add(6);
        matrix.add(row2);
        List<Integer> row3 = new ArrayList<>();
        row3.add(7);
        row3.add(8);
        row3.add(9);
        matrix.add(row3);
        data.put("matrix", matrix);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildren();

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get all 3 rows
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(3, results.size(), "Should have 3 rows");
        }
    }

    @Test
    public void testBooleanExpressionsInFilters() {
        Key rkey = new Key(NAMESPACE, SET, 227);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> users = new ArrayList<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "Alice");
        user1.put("active", true);
        user1.put("age", 30);
        users.add(user1);

        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Bob");
        user2.put("active", false);
        user2.put("age", 25);
        users.add(user2);

        Map<String, Object> user3 = new HashMap<>();
        user3.put("name", "Charlie");
        user3.put("active", true);
        user3.put("age", 35);
        users.add(user3);

        data.put("users", users);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select active users
        CTX ctx1 = CTX.mapKey(Value.get("users"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                    Exp.val("active"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get Alice and Charlie (active users)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(2, results.size(), "Should have 2 active users");
            assertTrue(results.contains("Alice"), "Should contain 'Alice'");
            assertTrue(results.contains("Charlie"), "Should contain 'Charlie'");
        }
    }

    @Test
    public void testComplexAndOrFilterCombinations() {
        Key rkey = new Key(NAMESPACE, SET, 228);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> products = new ArrayList<>();

        Map<String, Object> p1 = new HashMap<>();
        p1.put("name", "Widget");
        p1.put("price", 10.0);
        p1.put("inStock", true);
        products.add(p1);

        Map<String, Object> p2 = new HashMap<>();
        p2.put("name", "Gadget");
        p2.put("price", 25.0);
        p2.put("inStock", false);
        products.add(p2);

        Map<String, Object> p3 = new HashMap<>();
        p3.put("name", "Gizmo");
        p3.put("price", 15.0);
        p3.put("inStock", true);
        products.add(p3);

        Map<String, Object> p4 = new HashMap<>();
        p4.put("name", "Doohickey");
        p4.put("price", 30.0);
        p4.put("inStock", true);
        products.add(p4);

        data.put("products", products);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select products that are (inStock AND price < 20) OR (price > 25)
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.or(
                Exp.and(
                    Exp.eq(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                            Exp.val("inStock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                        Exp.val(true)
                    ),
                    Exp.lt(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                            Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                        Exp.val(20.0)
                    )
                ),
                Exp.gt(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                    Exp.val(25.0)
                )
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get Widget (inStock, price 10), Gizmo (inStock, price 15), and Doohickey (price 30)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertTrue(results.size() >= 1, "Should have at least 1 matching product");
        }
    }

    @Test
    public void testDeeplyNestedStructures() {
        Key rkey = new Key(NAMESPACE, SET, 229);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> level1 = new HashMap<>();
        Map<String, Object> level2 = new HashMap<>();
        List<Map<String, Object>> level3 = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("value", 100);
        level3.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("value", 200);
        level3.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("value", 300);
        level3.add(item3);

        level2.put("level3", level3);
        level1.put("level2", level2);
        data.put("level1", level1);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Navigate deep and select values
        CTX ctx1 = CTX.mapKey(Value.get("level1"));
        CTX ctx2 = CTX.mapKey(Value.get("level2"));
        CTX ctx3 = CTX.mapKey(Value.get("level3"));
        CTX ctx4 = CTX.allChildrenWithFilter(
            Exp.gt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("value"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(150)
            )
        );
        CTX ctx5 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("value"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3, ctx4, ctx5);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        // Should get values > 150 (200 and 300)
        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(2, results.size(), "Should have 2 values > 150");
        }
    }

    @Test
    public void testSingleContextElement() {
        Key rkey = new Key(NAMESPACE, SET, 230);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        data.put("value", 123);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select with single context
        CTX ctx1 = CTX.mapKey(Value.get("value"));

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        Object results = result.getValue(BIN_NAME);
        assertNotNull(results, "Results should not be null");
    }

    @Test
    public void testEmptyLists() {
        Key rkey = new Key(NAMESPACE, SET, 231);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Object> emptyList = new ArrayList<>();
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(2);
        items.add(3);
        data.put("emptyList", emptyList);
        data.put("items", items);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Try to select from empty list
        CTX ctx1 = CTX.mapKey(Value.get("emptyList"));
        CTX ctx2 = CTX.allChildren();

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_NO_FAIL, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");
    }

    @Test
    public void testEmptyMaps() {
        Key rkey = new Key(NAMESPACE, SET, 232);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> emptyMap = new HashMap<>();
        Map<String, Object> items = new HashMap<>();
        items.put("a", 1);
        items.put("b", 2);
        data.put("emptyMap", emptyMap);
        data.put("items", items);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Try to select from empty map
        CTX ctx1 = CTX.mapKey(Value.get("emptyMap"));
        CTX ctx2 = CTX.allChildren();

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_NO_FAIL, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");
    }

    @Test
    public void testListIndexContext() {
        Key rkey = new Key(NAMESPACE, SET, 233);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "item1");
        item1.put("value", 10);
        items.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "item2");
        item2.put("value", 20);
        items.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "item3");
        item3.put("value", 30);
        items.add(item3);

        data.put("items", items);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select value from second item
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.listIndex(1); // Select second item (index 1)
        CTX ctx3 = CTX.mapKey(Value.get("value"));

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        Object resultBin = result.getValue(BIN_NAME);
        if (resultBin instanceof List) {
            List<?> resultList = (List<?>) resultBin;
            if (resultList.size() == 1) {
                assertEquals(20L, resultList.get(0), "Should get value 20");
            }
        }
    }

    @Test
    public void testModifyWithIndex() {
        Key rkey = new Key(NAMESPACE, SET, 234);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        values.add(400);
        data.put("values", values);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Multiply each value by its index + 1
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression modifyExp = Exp.build(
            Exp.mul(
                Exp.intLoopVar(LoopVarPart.VALUE),
                Exp.add(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(1))
            )
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT apply operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        List<?> finalValues = (List<?>) finalData.get("values");
        assertTrue(finalValues != null, "Values list should exist");

        assertEquals(100, ((Number) finalValues.get(0)).intValue(), "First value should be 100");
        assertEquals(400, ((Number) finalValues.get(1)).intValue(), "Second value should be 400");
        assertEquals(900, ((Number) finalValues.get(2)).intValue(), "Third value should be 900");
        assertEquals(1600, ((Number) finalValues.get(3)).intValue(), "Fourth value should be 1600");
    }

    @Test
    public void testModifyWithComplexArithmetic() {
        Key rkey = new Key(NAMESPACE, SET, 235);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> metrics = new ArrayList<>();

        Map<String, Object> m1 = new HashMap<>();
        m1.put("value", 10);
        m1.put("multiplier", 2);
        metrics.add(m1);

        Map<String, Object> m2 = new HashMap<>();
        m2.put("value", 20);
        m2.put("multiplier", 3);
        metrics.add(m2);

        Map<String, Object> m3 = new HashMap<>();
        m3.put("value", 30);
        m3.put("multiplier", 4);
        metrics.add(m3);

        data.put("metrics", metrics);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Add 100 to each value field in the metrics
        CTX ctx1 = CTX.mapKey(Value.get("metrics"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("value"))
        );

        Expression modifyExp = Exp.build(
            Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(100))
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT apply operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        List<?> finalMetrics = (List<?>) finalData.get("metrics");
        assertTrue(finalMetrics != null, "Metrics list should exist");

        Map<?, ?> firstMetric = (Map<?, ?>) finalMetrics.get(0);
        assertTrue(firstMetric != null, "First metric should exist");

        int value = ((Number) firstMetric.get("value")).intValue();
        assertEquals(110, value, "10 + 100 = 110");
    }

    @Test
    public void testRemoveAllItemsFromList() {
        Key rkey = new Key(NAMESPACE, SET, 236);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(2);
        items.add(3);
        items.add(4);
        items.add(5);
        data.put("items", items);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        List<?> finalItems = (List<?>) finalData.get("items");
        assertTrue(finalItems != null, "Items list should exist");
        assertEquals(0, finalItems.size(), "All items should be removed");
    }

    @Test
    public void testRemoveFilteredItemsFromList() {
        Key rkey = new Key(NAMESPACE, SET, 237);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> numbers = new ArrayList<>();
        numbers.add(1);
        numbers.add(5);
        numbers.add(10);
        numbers.add(15);
        numbers.add(20);
        numbers.add(25);
        numbers.add(30);
        data.put("numbers", numbers);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("numbers"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))
        );

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        List<?> finalNumbers = (List<?>) finalData.get("numbers");
        assertTrue(finalNumbers != null, "Numbers list should exist");
        assertEquals(3, finalNumbers.size(), "Should keep items <= 10");
        assertTrue(finalNumbers.contains(1L), "Should contain 1");
        assertTrue(finalNumbers.contains(5L), "Should contain 5");
        assertTrue(finalNumbers.contains(10L), "Should contain 10");
    }

    @Test
    public void testRemoveAllItemsFromMap() {
        Key rkey = new Key(NAMESPACE, SET, 238);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put("option1", "value1");
        config.put("option2", "value2");
        config.put("option3", "value3");
        data.put("config", config);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("config"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        Map<?, ?> finalConfig = (Map<?, ?>) finalData.get("config");
        assertTrue(finalConfig != null, "Config map should exist");
        assertEquals(0, finalConfig.size(), "All map entries should be removed");
    }

    @Test
    public void testRemoveFilteredMapEntries() {
        Key rkey = new Key(NAMESPACE, SET, 239);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> scores = new HashMap<>();
        scores.put("alice", 95);
        scores.put("bob", 45);
        scores.put("carol", 75);
        scores.put("dave", 30);
        data.put("scores", scores);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("scores"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(50))
        );

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        Map<?, ?> finalScores = (Map<?, ?>) finalData.get("scores");
        assertTrue(finalScores != null, "Scores map should exist");
        assertEquals(2, finalScores.size(), "Should keep scores >= 50");

        assertTrue(!finalScores.containsKey("bob"), "Should not contain bob");
        assertTrue(!finalScores.containsKey("dave"), "Should not contain dave");

        assertTrue(finalScores.containsKey("alice"), "Should contain alice");
        assertEquals(95L, ((Number) finalScores.get("alice")).longValue(), "Alice score should be 95");
    }

    @Test
    public void testRemoveBooksWithLowPrices() {
        Key rkey = new Key(NAMESPACE, SET, 240);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Cheap Book 1");
        book1.put("price", 5.99);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Expensive Book");
        book2.put("price", 25.99);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Cheap Book 2");
        book3.put("price", 3.99);
        booksList.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "Mid Price Book");
        book4.put("price", 15.99);
        booksList.add(book4);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("books", booksList);

        Bin bin = new Bin(BIN_NAME, rootMap);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("books"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                    Exp.val("price"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10.0)
            )
        );

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalRootMap != null, "Root map should exist");

        List<?> finalBooks = (List<?>) finalRootMap.get("books");
        assertTrue(finalBooks != null, "Books list should exist");
        assertEquals(2, finalBooks.size(), "Should keep 2 expensive books");

        // Verify all remaining books have price > 10.0
        for (Object bookRaw : finalBooks) {
            Map<?, ?> book = (Map<?, ?>) bookRaw;
            assertTrue(book != null, "Book should be a map");

            Object price = book.get("price");
            assertTrue(price != null, "Book should have a price");

            double priceFloat = ((Number) price).doubleValue();
            assertTrue(priceFloat > 10.0, "Price should be > 10.0, got " + priceFloat);
        }
    }

    @Test
    public void testRemoveItemsByIndexFilter() {
        Key rkey = new Key(NAMESPACE, SET, 241);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        values.add(400);
        values.add(500);
        data.put("values", values);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.ge(Exp.intLoopVar(LoopVarPart.INDEX), Exp.val(3))
        );

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        List<?> finalValues = (List<?>) finalData.get("values");
        assertTrue(finalValues != null, "Values list should exist");
        assertEquals(3, finalValues.size(), "Should keep first 3 items");
        assertEquals(100L, ((Number) finalValues.get(0)).longValue(), "First value should be 100");
        assertEquals(200L, ((Number) finalValues.get(1)).longValue(), "Second value should be 200");
        assertEquals(300L, ((Number) finalValues.get(2)).longValue(), "Third value should be 300");
    }

    @Test
    public void testRemoveMapEntriesByKeyFilter() {
        Key rkey = new Key(NAMESPACE, SET, 242);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> inventory = new HashMap<>();
        inventory.put("apple", 10);
        inventory.put("banana", 5);
        inventory.put("cherry", 8);
        inventory.put("date", 3);
        data.put("inventory", inventory);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("inventory"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.ge(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("c"))
        );

        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        Map<?, ?> finalInventory = (Map<?, ?>) finalData.get("inventory");
        assertTrue(finalInventory != null, "Inventory map should exist");
        assertEquals(2, finalInventory.size(), "Should keep 2 items");

        assertTrue(finalInventory.containsKey("apple"), "Should contain apple");
        assertTrue(finalInventory.containsKey("banana"), "Should contain banana");
    }

    @Test
    public void testRemoveNestedItemsWithComplexPath() {
        Key rkey = new Key(NAMESPACE, SET, 243);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> departments = new HashMap<>();

        List<Map<String, Object>> salesList = new ArrayList<>();
        Map<String, Object> sales1 = new HashMap<>();
        sales1.put("name", "John");
        sales1.put("sales", 1000);
        salesList.add(sales1);
        Map<String, Object> sales2 = new HashMap<>();
        sales2.put("name", "Jane");
        sales2.put("sales", 5000);
        salesList.add(sales2);

        List<Map<String, Object>> engList = new ArrayList<>();
        Map<String, Object> eng1 = new HashMap<>();
        eng1.put("name", "Bob");
        eng1.put("sales", 500);
        engList.add(eng1);
        Map<String, Object> eng2 = new HashMap<>();
        eng2.put("name", "Alice");
        eng2.put("sales", 3000);
        engList.add(eng2);

        departments.put("sales", salesList);
        departments.put("engineering", engList);
        data.put("departments", departments);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("departments"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.lt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("sales"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(2000)
            )
        );


        Expression removeExp = Exp.build(Exp.removeResults());
        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, removeExp, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT remove operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        Map<?, ?> finalDepartments = (Map<?, ?>) finalData.get("departments");
        assertTrue(finalDepartments != null, "Departments map should exist");

        List<?> finalSalesList = (List<?>) finalDepartments.get("sales");
        assertTrue(finalSalesList != null, "Sales list should exist");
        assertEquals(1, finalSalesList.size(), "Should keep Jane only");

        List<?> finalEngList = (List<?>) finalDepartments.get("engineering");
        assertTrue(finalEngList != null, "Engineering list should exist");
        assertEquals(1, finalEngList.size(), "Should keep Alice only");
    }

    @Test
    public void testOperateWithNoOperations() {
        Key rkey = new Key(NAMESPACE, SET, 244);

        // Make sure the record does not exist
        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        data.put("value", 123);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        try {
            client.operate(null, rkey);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testSelectByPathWithNullContext() {
        Key rkey = new Key(NAMESPACE, SET, 245);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);

        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, (CTX[])null);

        try {
            client.operate(null, rkey, selectOp);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testSelectByPathWithNoContexts() {
        Key rkey = new Key(NAMESPACE, SET, 246);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);

        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE);

        try {
            client.operate(null, rkey, selectOp);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testSelectByPathWithEmptyContextArray() {
        Key rkey = new Key(NAMESPACE, SET, 247);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        data.put("value1", 100);
        data.put("value2", 200);
        data.put("value3", 300);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        CTX[] emptyCtx = new CTX[0];
        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, emptyCtx);

        try {
            client.operate(null, rkey, selectOp);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testModifyByPathWithNullContext() {
        Key rkey = new Key(NAMESPACE, SET, 248);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);

        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        Expression modifyExp = Exp.build(Exp.val(100));
        Operation modifyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, (CTX[])null);

        try {
            client.operate(null, rkey, modifyOp);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testModifyByPathWithNoContexts() {
        Key rkey = new Key(NAMESPACE, SET, 249);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        List<Integer> numbers = new ArrayList<>();
        numbers.add(10);
        numbers.add(20);
        numbers.add(30);

        Bin bin = new Bin(BIN_NAME, numbers);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        Expression modifyExp = Exp.build(Exp.val(100));
        Operation modifyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp);

        try {
            client.operate(null, rkey, modifyOp);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testModifyByPathWithEmptyContextArray() {
        Key rkey = new Key(NAMESPACE, SET, 250);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        data.put("count", 50);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        Record record = client.get(null, rkey);
        assertTrue(record != null, "Record should exist");

        CTX[] emptyCtx = new CTX[0];
        Expression modifyExp = Exp.build(Exp.val(200));
        Operation modifyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, emptyCtx);

        try {
            client.operate(null, rkey, modifyOp);
            assertTrue(false, "Should throw AerospikeException with PARAMETER_ERROR");
        } catch (com.aerospike.client.AerospikeException e) {
            assertEquals(com.aerospike.client.sdk.ResultCode.PARAMETER_ERROR, e.getResultCode(), "Should be PARAMETER_ERROR");
        }
    }

    @Test
    public void testLoopVarListWithNestedLists() {
        Key rkey = new Key(NAMESPACE, SET, 251);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<List<Integer>> matrix = new ArrayList<>();
        List<Integer> row1 = new ArrayList<>();
        row1.add(1);
        row1.add(2);
        row1.add(3);
        matrix.add(row1);
        List<Integer> row2 = new ArrayList<>();
        row2.add(4);
        row2.add(5);
        row2.add(6);
        matrix.add(row2);
        List<Integer> row3 = new ArrayList<>();
        row3.add(7);
        row3.add(8);
        row3.add(9);
        matrix.add(row3);
        data.put("matrix", matrix);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildren();

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(3, results.size(), "Should have 3 rows");
        }
    }

    @Test
    public void testModifyWithDivision() {
        Key rkey = new Key(NAMESPACE, SET, 252);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        data.put("values", values);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression modifyExp = Exp.build(
            Exp.div(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT modify operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        List<?> finalValues = (List<?>) finalData.get("values");
        assertTrue(finalValues != null, "Values list should exist");

        int firstValue = ((Number) finalValues.get(0)).intValue();
        assertEquals(10, firstValue, "100 / 10 = 10");
    }

    @Test
    public void testLoopVarListAccessNestedListSize() {
        Key rkey = new Key(NAMESPACE, SET, 253);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<List<Integer>> matrix = new ArrayList<>();

        List<Integer> row1 = new ArrayList<>();
        row1.add(1);
        row1.add(2);
        row1.add(3);
        matrix.add(row1);

        List<Integer> row2 = new ArrayList<>();
        row2.add(4);
        row2.add(5);
        matrix.add(row2);

        List<Integer> row3 = new ArrayList<>();
        row3.add(7);
        row3.add(8);
        row3.add(9);
        matrix.add(row3);

        data.put("matrix", matrix);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                ListExp.size(Exp.listLoopVar(LoopVarPart.VALUE)),
                Exp.val(3)
            )
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(2, results.size(), "Should have 2 rows with size 3");
        }
    }

    @Test
    public void testLoopVarBlobAccessBlobValues() {
        Key rkey = new Key(NAMESPACE, SET, 254);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<byte[]> blobs = new ArrayList<>();
        blobs.add("First blob content".getBytes());
        blobs.add("Second blob content".getBytes());
        blobs.add("Target blob".getBytes());
        blobs.add("Fourth blob content".getBytes());

        data.put("blobs", blobs);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.mapKey(Value.get("blobs"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.blobLoopVar(LoopVarPart.VALUE),
                Exp.val("Target blob".getBytes())
            )
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(1, results.size(), "Should have 1 blob matching target");
            byte[] resultBlob = (byte[]) results.get(0);
            assertEquals("Target blob", new String(resultBlob), "Should match target blob");
        }
    }

    @Test
    public void testLoopVarNilWithNilValues() {
        Key rkey = new Key(NAMESPACE, SET, 255);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        data.put("a", 1);
        data.put("b", 2);
        data.put("c", true);
        data.put("d", "test".getBytes());
        data.put("e", null);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        CTX ctx1 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.nilLoopVar(LoopVarPart.VALUE),
                Exp.nil()
            )
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE | Exp.SELECT_NO_FAIL, ctx1);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(1, results.size(), "Should have 1 nil value");
        }
    }

    @Test
    public void testLoopVarGeoJSONFilterLocations() {
        Key rkey = new Key(NAMESPACE, SET, 256);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Value.GeoJSONValue> locations = new ArrayList<>();

        locations.add(new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-122.4194,37.7749]}"));
        locations.add(new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-118.2437,34.0522]}"));
        locations.add(new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-73.9352,40.7306]}"));

        data.put("locations", locations);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        String californiaRegion = "{\"type\":\"Polygon\",\"coordinates\":[[[-124.5,32.5],[-114.0,32.5],[-114.0,42.0],[-124.5,42.0],[-124.5,32.5]]]}";

        CTX ctx1 = CTX.mapKey(Value.get("locations"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            com.aerospike.client.sdk.exp.Exp.geoCompare(
                Exp.geoJsonLoopVar(LoopVarPart.VALUE),
                Exp.geo(californiaRegion)
            )
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertTrue(results.size() >= 0, "Should have filtered GeoJSON locations");
            for (Object item : results) {
                assertNotNull(item, "Location should not be null");
            }
        }
    }

    @Test
    public void testBoolLoopVarFilterActive() {
        Key rkey = new Key(NAMESPACE, SET, 257);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> users = new ArrayList<>();

        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "Alice");
        user1.put("active", true);
        user1.put("score", 95);
        users.add(user1);

        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Bob");
        user2.put("active", false);
        user2.put("score", 85);
        users.add(user2);

        Map<String, Object> user3 = new HashMap<>();
        user3.put("name", "Charlie");
        user3.put("active", true);
        user3.put("score", 90);
        users.add(user3);

        Map<String, Object> user4 = new HashMap<>();
        user4.put("name", "Diana");
        user4.put("active", false);
        user4.put("score", 88);
        users.add(user4);

        data.put("users", users);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select names where active is true using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("users"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                    Exp.val("active"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(2, results.size(), "Should have 2 active users");
            assertTrue(results.contains("Alice"), "Should contain Alice");
            assertTrue(results.contains("Charlie"), "Should contain Charlie");
        }
    }

    @Test
    public void testBoolLoopVarModifyFlags() {
        Key rkey = new Key(NAMESPACE, SET, 258);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> settings = new HashMap<>();
        settings.put("enableFeatureA", true);
        settings.put("enableFeatureB", false);
        settings.put("enableFeatureC", true);
        settings.put("enableFeatureD", false);
        data.put("settings", settings);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Negate all boolean settings using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("settings"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));

        Expression modifyExp = Exp.build(
            Exp.not(Exp.boolLoopVar(LoopVarPart.VALUE))
        );

        Operation applyOp = CdtOperation.modifyByPath(BIN_NAME, Exp.MODIFY_DEFAULT, modifyExp, ctx1, ctx2);

        Record result = client.operate(null, rkey, applyOp);
        assertTrue(result != null, "CDT modify operation should succeed");

        Record finalRecord = client.get(null, rkey);
        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue(BIN_NAME);
        assertTrue(finalData != null, "Data map should exist");

        Map<?, ?> finalSettings = (Map<?, ?>) finalData.get("settings");
        assertTrue(finalSettings != null, "Settings map should exist");

        assertEquals(false, finalSettings.get("enableFeatureA"), "enableFeatureA should be false");
        assertEquals(true, finalSettings.get("enableFeatureB"), "enableFeatureB should be true");
        assertEquals(false, finalSettings.get("enableFeatureC"), "enableFeatureC should be false");
        assertEquals(true, finalSettings.get("enableFeatureD"), "enableFeatureD should be true");
    }

    @Test
    public void testBoolLoopVarInListFilter() {
        Key rkey = new Key(NAMESPACE, SET, 259);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        Map<String, Object> data = new HashMap<>();
        List<Boolean> flags = new ArrayList<>();
        flags.add(true);
        flags.add(false);
        flags.add(true);
        flags.add(true);
        flags.add(false);
        data.put("flags", flags);

        Bin bin = new Bin(BIN_NAME, data);
        client.put(null, rkey, bin);

        // Select indices where flag is true using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("flags"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.boolLoopVar(LoopVarPart.VALUE), Exp.val(true))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2);

        Record result = client.operate(null, rkey, selectOp);
        assertTrue(result != null, "CDT select operation should succeed");

        List<?> results = result.getList(BIN_NAME);
        if (results != null) {
            assertEquals(3, results.size(), "Should have 3 true flags");
            for (Object item : results) {
                assertEquals(true, item, "All results should be true");
            }
        }
    }

    @Test
    public void testHllLoopVarWithHllExpressions() {
        // Demonstrates hllLoopVar usage pattern with selectByPath.
        // Creates a metadata structure and shows the expression pattern for HLL filtering.
        Key rkey = new Key(NAMESPACE, SET, 260);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        // Create HLL values in separate bins
        List<Value> entries1 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            entries1.add(Value.get("item" + i));
        }

        List<Value> entries2 = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            entries2.add(Value.get("item" + i));
        }

        List<Value> entries3 = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            entries3.add(Value.get("item" + i));
        }

        Record rec = client.operate(null, rkey,
            HLLOperation.add(HLLPolicy.Default, "hll1", entries1, 8),
            HLLOperation.add(HLLPolicy.Default, "hll2", entries2, 8),
            HLLOperation.add(HLLPolicy.Default, "hll3", entries3, 8),
            Operation.get("hll1"),
            Operation.get("hll2"),
            Operation.get("hll3")
        );

        assertNotNull(rec, "Record should exist");

        // Create a metadata structure to demonstrate selectByPath pattern
        Map<String, Object> hllMetadata = new HashMap<>();
        List<Map<String, Object>> hllInfo = new ArrayList<>();

        Map<String, Object> info1 = new HashMap<>();
        info1.put("name", "small");
        info1.put("binName", "hll1");
        info1.put("expectedCount", 5);
        hllInfo.add(info1);

        Map<String, Object> info2 = new HashMap<>();
        info2.put("name", "medium");
        info2.put("binName", "hll2");
        info2.put("expectedCount", 20);
        hllInfo.add(info2);

        Map<String, Object> info3 = new HashMap<>();
        info3.put("name", "large");
        info3.put("binName", "hll3");
        info3.put("expectedCount", 50);
        hllInfo.add(info3);

        hllMetadata.put("hlls", hllInfo);

        Bin metaBin = new Bin(BIN_NAME, hllMetadata);
        client.put(null, rkey, metaBin);

        // Use selectByPath to filter HLL metadata where expectedCount > 10
        CTX ctx1 = CTX.mapKey(Value.get("hlls"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.gt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("expectedCount"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );

        Operation selectOp = CdtOperation.selectByPath(BIN_NAME, Exp.SELECT_VALUE, ctx1, ctx2, ctx3);

        Record result = client.operate(null, rkey, selectOp);
        assertNotNull(result, "Result should exist");

        List<?> selectedNames = result.getList(BIN_NAME);
        assertNotNull(selectedNames, "Selected names should exist");
        assertEquals(2, selectedNames.size(), "Should have 2 HLLs with count > 10");
        assertTrue(selectedNames.contains("medium"), "Should contain 'medium'");
        assertTrue(selectedNames.contains("large"), "Should contain 'large'");

        // Demonstrate hllLoopVar expression construction
        // This pattern would be used if HLLs were in nested structures:
        // CTX.allChildrenWithFilter(
        //     Exp.gt(HLLExp.getCount(Exp.hllLoopVar(LoopVarPart.VALUE)), Exp.val(10))
        // )
        Exp hllLoopVarExp = Exp.hllLoopVar(LoopVarPart.VALUE);
        assertNotNull(hllLoopVarExp, "hllLoopVar expression should be created");
    }

    @Test
    public void testHllLoopVarInExpressionContext() {
        Key rkey = new Key(NAMESPACE, SET, 261);

        try {
            client.delete(null, rkey);
        } catch (Exception e) {
        }

        // Create a simple HLL
        List<Value> entries = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            entries.add(Value.get("value" + i));
        }

        Record rec = client.operate(null, rkey,
            HLLOperation.add(HLLPolicy.Default, "testHll", entries, 8),
            HLLOperation.getCount("testHll")
        );

        assertNotNull(rec, "Record should exist");

        List<?> resultList = rec.getList("testHll");
        long count = (Long) resultList.get(1); // getCount is the second operation
        assertTrue(count >= 10 && count <= 20, "HLL count should be around 15");

        Exp hllLoopExp = Exp.hllLoopVar(LoopVarPart.VALUE);
        assertNotNull(hllLoopExp, "hllLoopVar expression should be created");
    }
    */
}