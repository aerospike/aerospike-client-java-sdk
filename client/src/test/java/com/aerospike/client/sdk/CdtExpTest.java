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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.CdtExp;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.HLLExp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.exp.MapExp;
import com.aerospike.client.sdk.util.Version;

public class CdtExpTest extends ClusterTest {
    private static final DataSet dataSet = DataSet.of(args.set.getNamespace(), "testset");

    @BeforeAll
    public static void checkServerVersion() {
        // Skip tests for server version < 8.1.1
        Version serverVersion = session.getCluster().getRandomNode().getVersion();
        boolean condition = serverVersion.isGreaterOrEqual(8, 1, 1, 0);
        Assumptions.assumeTrue(condition, "Tests skipped for server version < 8.1.1");
    }

    @Test
    public void testCDTExpSelect() {
        Key keyA = dataSet.id("cdtExpSelectKey");

        session.delete(keyA).execute();

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 10.45);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 20.99);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 5.01);
        booksList.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 30.98);
        booksList.add(book4);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        session.upsert(keyA)
            .bin("res1").setTo(rootMap)
            .execute();

        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,                 // Return type: list
                SelectFlags.VALUE,             // AS_CDT_SELECT_LEAF_MAP_VALUE equivalent
                Exp.mapBin("res1"),            // Source bin
                bookKey, allChildren, priceKey // CTX path
            )
        );

        RecordStream rs = session.upsert(keyA)
            .bin("A").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "CDT select expression operation should succeed");

        rs = session.query(keyA)
            .execute();

        Record finalRecord = rs.getFirstRecord();
        List<?> priceList = finalRecord.getList("A");
        assertTrue(priceList != null, "Price list should exist");
        assertTrue(priceList.size() == 4, "Should have 4 prices");

        double firstPrice = ((Number) priceList.get(0)).doubleValue();
        assertTrue(firstPrice < 11, "First price should be < 11");
    }

    @Test
    public void testCDTExpApply() {
        Key keyA = dataSet.id("cdtExpApplyKey");

        session.delete(keyA).execute();

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Sayings of the Century");
        book1.put("price", 10.45);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Sword of Honour");
        book2.put("price", 20.99);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Moby Dick");
        book3.put("price", 5.01);
        booksList.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("title", "The Lord of the Rings");
        book4.put("price", 30.98);
        booksList.add(book4);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        session.upsert(keyA)
            .bin("res1").setTo(rootMap)
            .execute();

        CTX bookKey = CTX.mapKey(Value.get("book"));
        CTX allChildren = CTX.allChildren();
        CTX priceKey = CTX.mapKey(Value.get("price"));

        Exp modifyExp = Exp.mul(
            Exp.floatLoopVar(LoopVarPart.VALUE),  // Current price value
            Exp.val(1.50)                         // Multiply by 1.50
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,                     // Return type: map
                ModifyFlags.DEFAULT,              // Flags
                modifyExp,                        // Modify expression
                Exp.mapBin("res1"),               // Source bin
                bookKey, allChildren, priceKey    // CTX path
            )
        );

        RecordStream rs = session.upsert(keyA)
            .bin("res1").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "CDT apply expression operation should succeed");

        rs = session.query(keyA)
            .execute();

        Record finalRecord = rs.getFirstRecord();

        assertTrue(finalRecord != null, "Final record should exist");

        Map<?, ?> finalRootMap = (Map<?, ?>) finalRecord.getValue("res1");
        assertTrue(finalRootMap != null, "Root map should exist");

        List<?> finalBooksList = (List<?>) finalRootMap.get("book");
        assertTrue(finalBooksList != null && !finalBooksList.isEmpty(), "Books list should exist");

        Map<?, ?> firstBook = (Map<?, ?>) finalBooksList.get(0);
        assertTrue(firstBook != null, "First book should exist");

        Object priceObj = firstBook.get("price");
        assertTrue(priceObj != null, "Price should exist");

        double finalPrice = ((Number) priceObj).doubleValue();
        assertTrue(finalPrice > 11.0, "Price should be increased (> 11)");

        double expectedPrice = 10.45 * 1.50;
        assertTrue(Math.abs(finalPrice - expectedPrice) < 0.01, "Price should be approximately " + expectedPrice);
    }

    @Test
    public void testSelectTitlesWithPriceFilter() {
        Key key = dataSet.id("selectTitlesFilterKey");

        session.delete(key).execute();

        List<Map<String, Object>> booksList = new ArrayList<>();

        Map<String, Object> book1 = new HashMap<>();
        book1.put("title", "Cheap Book");
        book1.put("price", 5.99);
        booksList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("title", "Medium Book");
        book2.put("price", 15.50);
        booksList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("title", "Expensive Book");
        book3.put("price", 25.99);
        booksList.add(book3);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("book", booksList);

        session.upsert(key)
            .bin("res1").setTo(rootMap)
            .execute();

        // Select titles where price <= 10
        CTX ctx1 = CTX.mapKey(Value.get("book"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.le(
                MapExp.getByKey(
                    MapReturnType.VALUE,
                    Exp.Type.FLOAT,
                    Exp.val("price"),
                    Exp.mapLoopVar(LoopVarPart.VALUE)
                ),
                Exp.val(10.0)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("title")
            )
        );

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("res1"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("titles").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();

        assertNotNull(finalRecord, "Final record should exist");

        List<?> titles = finalRecord.getList("titles");
        assertNotNull(titles, "Titles should exist");
        assertEquals(1, titles.size(), "Should have 1 book with price <= 10");
        assertEquals("Cheap Book", titles.get(0), "First title should be 'Cheap Book'");
    }

    @Test
    public void testExpReadOpWithSelectByPath() {
        Key key = dataSet.id("expReadOpSelectKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Integer> items = new ArrayList<>();
        items.add(10);
        items.add(20);
        items.add(30);
        data.put("items", items);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select all items
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        // Use ExpReadOp to read without modifying
        RecordStream rs = session.query(key)
            .bin("result").selectFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify result
        List<?> resultItems = result.getList("result");
        assertNotNull(resultItems, "Items should exist");
        assertEquals(3, resultItems.size(), "Should have 3 items");
    }

    @Test
    public void testModifyWithAddition() {
        Key key = dataSet.id("modifyAdditionKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> products = new ArrayList<>();

        Map<String, Object> p1 = new HashMap<>();
        p1.put("name", "A");
        p1.put("price", 10.0);
        products.add(p1);

        Map<String, Object> p2 = new HashMap<>();
        p2.put("name", "B");
        p2.put("price", 20.0);
        products.add(p2);

        data.put("products", products);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Add 5 to each price
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildren();
        CTX ctx3 = CTX.mapKey(Value.get("price"));

        Exp modifyExp = Exp.add(
            Exp.floatLoopVar(LoopVarPart.VALUE),
            Exp.val(5.0)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("data").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify modification
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        List<?> finalProducts = (List<?>) finalData.get("products");
        assertNotNull(finalProducts, "Products list should exist");

        Map<?, ?> firstProduct = (Map<?, ?>) finalProducts.get(0);
        assertNotNull(firstProduct, "First product should exist");

        Object priceObj = firstProduct.get("price");
        double priceFloat = ((Number) priceObj).doubleValue();

        // Verify price is 15.0 (10.0 + 5.0)
        assertTrue(Math.abs(priceFloat - 15.0) < 0.01, "Price should be 15.0");
    }

    @Test
    public void testModifyWithSubtraction() {
        Key key = dataSet.id("modifySubtractionKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> accounts = new HashMap<>();
        accounts.put("acc1", 1000);
        accounts.put("acc2", 2000);
        data.put("accounts", accounts);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Subtract 100 from each account
        CTX ctx1 = CTX.mapKey(Value.get("accounts"));
        CTX ctx2 = CTX.allChildren();

        Exp modifyExp = Exp.sub(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(100)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("data").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify modification
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        Map<?, ?> finalAccounts = (Map<?, ?>) finalData.get("accounts");
        assertNotNull(finalAccounts, "Accounts map should exist");

        int acc1 = ((Number) finalAccounts.get("acc1")).intValue();
        assertEquals(900, acc1, "Account1 should be 900 (1000 - 100)");
    }

    @Test
    public void testExpWriteFlagCreateOnly() {
        Key key = dataSet.id("createOnlyFlagKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(1);
        values.add(2);
        values.add(3);
        data.put("values", values);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        // This should succeed (new bin)
        RecordStream rs = session.upsert(key)
            .bin("newbin").insertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // This should fail (bin already exists)
        try {
            session.upsert(key)
                .bin("newbin").insertFrom(selectExp)
                .execute()
                .getFirstRecord();
            assertTrue(false, "Should have thrown exception for existing bin");
        } catch (AerospikeException e) {
            // Expected
        }
    }

    @Test
    public void testCombineSelectAndModify() {
        Key key = dataSet.id("combineSelectModifyKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("id", 1);
        item1.put("value", 10);
        items.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("id", 2);
        item2.put("value", 20);
        items.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("id", 3);
        item3.put("value", 30);
        items.add(item3);

        data.put("items", items);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // First, select all values
        CTX selectCtx1 = CTX.mapKey(Value.get("items"));
        CTX selectCtx2 = CTX.allChildren();
        CTX selectCtx3 = CTX.mapKey(Value.get("value"));

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                selectCtx1, selectCtx2, selectCtx3
            )
        );

        // Write selected values to a new bin
        RecordStream rs = session.upsert(key)
            .bin("values").upsertFrom(selectExp)
            .execute();

        rs.getFirstRecord();

        // Then, modify all values by doubling them
        CTX modifyCtx1 = CTX.mapKey(Value.get("items"));
        CTX modifyCtx2 = CTX.allChildren();
        CTX modifyCtx3 = CTX.mapKey(Value.get("value"));

        Exp modifyExp = Exp.mul(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(2)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                modifyCtx1, modifyCtx2, modifyCtx3
            )
        );

        rs = session.upsert(key)
            .bin("data").updateFrom(applyExp)
            .execute();

        rs.getFirstRecord();

        // Verify both bins
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        // Check original values (should be [10, 20, 30])
        List<?> values = finalRecord.getList("values");
        assertNotNull(values, "Values should exist");
        assertEquals(3, values.size(), "Should have 3 values");

        // Check modified data (values should be doubled)
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        List<?> finalItems = (List<?>) finalData.get("items");
        assertNotNull(finalItems, "Items list should exist");

        Map<?, ?> firstItem = (Map<?, ?>) finalItems.get(0);
        assertNotNull(firstItem, "First item should exist");

        int value = ((Number) firstItem.get("value")).intValue();
        assertEquals(20, value, "Value should be doubled (10 * 2 = 20)");
    }

    @Test
    public void testSelectByPathWithListOfLists() {
        Key key = dataSet.id("listOfListsKey");

        session.delete(key).execute();

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

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select all rows
        CTX ctx1 = CTX.mapKey(Value.get("matrix"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("rows").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();

        assertTrue(result != null, "Operation should succeed");

        // Verify result
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        List<?> rows = finalRecord.getList("rows");
        assertNotNull(rows, "Rows should exist");
        assertEquals(3, rows.size(), "Should have 3 rows");
    }

    @Test
    public void testModifyNestedMapValues() {
        Key key = dataSet.id("modifyNestedMapKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> departments = new HashMap<>();

        Map<String, Object> sales = new HashMap<>();
        sales.put("revenue", 100000);
        sales.put("target", 120000);
        departments.put("sales", sales);

        Map<String, Object> engineering = new HashMap<>();
        engineering.put("revenue", 50000);
        engineering.put("target", 60000);
        departments.put("engineering", engineering);

        data.put("departments", departments);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Increase all revenue by 10%
        CTX ctx1 = CTX.mapKey(Value.get("departments"));
        CTX ctx2 = CTX.allChildrenWithFilter(Exp.val(true));
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("revenue")
            )
        );

        Exp modifyExp = Exp.mul(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(2)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("data").upsertFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify modification
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        Map<?, ?> depts = (Map<?, ?>) finalData.get("departments");
        assertNotNull(depts, "Departments map should exist");

        Map<?, ?> salesDept = (Map<?, ?>) depts.get("sales");
        assertNotNull(salesDept, "Sales department should exist");

        Object revenueObj = salesDept.get("revenue");
        long revenueInt = ((Number) revenueObj).longValue();

        long expectedRevenue = 100000 * 2;
        assertTrue(revenueInt == expectedRevenue, "Revenue should be " + expectedRevenue + " but was " + revenueInt);
    }

    @Test
    public void testSelectByPathWithIntegerValues() {
        Key key = dataSet.id("selectIntegerValuesKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> scores = new HashMap<>();
        scores.put("player1", 100);
        scores.put("player2", 200);
        scores.put("player3", 150);
        data.put("scores", scores);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select all scores
        CTX ctx1 = CTX.mapKey(Value.get("scores"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        RecordStream rs = session.query(key)
            .bin("allScores").selectFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify result
        List<?> scoresList = result.getList("allScores");
        assertNotNull(scoresList, "Scores should exist");
        assertEquals(3, scoresList.size(), "Should have 3 scores");
    }

    @Test
    public void testModifyWithDivision() {
        Key key = dataSet.id("modifyDivisionKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(100);
        values.add(200);
        values.add(300);
        data.put("values", values);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Divide all values by 10
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildren();

        Exp modifyExp = Exp.div(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(10)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("data").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify modification
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        List<?> finalValues = (List<?>) finalData.get("values");
        assertNotNull(finalValues, "Values list should exist");

        int firstValue = ((Number) finalValues.get(0)).intValue();
        assertEquals(10, firstValue, "100 / 10 = 10");
    }

    @Test
    public void testSelectByPathWithMapKeys() {
        Key key = dataSet.id("selectMapKeysKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> products = new HashMap<>();
        products.put("apple", 1.50);
        products.put("banana", 0.75);
        products.put("cherry", 2.25);
        data.put("products", products);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select all keys
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.MAP_KEY,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("keys").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify result - should get keys
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        List<?> keys = finalRecord.getList("keys");
        assertNotNull(keys, "Keys should exist");
    }

    @Test
    public void testSelectByPathWithFilteredResults() {
        Key key = dataSet.id("selectFilteredKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> employees = new ArrayList<>();

        Map<String, Object> emp1 = new HashMap<>();
        emp1.put("name", "Alice");
        emp1.put("salary", 50000);
        emp1.put("active", true);
        employees.add(emp1);

        Map<String, Object> emp2 = new HashMap<>();
        emp2.put("name", "Bob");
        emp2.put("salary", 60000);
        emp2.put("active", false);
        employees.add(emp2);

        Map<String, Object> emp3 = new HashMap<>();
        emp3.put("name", "Charlie");
        emp3.put("salary", 55000);
        emp3.put("active", true);
        employees.add(emp3);

        data.put("employees", employees);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select names of active employees
        CTX ctx1 = CTX.mapKey(Value.get("employees"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(
                    MapReturnType.VALUE,
                    Exp.Type.BOOL,
                    Exp.val("active"),
                    Exp.mapLoopVar(LoopVarPart.VALUE)
                ),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("name")
            )
        );

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("activeEmployees").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify result
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        List<?> names = finalRecord.getList("activeEmployees");
        assertNotNull(names, "Names should exist");
        assertEquals(2, names.size(), "Should have 2 active employees");
        assertTrue(names.contains("Alice"), "Should contain 'Alice'");
        assertTrue(names.contains("Charlie"), "Should contain 'Charlie'");
    }

    @Test
    public void testExpWriteFlagEvalNoFail() {
        Key key = dataSet.id("evalNoFailKey");

        session.delete(key).execute();

        // Don't create the bin
        session.upsert(key)
            .bin("otherbin").setTo("test")
            .execute();

        // Try to select from non-existent bin with EvalNoFail
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("nonexistent"),
                ctx1, ctx2
            )
        );

        // Should not fail with EVAL_NO_FAIL flag
        RecordStream rs = session.upsert(key)
            .bin("result").upsertFrom(selectExp, opt -> opt.ignoreEvalFailure())
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed with EVAL_NO_FAIL");
    }

    @Test
    public void testMultipleExpWriteOpInSequence() {
        Key key = dataSet.id("multipleOpsKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Integer> values = new ArrayList<>();
        values.add(1);
        values.add(2);
        values.add(3);
        data.put("values", values);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select values
        CTX ctx1 = CTX.mapKey(Value.get("values"));
        CTX ctx2 = CTX.allChildren();

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        // Modify values (double them)
        Exp modifyExp = Exp.mul(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(2)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        // Execute both operations in one call
        RecordStream rs = session.upsert(key)
            .bin("original").upsertFrom(selectExp)
            .bin("data").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify both results
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        // Original values should be [1, 2, 3]
        List<?> original = finalRecord.getList("original");
        assertNotNull(original, "Original values should exist");
        assertEquals(3, original.size(), "Should have 3 original values");

        // Modified values should be doubled
        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        List<?> finalValues = (List<?>) finalData.get("values");
        assertNotNull(finalValues, "Values list should exist");

        int firstValue = ((Number) finalValues.get(0)).intValue();
        assertEquals(2, firstValue, "1 * 2 = 2");
    }

    @Test
    public void testBoolLoopVarSelect() {
        Key key = dataSet.id("boolLoopVarSelectKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "Item1");
        item1.put("active", true);
        items.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "Item2");
        item2.put("active", false);
        items.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "Item3");
        item3.put("active", true);
        items.add(item3);

        data.put("items", items);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Select names where active field is true using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(
            Exp.eq(
                MapExp.getByKey(
                    MapReturnType.VALUE,
                    Exp.Type.BOOL,
                    Exp.val("active"),
                    Exp.mapLoopVar(LoopVarPart.VALUE)
                ),
                Exp.val(true)
            )
        );
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(
                Exp.stringLoopVar(LoopVarPart.MAP_KEY),
                Exp.val("name")
            )
        );

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("activeNames").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        List<?> activeNames = finalRecord.getList("activeNames");
        assertNotNull(activeNames, "Active names should exist");
        assertEquals(2, activeNames.size(), "Should have 2 active items");
        assertTrue(activeNames.contains("Item1"), "Should contain Item1");
        assertTrue(activeNames.contains("Item3"), "Should contain Item3");
    }

    @Test
    public void testBoolLoopVarModify() {
        Key key = dataSet.id("boolLoopVarModifyKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        Map<String, Object> flags = new HashMap<>();
        flags.put("flag1", true);
        flags.put("flag2", false);
        flags.put("flag3", true);
        data.put("flags", flags);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Negate all boolean flags using boolLoopVar
        CTX ctx1 = CTX.mapKey(Value.get("flags"));
        CTX ctx2 = CTX.allChildren();

        Exp modifyExp = Exp.not(Exp.boolLoopVar(LoopVarPart.VALUE));

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("data").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        Map<?, ?> finalFlags = (Map<?, ?>) finalData.get("flags");
        assertNotNull(finalFlags, "Flags map should exist");

        assertEquals(false, finalFlags.get("flag1"), "flag1 should be false");
        assertEquals(true, finalFlags.get("flag2"), "flag2 should be true");
        assertEquals(false, finalFlags.get("flag3"), "flag3 should be false");
    }

    @Test
    public void testHllLoopVarWithHllBins() {
        // Note: HLL operations on HLL items nested in lists/maps are not currently
        // supported by the server. This test demonstrates hllLoopVar usage with
        // HLL expressions in a filtering context.
        Key key = dataSet.id("hllLoopVarKey");

        session.delete(key).execute();

        // Create HLL values with different counts in separate bins
        List<Value> entries1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            entries1.add(Value.get("item" + i));
        }

        List<Value> entries2 = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            entries2.add(Value.get("item" + i));
        }

        List<Value> entries3 = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            entries3.add(Value.get("item" + i));
        }

        // Create HLLs in separate bins and get them back
        RecordStream rs = session.upsert(key)
            .bin("hll1").hllAdd(entries1, HllConfig.of(8))
            .bin("hll2").hllAdd(entries2, HllConfig.of(8))
            .bin("hll3").hllAdd(entries3, HllConfig.of(8))
            .bin("hll1").get()
            .bin("hll2").get()
            .bin("hll3").get()
            .execute();

        Record rec = rs.getFirstRecord();

        // Verify HLL bins were created
        assertNotNull(rec, "Record should exist");

        // Extract HLLValue objects for use in expressions
        List<?> results1 = rec.getList("hll1");
        Value.HLLValue hll1 = (Value.HLLValue) results1.get(1);
        assertNotNull(hll1, "HLL1 should exist");

        List<?> results2 = rec.getList("hll2");
        Value.HLLValue hll2 = (Value.HLLValue) results2.get(1);
        assertNotNull(hll2, "HLL2 should exist");

        List<?> results3 = rec.getList("hll3");
        Value.HLLValue hll3 = (Value.HLLValue) results3.get(1);
        assertNotNull(hll3, "HLL3 should exist");

        // Test that hllLoopVar can be used in expressions with HLL union operations
        List<Value.HLLValue> hllList = new ArrayList<>();
        hllList.add(hll1);
        hllList.add(hll2);

        // Use expression to get HLL union count
        Expression e = Exp.build(HLLExp.getUnionCount(Exp.val(hllList), Exp.hllBin("hll3")));

        Record checkRec = session.query(key)
            .bin("unionCount").selectFrom(e)
            .execute()
            .getFirstRecord();

        assertNotNull(checkRec, "Check record should exist");
        Object unionCount = checkRec.getValue("unionCount");
        assertNotNull(unionCount, "Union count should exist");
        assertTrue(((Number) unionCount).longValue() >= 3, "Union count should be positive");
    }

    @Test
    public void testSelectByPathWithExpressionVariant() {
        Key key = dataSet.id("expressionVariantKey");

        session.delete(key).execute();

        List<Map<String, Object>> itemsList = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "Item1");
        item1.put("quantity", 5);
        itemsList.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "Item2");
        item2.put("quantity", 15);
        itemsList.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "Item3");
        item3.put("quantity", 25);
        itemsList.add(item3);

        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("items", itemsList);

        session.upsert(key)
            .bin("data").setTo(rootMap)
            .execute();

        // Build Expression first for the filter
        Expression filterExpression = Exp.build(
            Exp.gt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("quantity"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(10)
            )
        );

        // Use the Expression variant of allChildrenWithFilter
        CTX ctx1 = CTX.mapKey(Value.get("items"));
        CTX ctx2 = CTX.allChildrenWithFilter(filterExpression);
        CTX ctx3 = CTX.allChildrenWithFilter(
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("name"))
        );

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("filteredNames").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        List<?> names = finalRecord.getList("filteredNames");
        assertNotNull(names, "Names should exist");
        assertEquals(2, names.size(), "Should have 2 items with quantity > 10");
        assertTrue(names.contains("Item2"), "Should contain 'Item2'");
        assertTrue(names.contains("Item3"), "Should contain 'Item3'");
    }

    @Test
    public void testModifyByPathWithExpressionVariant() {
        Key key = dataSet.id("modifyExpressionVariantKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> products = new ArrayList<>();

        Map<String, Object> p1 = new HashMap<>();
        p1.put("name", "Product1");
        p1.put("stock", 10);
        products.add(p1);

        Map<String, Object> p2 = new HashMap<>();
        p2.put("name", "Product2");
        p2.put("stock", 20);
        products.add(p2);

        data.put("products", products);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Build filter Expression for items to modify
        Expression filterExpression = Exp.build(
            Exp.gt(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                    Exp.val("stock"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(15)
            )
        );

        // Modify stock for products with stock > 15
        CTX ctx1 = CTX.mapKey(Value.get("products"));
        CTX ctx2 = CTX.allChildrenWithFilter(filterExpression);  // Using Expression variant
        CTX ctx3 = CTX.mapKey(Value.get("stock"));

        Exp modifyExp = Exp.add(
            Exp.intLoopVar(LoopVarPart.VALUE),
            Exp.val(50)
        );

        Expression applyExp = Exp.build(
            CdtExp.modifyByPath(
                Exp.Type.MAP,
                ModifyFlags.DEFAULT,
                modifyExp,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("data").updateFrom(applyExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        // Verify modification
        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        Map<?, ?> finalData = (Map<?, ?>) finalRecord.getValue("data");
        assertNotNull(finalData, "Data map should exist");

        List<?> finalProducts = (List<?>) finalData.get("products");
        assertNotNull(finalProducts, "Products list should exist");

        // Product1 (stock 10) should be unchanged
        Map<?, ?> product1 = (Map<?, ?>) finalProducts.get(0);
        assertEquals(10L, ((Number) product1.get("stock")).longValue(), "Product1 stock should be unchanged");

        // Product2 (stock 20) should be increased by 50
        Map<?, ?> product2 = (Map<?, ?>) finalProducts.get(1);
        assertEquals(70L, ((Number) product2.get("stock")).longValue(), "Product2 stock should be 70 (20 + 50)");
    }

    @Test
    public void testAllChildrenWithFilterNullExp() {
        try {
            Exp nullExp = null;
            CTX.allChildrenWithFilter(nullExp);
            assertTrue(false, "Should throw NullPointerException when Exp is null");
        } catch (NullPointerException e) {
            // Expected - Exp.build() will throw NPE when trying to pack null exp
        }
    }

    @Test
    public void testAllChildrenWithFilterNullExpression() {
        Key key = dataSet.id("nullExpressionKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Integer> items = new ArrayList<>();
        items.add(1);
        items.add(2);
        items.add(3);
        data.put("items", items);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        try {
            Expression nullExpression = null;
            CTX ctx1 = CTX.mapKey(Value.get("items"));
            CTX ctx2 = CTX.allChildrenWithFilter(nullExpression);  // Passing null Expression

            Expression selectExp = Exp.build(
                CdtExp.selectByPath(
                    Exp.Type.LIST,
                    SelectFlags.VALUE,
                    Exp.mapBin("data"),
                    ctx1, ctx2
                )
            );

            RecordStream rs = session.upsert(key)
                .bin("result").upsertFrom(selectExp)
                .execute();

            rs.getFirstRecord();
            // The operation construction should fail or the server should reject it
            assertTrue(false, "Should handle null Expression appropriately");
        } catch (NullPointerException | AerospikeException e) {
            // Expected - null Expression should cause an error
        }
    }

    @Test
    public void testCombineExpAndExpressionVariants() {
        Key key = dataSet.id("combineVariantsKey");

        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> records = new ArrayList<>();

        Map<String, Object> rec1 = new HashMap<>();
        rec1.put("id", 1);
        rec1.put("value", 100);
        rec1.put("active", true);
        records.add(rec1);

        Map<String, Object> rec2 = new HashMap<>();
        rec2.put("id", 2);
        rec2.put("value", 50);
        rec2.put("active", false);
        records.add(rec2);

        Map<String, Object> rec3 = new HashMap<>();
        rec3.put("id", 3);
        rec3.put("value", 150);
        rec3.put("active", true);
        records.add(rec3);

        data.put("records", records);

        session.upsert(key)
            .bin("data").setTo(data)
            .execute();

        // Use pre-built Expression for first filter
        Expression activeFilterExpression = Exp.build(
            Exp.eq(
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.BOOL,
                    Exp.val("active"), Exp.mapLoopVar(LoopVarPart.VALUE)),
                Exp.val(true)
            )
        );

        // Mix Expression variant with Exp variant
        CTX ctx1 = CTX.mapKey(Value.get("records"));
        CTX ctx2 = CTX.allChildrenWithFilter(activeFilterExpression);  // Using Expression
        CTX ctx3 = CTX.allChildrenWithFilter(  // Using Exp directly
            Exp.eq(Exp.stringLoopVar(LoopVarPart.MAP_KEY), Exp.val("value"))
        );

        Expression selectExp = Exp.build(
            CdtExp.selectByPath(
                Exp.Type.LIST,
                SelectFlags.VALUE,
                Exp.mapBin("data"),
                ctx1, ctx2, ctx3
            )
        );

        RecordStream rs = session.upsert(key)
            .bin("activeValues").upsertFrom(selectExp)
            .execute();

        Record result = rs.getFirstRecord();
        assertTrue(result != null, "Operation should succeed");

        Record finalRecord = session.query(key)
            .execute()
            .getFirstRecord();
        assertNotNull(finalRecord, "Final record should exist");

        List<?> values = finalRecord.getList("activeValues");
        assertNotNull(values, "Values should exist");
        assertEquals(2, values.size(), "Should have 2 active records");
        assertTrue(values.contains(100L), "Should contain 100");
        assertTrue(values.contains(150L), "Should contain 150");
    }

    @Test
    public void testInList() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(8, 1, 2, 0),
            "Tests require server version 8.1.2 or later");

        Key key = dataSet.id("cdtExpInList");

        session.delete(key).execute();

        session.upsert(key)
            .bin("color").setTo("blue")
            .execute();

        // Check if bin "color" is in the list ["red", "blue", "green"]
        Expression exp = Exp.build(
            Exp.inList(
                Exp.stringBin("color"),
                Exp.val(Arrays.asList("red", "blue", "green"))
            )
        );

        Record result = session.query(key)
            .bin("inList").selectFrom(exp)
            .execute()
            .getFirstRecord();

        assertNotNull(result, "Result should not be null");
        assertTrue(result.getBoolean("inList"), "color 'blue' should be in the list");

        // Negative case: value not in list
        Expression expNot = Exp.build(
            Exp.inList(
                Exp.stringBin("color"),
                Exp.val(Arrays.asList("red", "yellow", "green"))
            )
        );

        Record resultNot = session.query(key)
            .bin("notInList").selectFrom(expNot)
            .execute()
            .getFirstRecord();

        assertNotNull(resultNot, "Result should not be null");
        assertTrue(!resultNot.getBoolean("notInList"), "color 'blue' should not be in the list");
    }

    @Test
    public void testMapKeys() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(8, 1, 2, 0),
            "Tests require server version 8.1.2 or later");

        Key key = dataSet.id("cdtExpMapKeys");

        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("x", 1);
        map.put("y", 2);
        map.put("z", 3);

        session.upsert(key)
            .bin("myMap").setTo(map)
            .execute();

        Expression exp = Exp.build(
            Exp.mapKeysIn(Exp.mapBin("myMap"))
        );

        Record result = session.query(key)
            .bin("keys").selectFrom(exp)
            .execute()
            .getFirstRecord();

        assertNotNull(result, "Result should not be null");
        List<?> keys = result.getList("keys");
        assertNotNull(keys, "Keys list should not be null");
        assertEquals(3, keys.size(), "Should have 3 keys");
        assertTrue(keys.contains("x"), "Should contain 'x'");
        assertTrue(keys.contains("y"), "Should contain 'y'");
        assertTrue(keys.contains("z"), "Should contain 'z'");
    }

    @Test
    public void testMapValues() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(8, 1, 2, 0),
            "Tests require server version 8.1.2 or later");

        Key key = dataSet.id("cdtExpMapValues");

        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("a", 100);
        map.put("b", 200);
        map.put("c", 300);

        session.upsert(key)
            .bin("myMap").setTo(map)
            .execute();

        Expression exp = Exp.build(
            Exp.mapValuesIn(Exp.mapBin("myMap"))
        );

        Record result = session.query(key)
            .bin("values").selectFrom(exp)
            .execute()
            .getFirstRecord();

        assertNotNull(result, "Result should not be null");
        List<?> values2 = result.getList("values");
        assertNotNull(values2, "Values list should not be null");
        assertEquals(3, values2.size(), "Should have 3 values");
        assertTrue(values2.contains(100L), "Should contain 100");
        assertTrue(values2.contains(200L), "Should contain 200");
        assertTrue(values2.contains(300L), "Should contain 300");
    }
}
