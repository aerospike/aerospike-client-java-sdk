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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;

/**
 * Tests for complex CDT (Collection Data Type) operations across read and write paths.
 *
 * Covers:
 * - Top-level list/map operations (no CDT navigation)
 * - Single-level CDT navigation into maps and lists
 * - Multi-level nested CDT navigation (map-within-map, list-within-map, etc.)
 * - Query (read-only) path: listSize, mapSize, listGet, listGetRange
 * - Write path: listAppend, listInsert, listSet, listIncrement, listSort,
 *   listRemove, listPop, listTrim, listClear, listAppendItems, listInsertItems,
 *   listCreate, listSetOrder, mapUpsertItems, mapCreate, mapSetPolicy, mapClear
 * - Combined multi-bin CDT operations in a single call
 */
public class CdtOperateComplexTest extends ClusterTest {

    private static final String KEY_PREFIX = "cdt_complex_";

    // ============================================================
    // Top-level list and map operations (no CDT navigation)
    // ============================================================

    @Nested
    class TopLevelOperations {

        private Key key;

        @BeforeEach
        public void setup() {
            key = args.set.id(KEY_PREFIX + "toplevel");
            session.delete(key).execute();
            session.upsert(key)
                .bin("scores").setTo(List.of(10, 20, 30, 40, 50))
                .bin("tags").setTo(List.of("java", "python", "rust"))
                .bin("inventory").setTo(Map.of("apples", 5, "bananas", 3, "cherries", 8))
                .execute();
        }

        @Test
        public void queryListSize() {
            RecordStream rs = session.query(key)
                .bin("scores").listSize()
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(5L, rec.getLong("scores"));
        }

        @Test
        public void queryMapSize() {
            RecordStream rs = session.query(key)
                .bin("inventory").mapSize()
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(3L, rec.getLong("inventory"));
        }

        @Test
        public void queryListGet() {
            RecordStream rs = session.query(key)
                .bin("scores").listGet(0)
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(10L, rec.getLong("scores"));
        }

        @Test
        public void queryListGetLastElement() {
            RecordStream rs = session.query(key)
                .bin("scores").listGet(-1)
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(50L, rec.getLong("scores"));
        }

        @Test
        public void queryListGetRangeWithCount() {
            RecordStream rs = session.query(key)
                .bin("scores").listGetRange(1, 3)
                .execute();
            Record rec = rs.next().recordOrThrow();
            List<?> result = rec.getList("scores");
            assertEquals(3, result.size());
            assertEquals(20L, result.get(0));
            assertEquals(30L, result.get(1));
            assertEquals(40L, result.get(2));
        }

        @Test
        public void queryListGetRangeToEnd() {
            RecordStream rs = session.query(key)
                .bin("scores").listGetRange(3)
                .execute();
            Record rec = rs.next().recordOrThrow();
            List<?> result = rec.getList("scores");
            assertEquals(2, result.size());
            assertEquals(40L, result.get(0));
            assertEquals(50L, result.get(1));
        }

        @Test
        public void queryMultipleBinCdtReads() {
            RecordStream rs = session.query(key)
                .bin("scores").listSize()
                .bin("inventory").mapSize()
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(5L, rec.getLong("scores"));
            assertEquals(3L, rec.getLong("inventory"));
        }

        @Test
        public void upsertListAppendItems() {
            session.upsert(key)
                .bin("scores").listAppendItems(List.of(60, 70))
                .execute();
            RecordStream rs = session.query(key).bin("scores").listSize().execute();
            assertEquals(7L, rs.next().recordOrThrow().getLong("scores"));
        }

        @Test
        public void updateListInsert() {
            session.update(key)
                .bin("tags").listInsert(1, "go")
                .execute();
            RecordStream rs = session.query(key).bin("tags").listGet(1).execute();
            assertEquals("go", rs.next().recordOrThrow().getString("tags"));
        }

        @Test
        public void updateListSet() {
            session.update(key)
                .bin("tags").listSet(0, "kotlin")
                .execute();
            RecordStream rs = session.query(key).bin("tags").listGet(0).execute();
            assertEquals("kotlin", rs.next().recordOrThrow().getString("tags"));
        }

        @Test
        public void updateListIncrement() {
            session.update(key)
                .bin("scores").listIncrement(0, 100)
                .execute();
            RecordStream rs = session.query(key).bin("scores").listGet(0).execute();
            assertEquals(110L, rs.next().recordOrThrow().getLong("scores"));
        }

        @Test
        public void updateListSort() {
            session.upsert(key)
                .bin("unsorted").setTo(List.of(50, 10, 40, 20, 30))
                .execute();
            session.update(key)
                .bin("unsorted").listSort()
                .execute();
            RecordStream rs = session.query(key).bin("unsorted").listGetRange(0).execute();
            List<?> sorted = rs.next().recordOrThrow().getList("unsorted");
            assertEquals(List.of(10L, 20L, 30L, 40L, 50L), sorted);
        }

        @Test
        public void updateListRemove() {
            session.update(key)
                .bin("scores").listRemove(0)
                .execute();
            RecordStream rs = session.query(key).bin("scores").listSize().execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("scores"));

            rs = session.query(key).bin("scores").listGet(0).execute();
            assertEquals(20L, rs.next().recordOrThrow().getLong("scores"));
        }

        @Test
        public void updateListRemoveRange() {
            session.update(key)
                .bin("scores").listRemoveRange(1, 2)
                .execute();
            RecordStream rs = session.query(key).bin("scores").listGetRange(0).execute();
            List<?> result = rs.next().recordOrThrow().getList("scores");
            assertEquals(List.of(10L, 40L, 50L), result);
        }

        @Test
        public void updateListPop() {
            RecordStream rs = session.update(key)
                .bin("scores").listPop(0)
                .execute();
            assertEquals(10L, rs.next().recordOrThrow().getLong("scores"));

            rs = session.query(key).bin("scores").listSize().execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("scores"));
        }

        @Test
        public void updateListTrim() {
            session.update(key)
                .bin("scores").listTrim(1, 3)
                .execute();
            RecordStream rs = session.query(key).bin("scores").listGetRange(0).execute();
            List<?> result = rs.next().recordOrThrow().getList("scores");
            assertEquals(List.of(20L, 30L, 40L), result);
        }

        @Test
        public void updateListClear() {
            session.update(key)
                .bin("tags").listClear()
                .execute();
            RecordStream rs = session.query(key).bin("tags").listSize().execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("tags"));
        }

        @Test
        public void updateMapUpsertItems() {
            session.update(key)
                .bin("inventory").mapUpsertItems(Map.of("dates", 12, "apples", 99))
                .execute();
            RecordStream rs = session.query(key).bin("inventory").mapSize().execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("inventory"));
        }

        @Test
        public void updateMapClear() {
            session.update(key)
                .bin("inventory").mapClear()
                .execute();
            RecordStream rs = session.query(key).bin("inventory").mapSize().execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("inventory"));
        }

        @Test
        public void updateMapSetPolicy() {
            session.update(key)
                .bin("inventory").mapSetPolicy(MapOrder.KEY_ORDERED)
                .execute();
            // Verify data is still intact after policy change
            RecordStream rs = session.query(key).bin("inventory").mapSize().execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("inventory"));
        }

        @Test
        public void insertListInsertItems() {
            session.update(key)
                .bin("scores").listInsertItems(2, List.of(25, 26))
                .execute();
            RecordStream rs = session.query(key).bin("scores").listSize().execute();
            assertEquals(7L, rs.next().recordOrThrow().getLong("scores"));

            rs = session.query(key).bin("scores").listGetRange(1, 4).execute();
            List<?> result = rs.next().recordOrThrow().getList("scores");
            assertEquals(List.of(20L, 25L, 26L, 30L), result);
        }
    }

    // ============================================================
    // Single-level CDT navigation
    // ============================================================

    @Nested
    class SingleLevelNavigation {

        private Key key;

        @BeforeEach
        public void setup() {
            key = args.set.id(KEY_PREFIX + "single_nav");
            session.delete(key).execute();
            session.upsert(key)
                .bin("data").setTo(Map.of(
                    "users", List.of("Alice", "Bob", "Charlie"),
                    "counts", List.of(10, 20, 30),
                    "meta", Map.of("version", 1, "active", true)
                ))
                .execute();
        }

        @Test
        public void queryMapSizeAfterNavigation() {
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("meta").mapSize()
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(2L, rec.getLong("data"));
        }

        @Test
        public void queryListSizeAfterNavigation() {
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listSize()
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals(3L, rec.getLong("data"));
        }

        @Test
        public void queryListGetAfterNavigation() {
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listGet(1)
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertEquals("Bob", rec.getString("data"));
        }

        @Test
        public void queryListGetRangeAfterNavigation() {
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGetRange(0, 2)
                .execute();
            Record rec = rs.next().recordOrThrow();
            List<?> result = rec.getList("data");
            assertEquals(2, result.size());
            assertEquals(10L, result.get(0));
            assertEquals(20L, result.get(1));
        }

        @Test
        public void queryListGetRangeToEndAfterNavigation() {
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGetRange(1)
                .execute();
            Record rec = rs.next().recordOrThrow();
            List<?> result = rec.getList("data");
            assertEquals(2, result.size());
            assertEquals(20L, result.get(0));
            assertEquals(30L, result.get(1));
        }

        @Test
        public void updateListAppendAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("users").listAppend("Diana")
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listSize()
                .execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListInsertAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("users").listInsert(0, "Zara")
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listGet(0)
                .execute();
            assertEquals("Zara", rs.next().recordOrThrow().getString("data"));
        }

        @Test
        public void updateListSetAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("users").listSet(1, "Bobby")
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listGet(1)
                .execute();
            assertEquals("Bobby", rs.next().recordOrThrow().getString("data"));
        }

        @Test
        public void updateListRemoveAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("users").listRemove(0)
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListClearAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("users").listClear()
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listSize()
                .execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListSortAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("counts").listSort()
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGetRange(0)
                .execute();
            List<?> sorted = rs.next().recordOrThrow().getList("data");
            assertEquals(List.of(10L, 20L, 30L), sorted);
        }

        @Test
        public void updateListIncrementAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("counts").listIncrement(0, 5)
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGet(0)
                .execute();
            assertEquals(15L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListAppendItemsAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("users").listAppendItems(List.of("Eve", "Frank"))
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("users").listSize()
                .execute();
            assertEquals(5L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListInsertItemsAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("counts").listInsertItems(1, List.of(15, 16))
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGetRange(0)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("data");
            assertEquals(List.of(10L, 15L, 16L, 20L, 30L), result);
        }

        @Test
        public void updateMapClearAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("meta").mapClear()
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("meta").mapSize()
                .execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListTrimAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("counts").listTrim(0, 2)
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGetRange(0)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("data");
            assertEquals(List.of(10L, 20L), result);
        }

        @Test
        public void updateListPopAfterNavigation() {
            RecordStream rs = session.update(key)
                .bin("data").onMapKey("counts").listPop(0)
                .execute();
            assertEquals(10L, rs.next().recordOrThrow().getLong("data"));
        }

        @Test
        public void updateListRemoveRangeAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("counts").listRemoveRange(0, 2)
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("counts").listGetRange(0)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("data");
            assertEquals(List.of(30L), result);
        }

        @Test
        public void updateMapUpsertItemsAfterNavigation() {
            session.update(key)
                .bin("data").onMapKey("meta").mapUpsertItems(Map.of("region", "us-east"))
                .execute();
            RecordStream rs = session.query(key)
                .bin("data").onMapKey("meta").mapSize()
                .execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("data"));
        }
    }

    // ============================================================
    // Multi-level nested CDT navigation
    // ============================================================

    @Nested
    class MultiLevelNavigation {

        private Key key;

        @BeforeEach
        public void setup() {
            key = args.set.id(KEY_PREFIX + "multi_nav");
            session.delete(key).execute();
            session.upsert(key)
                .bin("org").setTo(Map.of(
                    "dept1", Map.of(
                        "name", "Engineering",
                        "members", List.of("Alice", "Bob", "Charlie"),
                        "projects", Map.of(
                            "alpha", Map.of("status", "active", "priority", 1),
                            "beta", Map.of("status", "complete", "priority", 3)
                        )
                    ),
                    "dept2", Map.of(
                        "name", "Marketing",
                        "members", List.of("Dave", "Eve")
                    )
                ))
                .execute();
        }

        // -- Query (read-only) --

        @Test
        public void queryListSizeTwoLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listSize()
                .execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void queryListGetTwoLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listGet(0)
                .execute();
            assertEquals("Alice", rs.next().recordOrThrow().getString("org"));
        }

        @Test
        public void queryListGetNegativeIndexTwoLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listGet(-1)
                .execute();
            assertEquals("Charlie", rs.next().recordOrThrow().getString("org"));
        }

        @Test
        public void queryListGetRangeTwoLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listGetRange(1, 2)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("org");
            assertEquals(2, result.size());
            assertEquals("Bob", result.get(0));
            assertEquals("Charlie", result.get(1));
        }

        @Test
        public void queryListGetRangeToEndTwoLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listGetRange(0)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("org");
            assertEquals(2, result.size());
            assertEquals("Dave", result.get(0));
            assertEquals("Eve", result.get(1));
        }

        @Test
        public void queryMapSizeTwoLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("projects").mapSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void queryMapSizeThreeLevelsDeep() {
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("projects").onMapKey("alpha").mapSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("org"));
        }

        // -- Write (update/upsert) --

        @Test
        public void updateListAppendTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listAppend("Diana")
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listSize()
                .execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void updateListInsertTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listInsert(0, "Zara")
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listGet(0)
                .execute();
            assertEquals("Zara", rs.next().recordOrThrow().getString("org"));
        }

        @Test
        public void updateListRemoveTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listRemove(0)
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listGet(0)
                .execute();
            assertEquals("Bob", rs.next().recordOrThrow().getString("org"));
        }

        @Test
        public void updateListClearTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listClear()
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listSize()
                .execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void updateMapClearThreeLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("projects").onMapKey("alpha").mapClear()
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("projects").onMapKey("alpha").mapSize()
                .execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void updateMapUpsertItemsThreeLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("projects").onMapKey("alpha")
                    .mapUpsertItems(Map.of("owner", "Alice", "priority", 0))
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("projects").onMapKey("alpha").mapSize()
                .execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void updateListSortTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listSort()
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listGet(0)
                .execute();
            assertEquals("Alice", rs.next().recordOrThrow().getString("org"));
        }

        @Test
        public void updateListAppendItemsTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listAppendItems(List.of("Frank", "Grace"))
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept2").onMapKey("members").listSize()
                .execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void updateListTrimTwoLevelsDeep() {
            session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listTrim(0, 2)
                .execute();
            RecordStream rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("org"));
        }

        @Test
        public void updateListPopTwoLevelsDeep() {
            RecordStream rs = session.update(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listPop(0)
                .execute();
            assertEquals("Alice", rs.next().recordOrThrow().getString("org"));

            rs = session.query(key)
                .bin("org").onMapKey("dept1").onMapKey("members").listSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("org"));
        }
    }

    // ============================================================
    // Creating nested structures via CDT navigation
    // ============================================================

    @Nested
    class NestedStructureCreation {

        private Key key;

        @BeforeEach
        public void setup() {
            key = args.set.id(KEY_PREFIX + "create_nested");
            session.delete(key).execute();
            session.upsert(key)
                .bin("root").setTo(Map.of("existing", 1))
                .execute();
        }

        @Test
        public void createOrderedMapViaNavigation() {
            session.upsert(key)
                .bin("root").onMapKey("child", MapOrder.KEY_ORDERED).onMapKey("key1").upsert(100)
                .execute();
            RecordStream rs = session.query(key)
                .bin("root").onMapKey("child").mapSize()
                .execute();
            assertEquals(1L, rs.next().recordOrThrow().getLong("root"));
        }

        @Test
        public void createOrderedListViaNavigation() {
            session.upsert(key)
                .bin("root").onMapKey("child", MapOrder.KEY_ORDERED)
                    .onMapKey("items").listCreate(ListOrder.ORDERED)
                .execute();
            session.update(key)
                .bin("root").onMapKey("child").onMapKey("items").listAddItems(List.of("cherry", "apple", "banana"))
                .execute();
            RecordStream rs = session.query(key)
                .bin("root").onMapKey("child").onMapKey("items").listGetRange(0)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("root");
            assertEquals("apple", result.get(0));
            assertEquals("banana", result.get(1));
            assertEquals("cherry", result.get(2));
        }

        @Test
        public void createNestedMapMultipleLevels() {
            session.upsert(key)
                .bin("root").onMapKey("level1", MapOrder.KEY_ORDERED)
                    .onMapKey("level2", MapOrder.KEY_ORDERED)
                        .onMapKey("level3").upsert("deep_value")
                .execute();
            RecordStream rs = session.query(key)
                .bin("root").onMapKey("level1").onMapKey("level2").onMapKey("level3").getValues()
                .execute();
            assertEquals("deep_value", rs.next().recordOrThrow().getString("root"));
        }
    }

    // ============================================================
    // Combined multi-bin CDT operations
    // ============================================================

    @Nested
    class CombinedOperations {

        private Key key;

        @BeforeEach
        public void setup() {
            key = args.set.id(KEY_PREFIX + "combined");
            session.delete(key).execute();
            session.upsert(key)
                .bin("scores").setTo(List.of(10, 20, 30))
                .bin("tags").setTo(List.of("a", "b", "c"))
                .bin("config").setTo(Map.of(
                    "settings", Map.of("debug", false),
                    "users", List.of("admin")
                ))
                .execute();
        }

        @Test
        public void mixedReadsAndWritesAcrossBins() {
            RecordStream rs = session.update(key)
                .bin("scores").listAppend(40)
                .bin("tags").listSize()
                .bin("config").onMapKey("users").listAppend("guest")
                .execute();
            assertNotNull(rs.next().recordOrThrow());

            rs = session.query(key)
                .bin("scores").listSize()
                .execute();
            assertEquals(4L, rs.next().recordOrThrow().getLong("scores"));

            rs = session.query(key)
                .bin("config").onMapKey("users").listSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("config"));
        }

        @Test
        public void multipleNavigatedWritesSameCall() {
            session.update(key)
                .bin("config").onMapKey("users").listAppend("user1")
                .bin("config").onMapKey("settings").mapUpsertItems(Map.of("debug", true, "verbose", true))
                .execute();

            RecordStream rs = session.query(key)
                .bin("config").onMapKey("users").listSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("config"));

            rs = session.query(key)
                .bin("config").onMapKey("settings").mapSize()
                .execute();
            assertEquals(2L, rs.next().recordOrThrow().getLong("config"));
        }

        @Test
        public void queryMultipleNavigatedReads() {
            RecordStream rs = session.query(key)
                .bin("scores").listSize()
                .bin("config").onMapKey("users").listSize()
                .bin("config").onMapKey("settings").mapSize()
                .execute();
            Record rec = rs.next().recordOrThrow();
            assertNotNull(rec);
        }

        @Test
        public void insertWithCdtOperations() {
            Key newKey = args.set.id(KEY_PREFIX + "combined_insert");
            session.delete(newKey).execute();
            session.insert(newKey)
                .bin("numbers").setTo(List.of(1, 2, 3))
                .bin("meta").setTo(Map.of("count", 3))
                .execute();

            RecordStream rs = session.query(newKey)
                .bin("numbers").listSize()
                .execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("numbers"));

            rs = session.query(newKey)
                .bin("meta").mapSize()
                .execute();
            assertEquals(1L, rs.next().recordOrThrow().getLong("meta"));
        }
    }

    // ============================================================
    // Batch key queries with CDT operations
    // ============================================================

    @Nested
    class BatchCdtOperations {

        @BeforeEach
        public void setup() {
            for (int i = 1; i <= 3; i++) {
                Key k = args.set.id(KEY_PREFIX + "batch_" + i);
                session.delete(k).execute();
                session.upsert(k)
                    .bin("items").setTo(List.of(i * 10, i * 20, i * 30))
                    .bin("props").setTo(Map.of("id", i, "label", "item" + i))
                    .execute();
            }
        }

        @Test
        public void batchQueryListSize() {
            Key k1 = args.set.id(KEY_PREFIX + "batch_1");
            Key k2 = args.set.id(KEY_PREFIX + "batch_2");

            RecordStream rs = session.query(k1, k2)
                .bin("items").listSize()
                .execute();

            int count = 0;
            while (rs.hasNext()) {
                RecordResult rr = rs.next();
                assertTrue(rr.isOk());
                assertEquals(3L, rr.recordOrThrow().getLong("items"));
                count++;
            }
            assertEquals(2, count);
        }

        @Test
        public void batchQueryMapSize() {
            Key k1 = args.set.id(KEY_PREFIX + "batch_1");
            Key k2 = args.set.id(KEY_PREFIX + "batch_2");
            Key k3 = args.set.id(KEY_PREFIX + "batch_3");

            RecordStream rs = session.query(k1, k2, k3)
                .bin("props").mapSize()
                .execute();

            int count = 0;
            while (rs.hasNext()) {
                RecordResult rr = rs.next();
                assertTrue(rr.isOk());
                assertEquals(2L, rr.recordOrThrow().getLong("props"));
                count++;
            }
            assertEquals(3, count);
        }

        @Test
        public void batchQueryListGet() {
            Key k1 = args.set.id(KEY_PREFIX + "batch_1");
            Key k2 = args.set.id(KEY_PREFIX + "batch_2");

            RecordStream rs = session.query(k1, k2)
                .bin("items").listGet(0)
                .execute();

            assertTrue(rs.hasNext());
            assertEquals(10L, rs.next().recordOrThrow().getLong("items"));
            assertTrue(rs.hasNext());
            assertEquals(20L, rs.next().recordOrThrow().getLong("items"));
        }
    }

    // ============================================================
    // Edge cases and regression guards
    // ============================================================

    @Nested
    class EdgeCases {

        private Key key;

        @BeforeEach
        public void setup() {
            key = args.set.id(KEY_PREFIX + "edge");
            session.delete(key).execute();
        }

        @Test
        public void listSizeOnEmptyList() {
            session.upsert(key)
                .bin("empty").setTo(List.of())
                .execute();
            RecordStream rs = session.query(key)
                .bin("empty").listSize()
                .execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("empty"));
        }

        @Test
        public void mapSizeOnEmptyMap() {
            session.upsert(key)
                .bin("empty").setTo(Map.of())
                .execute();
            RecordStream rs = session.query(key)
                .bin("empty").mapSize()
                .execute();
            assertEquals(0L, rs.next().recordOrThrow().getLong("empty"));
        }

        @Test
        public void listGetRangeReturnsEmptyForOutOfRange() {
            session.upsert(key)
                .bin("small").setTo(List.of(1, 2))
                .execute();
            RecordStream rs = session.query(key)
                .bin("small").listGetRange(2)
                .execute();
            Record rec = rs.next().recordOrThrow();
            List<?> result = rec.getList("small");
            assertTrue(result.isEmpty());
        }

        @Test
        public void listSizeOnSingleElement() {
            session.upsert(key)
                .bin("single").setTo(List.of(42))
                .execute();
            RecordStream rs = session.query(key)
                .bin("single").listSize()
                .execute();
            assertEquals(1L, rs.next().recordOrThrow().getLong("single"));
        }

        @Test
        public void deepNestedListSizeRegression() {
            session.upsert(key)
                .bin("deep").setTo(Map.of(
                    "a", Map.of(
                        "b", Map.of(
                            "c", List.of(1, 2, 3, 4, 5)
                        )
                    )
                ))
                .execute();
            RecordStream rs = session.query(key)
                .bin("deep").onMapKey("a").onMapKey("b").onMapKey("c").listSize()
                .execute();
            assertEquals(5L, rs.next().recordOrThrow().getLong("deep"));
        }

        @Test
        public void deepNestedListGetRegression() {
            session.upsert(key)
                .bin("deep").setTo(Map.of(
                    "a", Map.of(
                        "b", Map.of(
                            "c", List.of(100, 200, 300)
                        )
                    )
                ))
                .execute();
            RecordStream rs = session.query(key)
                .bin("deep").onMapKey("a").onMapKey("b").onMapKey("c").listGet(2)
                .execute();
            assertEquals(300L, rs.next().recordOrThrow().getLong("deep"));
        }

        @Test
        public void deepNestedListGetRangeRegression() {
            session.upsert(key)
                .bin("deep").setTo(Map.of(
                    "a", Map.of(
                        "b", Map.of(
                            "c", List.of(10, 20, 30, 40)
                        )
                    )
                ))
                .execute();
            RecordStream rs = session.query(key)
                .bin("deep").onMapKey("a").onMapKey("b").onMapKey("c").listGetRange(1, 2)
                .execute();
            List<?> result = rs.next().recordOrThrow().getList("deep");
            assertEquals(List.of(20L, 30L), result);
        }

        @Test
        public void deepNestedMapSizeRegression() {
            session.upsert(key)
                .bin("deep").setTo(Map.of(
                    "a", Map.of(
                        "b", Map.of("x", 1, "y", 2, "z", 3)
                    )
                ))
                .execute();
            RecordStream rs = session.query(key)
                .bin("deep").onMapKey("a").onMapKey("b").mapSize()
                .execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("deep"));
        }

        @Test
        public void deepNestedWriteThenReadRegression() {
            session.upsert(key)
                .bin("deep").setTo(Map.of(
                    "a", Map.of(
                        "b", Map.of(
                            "items", List.of("x", "y")
                        )
                    )
                ))
                .execute();

            session.update(key)
                .bin("deep").onMapKey("a").onMapKey("b").onMapKey("items").listAppend("z")
                .execute();

            RecordStream rs = session.query(key)
                .bin("deep").onMapKey("a").onMapKey("b").onMapKey("items").listSize()
                .execute();
            assertEquals(3L, rs.next().recordOrThrow().getLong("deep"));

            rs = session.query(key)
                .bin("deep").onMapKey("a").onMapKey("b").onMapKey("items").listGet(-1)
                .execute();
            assertEquals("z", rs.next().recordOrThrow().getString("deep"));
        }
    }
}
