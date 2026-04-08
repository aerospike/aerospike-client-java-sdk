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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive tests for read operations on queries.
 * Tests cover:
 * - Simple bin reads (get)
 * - Expression-based reads (selectFrom)
 * - CDT read operations (map/list navigation and reads)
 * - Single-key and batch-key queries
 * - Dataset queries (should throw for operations)
 */
public class QueryOperationsTest extends ClusterTest {

    private static final String KEY_PREFIX = "query_ops_";

    @BeforeEach
    public void setupTestData() {
        // Create test records with various data types
        for (int i = 1; i <= 3; i++) {
            String key = KEY_PREFIX + i;

            Map<String, Object> settings = new HashMap<>();
            settings.put("theme", "dark");
            settings.put("volume", i * 10);
            settings.put("notifications", true);

            session.upsert(args.set.id(key))
                .bin("name").setTo("user" + i)
                .bin("age").setTo(20 + i)
                .bin("score").setTo(i * 100)
                .bin("settings").setTo(settings)
                .bin("scores").setTo(List.of(i * 10, i * 20, i * 30))
                .execute();
        }
    }

    // ========== Simple Bin Read Tests ==========

    @Nested
    class SimpleBinReads {

        @Test
        public void getSingleBin() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("name").get()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("user1", rec.getString("name"));
        }

        @Test
        public void getMultipleBins() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("name").get()
                .bin("age").get()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("user1", rec.getString("name"));
            assertEquals(21, rec.getInt("age"));
        }
    }

    // ========== Expression Read Tests ==========

    @Nested
    class ExpressionReads {

        @Test
        public void selectFromSimpleExpression() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("ageIn20Years").selectFrom("$.age + 20")
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(41L, rec.getLong("ageIn20Years"));
        }

        @Test
        public void selectFromMultipleExpressions() {
            String key = KEY_PREFIX + "2";

            RecordStream rs = session.query(args.set.id(key))
                .bin("doubleAge").selectFrom("$.age * 2")
                .bin("tripleScore").selectFrom("$.score * 3")
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(44L, rec.getLong("doubleAge")); // (20+2) * 2
            assertEquals(600L, rec.getLong("tripleScore")); // 200 * 3
        }

        @Test
        public void selectFromWithGet() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("name").get()
                .bin("ageIn10Years").selectFrom("$.age + 10")
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("user1", rec.getString("name"));
            assertEquals(31L, rec.getLong("ageIn10Years"));
        }
    }

    // ========== CDT Map Read Tests ==========

    @Nested
    class CdtMapReads {

        @Test
        public void mapKeyGetValue() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("settings").onMapKey("theme").getValues()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("dark", rec.getString("settings"));
        }

        @Test
        public void mapKeyGetCount() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("settings").onMapKey("theme").count()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            // Count of a single key is 1
            assertEquals(1L, rec.getLong("settings"));
        }

        @Test
        public void mapIndexRangeGetValues() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("settings").onMapIndexRange(0, 2).getValues()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            Object result = rec.getValue("settings");
            assertNotNull(result);
            assertTrue(result instanceof List);
        }

        @Test
        public void mapRankGetValue() {
            String key = KEY_PREFIX + "2";

            RecordStream rs = session.query(args.set.id(key))
                .bin("settings").onMapRank(0).getValues()
                .execute();

            assertTrue(rs.hasNext());
            // Rank 0 = lowest value, which would be boolean 'true' (notifications)
        }
    }

    // ========== CDT List Read Tests ==========

    @Nested
    class CdtListReads {

        @Test
        public void listIndexGetValue() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("scores").onListIndex(0).getValues()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(10L, rec.getLong("scores"));
        }

        @Test
        public void listIndexGetCount() {
            String key = KEY_PREFIX + "1";

            RecordStream rs = session.query(args.set.id(key))
                .bin("scores").onListIndex(0).count()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(1L, rec.getLong("scores"));
        }

        @Test
        public void listRankGetValue() {
            String key = KEY_PREFIX + "2";

            RecordStream rs = session.query(args.set.id(key))
                .bin("scores").onListRank(0).getValues()
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            // Rank 0 = lowest value = 20 (for key 2: [20, 40, 60])
            assertEquals(20L, rec.getLong("scores"));
        }
    }

    // ========== Batch Key Query Tests ==========

    @Nested
    class BatchKeyQueries {

        @Test
        public void batchQueryWithGet() {
            Key key1 = args.set.id(KEY_PREFIX + "1");
            Key key2 = args.set.id(KEY_PREFIX + "2");

            RecordStream rs = session.query(key1, key2)
                .bin("name").get()
                .execute();

            int count = 0;
            while (rs.hasNext()) {
                RecordResult result = rs.next();
                assertTrue(result.isOk());
                assertNotNull(result.recordOrThrow().getString("name"));
                count++;
            }
            assertEquals(2, count);
        }

        @Test
        public void batchQueryWithSelectFrom() {
            Key key1 = args.set.id(KEY_PREFIX + "1");
            Key key2 = args.set.id(KEY_PREFIX + "2");

            RecordStream rs = session.query(key1, key2)
                .bin("ageIn20Years").selectFrom("$.age + 20")
                .execute();

            int count = 0;
            while (rs.hasNext()) {
                RecordResult result = rs.next();
                assertTrue(result.isOk());
                assertNotNull(result.recordOrThrow().getLong("ageIn20Years"));
                count++;
            }
            assertEquals(2, count);
        }

        @Test
        public void batchQueryWithCdtRead() {
            Key key1 = args.set.id(KEY_PREFIX + "1");
            Key key2 = args.set.id(KEY_PREFIX + "2");
            Key key3 = args.set.id(KEY_PREFIX + "3");

            RecordStream rs = session.query(key1, key2, key3)
                .bin("settings").onMapKey("theme").getValues()
                .execute();

            int count = 0;
            while (rs.hasNext()) {
                RecordResult result = rs.next();
                assertTrue(result.isOk());
                assertEquals("dark", result.recordOrThrow().getString("settings"));
                count++;
            }
            assertEquals(3, count);
        }
    }

    // ========== Dataset Query Tests (should throw) ==========

    @Nested
    class DatasetQueries {

        @Test
        public void datasetQueryWithCdtReadThrowsException() {
            // CDT operations on dataset queries are not supported by the server
            AerospikeException ex = assertThrows(AerospikeException.class, () -> {
                session.query(args.set)
                    .bin("settings").onMapKey("theme").getValues()
                    .execute();
            });

            assertTrue(ex.getMessage().contains("not currently supported"));
        }

        @Test
        public void datasetQueryWithSelectFromThrowsException() {
            // Expression operations on dataset queries are not supported
            AerospikeException ex = assertThrows(AerospikeException.class, () -> {
                session.query(args.set)
                    .bin("computed").selectFrom("$.age + 10")
                    .execute();
            });

            assertTrue(ex.getMessage().contains("not currently supported"));
        }
    }

    // ========== Chained Query Tests ==========

    @Nested
    class ChainedQueries {

        @Test
        public void chainedQueryWithOperations() {
            String key1 = KEY_PREFIX + "1";
            String key2 = KEY_PREFIX + "2";

            // First upsert, then query with operations
            RecordStream rs = session
                .upsert(args.set.id(key1))
                    .bin("status").setTo("active")
                .query(args.set.id(key2))
                    .bin("name").get()
                    .bin("computed").selectFrom("$.age * 2")
                .execute();

            // Should return results for the query (not the upsert)
            assertTrue(rs.hasNext());
        }
    }
}
