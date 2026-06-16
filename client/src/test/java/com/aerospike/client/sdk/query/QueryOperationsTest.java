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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.task.ExecuteTask;
import com.aerospike.client.sdk.util.Version;

public class QueryOperationsTest extends ClusterTest {
    private static final String indexName = "tqoindex";
    private static final String keyPrefix = "tqokey";
    private static final String binName1 = "tqobin1";
    private static final String binName2 = "tqobin2";
    private static final String binName3 = "tqobin3";
    private static final String mapBin = "tqomapbin";
    private static final int size = 20;

    @BeforeAll
    public static void prepare() {
        Assumptions.assumeTrue(
            session.getCluster().getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_2),
            "Ops projection extended requires server version 8.1.2 or later");

        try {
            session.createIndex(args.set, indexName, binName1, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        for (int i = 1; i <= size; i++) {
            Key key = args.set.id(keyPrefix + i);
            Map<String,Integer> map = new HashMap<>();
            map.put("a", i);
            map.put("b", i * 10);

            session.upsert(key)
                .bin(binName1).setTo(i)
                .bin(binName2).setTo(i * 10)
                .bin(binName3).setTo(i * 100)
                .bin(mapBin).setTo(map)
                .execute();
        }
    }

    @AfterAll
    public static void destroy() {
        session.dropIndex(args.set, indexName);
    }

    @Test
    public void queryProjectMultipleBins() {
        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin(binName2).get()
            .bin(mapBin).onMapKey("a").getValues()
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();

                assertNotNull(rec.getValue(binName1));
                assertNotNull(rec.getValue(binName2));
                assertNotNull(rec.getValue(mapBin));

                long val1 = rec.getLong(binName1);
                long val2 = rec.getLong(binName2);
                long mapVal = rec.getLong(mapBin);

                assertEquals(val1 * 10, val2);
                assertEquals(val1, mapVal);
                assertNull(rec.getValue(binName3));

                count++;
            }
            assertTrue(count >= size);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryProjectSubsetOfBins() {
        int begin = 1;
        int end = 10;

        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin(binName3).get()
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long val1 = rec.getLong(binName1);
                long val3 = rec.getLong(binName3);
                assertTrue(val1 >= begin && val1 <= end);
                assertEquals(val1 * 100, val3);
                assertNull(rec.getValue(binName2));
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryProjectBinsViaExpressionRead() {
        String typeSuffix = cluster.supportsAel() ? ":INT" : "";

        RecordStream rs = session.query(args.set)
                .bin("result1").selectFrom("$." + binName1 + typeSuffix)
                .bin("result2").selectFrom("$." + binName2 + typeSuffix)
                .bin("result3").selectFrom("$." + binName3 + typeSuffix)
                .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long r1 = rec.getLong("result1");
                long r2 = rec.getLong("result2");
                long r3 = rec.getLong("result3");
                assertEquals(r1 * 10, r2);
                assertEquals(r1 * 100, r3);
                count++;
            }
            assertTrue(count >= size);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryProjectBinsViaExpressionReadWithFilter() {
        int begin = 1;
        int end = 10;

        String typeSuffix = cluster.supportsAel() ? ":INT" : "";

        RecordStream rs = session.query(args.set)
            .bin("result1").selectFrom("$." + binName1 + typeSuffix)
            .bin("result2").selectFrom("$." + binName2 + typeSuffix)
            .bin("result3").selectFrom("$." + binName3 + typeSuffix)
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long r1 = rec.getLong("result1");
                long r2 = rec.getLong("result2");
                long r3 = rec.getLong("result3");
                assertTrue(r1 >= begin && r1 <= end);
                assertEquals(r1 * 10, r2);
                assertEquals(r1 * 100, r3);
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryProjectMixedGetAndExpressionRead() {
        int begin = 1;
        int end = 10;

        String typeSuffix = cluster.supportsAel() ? ":INT" : "";

        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin("sum").selectFrom("$." + binName1 + typeSuffix + " + $." + binName2 + typeSuffix)
            // TODO where queries go through client side parsing due to index selection - and hence no type annotation
            // this will be cleaned up when index selection is moved to server side
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long val1 = rec.getLong(binName1);
                long sum = rec.getLong("sum");
                assertTrue(val1 >= begin && val1 <= end);
                assertEquals(val1 + val1 * 10, sum);
                assertNull(rec.getValue(binName2));
                assertNull(rec.getValue(binName3));
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryWithExpReadOperation() {
        int begin = 1;
        int end = 10;

        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin("computed").selectFrom("$." + binName1 + " * 100")
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long computed = rec.getLong("computed");
                long original = rec.getLong(binName1);
                assertEquals(original * 100, computed);
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryWithMultipleExpReadOperations() {
        int begin = 5;
        int end = 15;

        String typeSuffix = cluster.supportsAel() ? ":INT" : "";

        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin(binName2).get()
            .bin("sum").selectFrom("$." + binName1 + typeSuffix + " + $." + binName2 + typeSuffix)
            .bin("diff").selectFrom("$." + binName2 + typeSuffix + " - $." + binName1 + typeSuffix)
            // TODO where queries go through client side parsing due to index selection - and hence no type annotation
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long val1 = rec.getLong(binName1);
                long val2 = rec.getLong(binName2);
                long sum = rec.getLong("sum");
                long diff = rec.getLong("diff");
                assertEquals(val1 + val2, sum);
                assertEquals(val2 - val1, diff);
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryWithExpReadAndFilterExp() {
        // Combined: secondary-index-style range filter AND an additional
        // less-than predicate, all expressed in a single where() clause.
        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin("doubled").selectFrom("$." + binName1 + " * 2")
            .where("$." + binName1 + " >= 1 and $." + binName1 + " <= 20 and $." + binName1 + " < 6")
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long doubled = rec.getLong("doubled");
                long original = rec.getLong(binName1);
                assertEquals(original * 2, doubled);
                assertTrue(original < 6);
                count++;
            }
            assertEquals(5, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryWithGetOperation() {
        int begin = 1;
        int end = 5;

        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                long val1 = rec.getLong(binName1);
                assertTrue(val1 >= begin && val1 <= end);
                assertNull(rec.getValue(binName2));
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void backgroundUpdateWriteSucceeds() {
        // New-API counterpart of the old executeWithWriteOperationSucceeds
        // test: write a "marker" bin to records in a filtered range using
        // a background task, then read it back per-key.
        int begin = 1;
        int end = 3;

        ExecuteTask task = session.backgroundTask()
            .update(args.set)
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .bin("marker").setTo("executed")
            .execute();
        task.waitTillComplete();

        for (int i = begin; i <= end; i++) {
            RecordStream rs = session.query(args.set.id(keyPrefix + i))
                .bin("marker").get()
                .execute();
            try {
                assertTrue(rs.hasNext());
                Record rec = rs.next().recordOrThrow();
                assertEquals("executed", rec.getString("marker"));
            }
            finally {
                rs.close();
            }
        }
    }

    @Test
    public void queryWithExpReadNoFilter() {
        RecordStream rs = session.query(args.set)
            .bin("offset").selectFrom("$." + binName1 + " + 1000")
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                Object offsetVal = rec.getValue("offset");
                assertNotNull(offsetVal);
                count++;
            }
            assertTrue(count >= size);
        }
        finally {
            rs.close();
        }
    }

    @Test
    public void queryWithExpReadEvalNoFail() {
        // Reading a non-existent bin would normally fail; ignoreEvalFailure
        // is the new-API replacement for ExpReadFlags.EVAL_NO_FAIL.
        int begin = 1;
        int end = 5;
        String typeSuffix = cluster.supportsAel() ? ":INT" : "";

        RecordStream rs = session.query(args.set)
            .bin(binName1).get()
            .bin("result").selectFrom("$.nonexistent" + typeSuffix, arg -> arg.ignoreEvalFailure())
            .where("$." + binName1 + " >= " + begin + " and $." + binName1 + " <= " + end)
            .execute();

        try {
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                assertNotNull(rec.getValue(binName1));
                count++;
            }
            assertEquals(end - begin + 1, count);
        }
        finally {
            rs.close();
        }
    }
}
