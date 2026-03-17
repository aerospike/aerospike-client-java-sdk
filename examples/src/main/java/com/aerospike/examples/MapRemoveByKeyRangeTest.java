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

import java.util.TreeMap;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.cdt.MapReturnType;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.MapExp;
import com.aerospike.client.fluent.policy.Behavior;

/**
 * Test to verify the actual return value of MapExp.removeByKeyRange when used
 * as a read expression.
 *
 * <p>The Javadoc for MapExp.removeByXxx states:
 * "Valid returnType values are MapReturnType.NONE or MapReturnType.INVERTED."
 *
 * <p>The class-level Javadoc says:
 * "Map modify expressions return the bin's value."
 *
 * <p>This test creates a known map and calls removeByKeyRange as a read expression
 * to see what is actually returned.
 */
public class MapRemoveByKeyRangeTest {

    public static void main(String[] args) throws Exception {
        try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet set = DataSet.of("test", "map_remove_test");

            session.truncate(set);

            // Seed: key-ordered map {a=1, b=2, c=3, d=4, e=5}
            TreeMap<String, Long> sourceMap = new TreeMap<>();
            sourceMap.put("a", 1L);
            sourceMap.put("b", 2L);
            sourceMap.put("c", 3L);
            sourceMap.put("d", 4L);
            sourceMap.put("e", 5L);

            session.upsert(set.id(1))
                    .bin("m").setTo(sourceMap)
                    .execute();

            System.out.println("Source map: " + sourceMap);
            System.out.println();

            // removeByKeyRange("b", "e") removes keys b, c, d (b inclusive, e exclusive)
            // That is 3 items removed, leaving {a=1, e=5}

            // --- Test 1: returnType = NONE ---
            System.out.println("=== Test 1: MapExp.removeByKeyRange(NONE, \"b\", \"e\") ===");
            System.out.println("Javadoc says NONE is valid. Class doc says modify expressions return the bin's value.");
            System.out.println("Expected: the modified map {a=1, e=5}");
            try {
                Exp removeNone = MapExp.removeByKeyRange(
                        MapReturnType.NONE,
                        Exp.val("b"), Exp.val("e"),
                        Exp.mapBin("m"));

                RecordStream rs = session.query(set.id(1))
                        .bin("result").selectFrom(removeNone)
                        .execute();
                Record rec = rs.getFirst().orElseThrow().recordOrThrow();
                Object result = rec.bins.get("result");
                System.out.println("Actual:   " + result);
                System.out.println("Type:     " + (result == null ? "null" : result.getClass().getName()));
            } catch (Exception e) {
                System.out.println("ERROR:    " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println();

            // --- Test 2: returnType = INVERTED ---
            System.out.println("=== Test 2: MapExp.removeByKeyRange(INVERTED, \"b\", \"e\") ===");
            System.out.println("Javadoc says INVERTED is valid. Inverted means remove everything OUTSIDE the range.");
            System.out.println("Expected: remove {a=1, e=5} (outside range b..e), leaving {b=2, c=3, d=4}");
            try {
                Exp removeInverted = MapExp.removeByKeyRange(
                        MapReturnType.INVERTED,
                        Exp.val("b"), Exp.val("e"),
                        Exp.mapBin("m"));

                RecordStream rs = session.query(set.id(1))
                        .bin("result").selectFrom(removeInverted)
                        .execute();
                Record rec = rs.getFirst().orElseThrow().recordOrThrow();
                Object result = rec.bins.get("result");
                System.out.println("Actual:   " + result);
                System.out.println("Type:     " + (result == null ? "null" : result.getClass().getName()));
            } catch (Exception e) {
                System.out.println("ERROR:    " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println();

            // --- Test 3: returnType = COUNT (Javadoc says this is NOT valid) ---
            System.out.println("=== Test 3: MapExp.removeByKeyRange(COUNT, \"b\", \"e\") ===");
            System.out.println("Javadoc says only NONE and INVERTED are valid.");
            System.out.println("If the doc is wrong, this might return the count of removed items (3).");
            try {
                Exp removeCount = MapExp.removeByKeyRange(
                        MapReturnType.COUNT,
                        Exp.val("b"), Exp.val("e"),
                        Exp.mapBin("m"));

                RecordStream rs = session.query(set.id(1))
                        .bin("result").selectFrom(removeCount)
                        .execute();
                Record rec = rs.getFirst().orElseThrow().recordOrThrow();
                Object result = rec.bins.get("result");
                System.out.println("Actual:   " + result);
                System.out.println("Type:     " + (result == null ? "null" : result.getClass().getName()));
            } catch (Exception e) {
                System.out.println("ERROR:    " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println();

            // --- Test 4: returnType = KEY (Javadoc says this is NOT valid) ---
            System.out.println("=== Test 4: MapExp.removeByKeyRange(KEY, \"b\", \"e\") ===");
            System.out.println("If the doc is wrong, this might return the removed keys [b, c, d].");
            try {
                Exp removeKey = MapExp.removeByKeyRange(
                        MapReturnType.KEY,
                        Exp.val("b"), Exp.val("e"),
                        Exp.mapBin("m"));

                RecordStream rs = session.query(set.id(1))
                        .bin("result").selectFrom(removeKey)
                        .execute();
                Record rec = rs.getFirst().orElseThrow().recordOrThrow();
                Object result = rec.bins.get("result");
                System.out.println("Actual:   " + result);
                System.out.println("Type:     " + (result == null ? "null" : result.getClass().getName()));
            } catch (Exception e) {
                System.out.println("ERROR:    " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println();

            // --- Test 5: returnType = VALUE (Javadoc says this is NOT valid) ---
            System.out.println("=== Test 5: MapExp.removeByKeyRange(VALUE, \"b\", \"e\") ===");
            System.out.println("If the doc is wrong, this might return the removed values [2, 3, 4].");
            try {
                Exp removeValue = MapExp.removeByKeyRange(
                        MapReturnType.VALUE,
                        Exp.val("b"), Exp.val("e"),
                        Exp.mapBin("m"));

                RecordStream rs = session.query(set.id(1))
                        .bin("result").selectFrom(removeValue)
                        .execute();
                Record rec = rs.getFirst().orElseThrow().recordOrThrow();
                Object result = rec.bins.get("result");
                System.out.println("Actual:   " + result);
                System.out.println("Type:     " + (result == null ? "null" : result.getClass().getName()));
            } catch (Exception e) {
                System.out.println("ERROR:    " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println();

            // --- Test 6: returnType = KEY_VALUE (Javadoc says this is NOT valid) ---
            System.out.println("=== Test 6: MapExp.removeByKeyRange(KEY_VALUE, \"b\", \"e\") ===");
            System.out.println("If the doc is wrong, this might return the removed entries {b=2, c=3, d=4}.");
            try {
                Exp removeKV = MapExp.removeByKeyRange(
                        MapReturnType.KEY_VALUE,
                        Exp.val("b"), Exp.val("e"),
                        Exp.mapBin("m"));

                RecordStream rs = session.query(set.id(1))
                        .bin("result").selectFrom(removeKV)
                        .execute();
                Record rec = rs.getFirst().orElseThrow().recordOrThrow();
                Object result = rec.bins.get("result");
                System.out.println("Actual:   " + result);
                System.out.println("Type:     " + (result == null ? "null" : result.getClass().getName()));
            } catch (Exception e) {
                System.out.println("ERROR:    " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            System.out.println();

            // Verify original record is unchanged (read expressions don't mutate)
            System.out.println("=== Verify original map is unchanged ===");
            RecordStream rs = session.query(set.id(1)).execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            System.out.println("Original map after all tests: " + rec.bins.get("m"));
        }
    }
}
