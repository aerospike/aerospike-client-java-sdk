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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;

/**
 * Demonstrates incongruities between the AEL spec and its implementation.
 *
 * <p>Each test method targets a specific issue identified in the spec review:
 * <ul>
 *   <li>2a: {@code let...then} vs {@code let...then} keyword mismatch</li>
 *   <li>2b: NAME_IDENTIFIER accepts digit-starting tokens, causing integer map key confusion</li>
 *   <li>2c: {@code >>} operator performs logical right shift instead of arithmetic</li>
 *   <li>2d: {@code exists()} path function silently ignored by visitor</li>
 *   <li>2e: Mutation path functions (sort, remove, etc.) silently ignored</li>
 * </ul>
 */
public class OperationDifferences {

    private static final String SEPARATOR = "=".repeat(70);
    private static final String PASS = "PASS";
    private static final String FAIL = "** FAIL **";

    private static int totalTests = 0;
    private static int failedTests = 0;

    public static void main(String[] args) throws Exception {
        try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet set = DataSet.of("test", "ael_diff_test");

            session.truncate(set);
            setupTestData(session, set);

//            test2a_LetThenVsWithDo(session, set);
            test2b_NameIdentifierTooPermissive(session, set);
            test2c_RightShiftReversed(session, set);
            test2d_ExistsSilentlyIgnored(session, set);
            test2e_MutationOperationsIgnored(session, set);
            testCasting_AsIntAsFloat(session, set);

            System.out.println(SEPARATOR);
            System.out.printf("SUMMARY: %d/%d tests show spec/implementation differences%n",
                    failedTests, totalTests);
            System.out.println(SEPARATOR);
        }
    }

    static void setupTestData(Session session, DataSet set) {
        // Record 1: integer bin for shift operator tests
        session.upsert(set.id(1))
                .bin("intBin").setTo(-8)
                .bin("posInt").setTo(16)
                .execute();

        // Record 2: map bin with integer keys only.
        // $.m.1 per the spec should look up integer key 1.
        // The AEL treats bare `1` as NAME_IDENTIFIER (string), so it looks up string key "1".
        Map<Object, Object> intKeyMap = new HashMap<>();
        intKeyMap.put(1L, "val_from_int_key_1");
        intKeyMap.put(2L, "val_from_int_key_2");
        intKeyMap.put(3L, "val_from_int_key_3");
        session.upsert(set.id(2))
                .bin("m").setTo(intKeyMap)
                .execute();

        // Record 3: map with both integer key 1 and string key "1" for contrast
        session.upsert(set.id(3))
                .bin("m").onMapKey(1L).setTo("INTEGER_KEY_1")
                .bin("m").onMapKey("1").setTo("STRING_KEY_1")
                .bin("m").onMapKey("name").setTo("hello")
                .execute();

        // Record 4: bins for exists() test -- has binA but NOT binB
        session.upsert(set.id(4))
                .bin("binA").setTo(42)
                .bin("flag").setTo(true)
                .execute();

        // Record 5: list bin for mutation operation tests
        session.upsert(set.id(5))
                .bin("listBin").setTo(List.of(50L, 10L, 40L, 20L, 30L))
                .execute();

        // Record 6: bins for let/then test
        session.upsert(set.id(6))
                .bin("x").setTo(10)
                .bin("y").setTo(20)
                .execute();

        // Record 7: int and float bins for asInt()/asFloat() casting tests
        session.upsert(set.id(7))
                .bin("intBin").setTo(10)
                .bin("floatBin").setTo(3.5)
                .execute();
    }

    // =========================================================================
    // 2a: Spec says `let...then`, implementation uses `let...then`
    // =========================================================================
    static void test2a_LetThenVsWithDo(Session session, DataSet set) {
        System.out.println(SEPARATOR);
        System.out.println("TEST 2a: Spec keyword 'let...then' vs implementation 'let...then'");
        System.out.println(SEPARATOR);
        System.out.println();
        System.out.println("Spec defines:     let ('x' = 1, 'y' = ${x} + 1) then (${y} + ${x})");
        System.out.println("Implementation:   let ('x' = 1, 'y' = ${x} + 1) then (${y} + ${x})");
        System.out.println();

        // let...then should work (current implementation)
        System.out.println("  [A] Testing let...then (implementation keyword):");
        try {
            RecordStream rs = session.query(set.id(6))
                    .bin("result").selectFrom("let ('x' = $.x, 'y' = $.y) then (${x} + ${y})")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            long result = rec.getLong("result");
            System.out.println("      Expression:  let ('x' = $.x, 'y' = $.y) then (${x} + ${y})");
            System.out.println("      Expected:    30");
            System.out.println("      Actual:      " + result);
            check("2a-let-then", result == 30, "let...then produces correct result");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2a-let-then", false, "let...then should work");
        }

        System.out.println();

        // let...then should work per spec, but fails because `let` and `then` are not keywords
        System.out.println("  [B] Testing let...then (spec keyword):");
        try {
            RecordStream rs = session.query(set.id(6))
                    .bin("result").selectFrom("let ('x' = $.x, 'y' = $.y) then (${x} + ${y})")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            long result = rec.getLong("result");
            System.out.println("      Expression:  let ('x' = $.x, 'y' = $.y) then (${x} + ${y})");
            System.out.println("      Expected:    30 (should work per spec)");
            System.out.println("      Actual:      " + result);
            check("2a-let-then", true, "let...then works per spec");
        } catch (Exception e) {
            System.out.println("      Expression:  let ('x' = $.x, 'y' = $.y) then (${x} + ${y})");
            System.out.println("      Expected:    30 (should work per spec)");
            System.out.println("      Actual:      " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2a-let-then", false, "let...then fails -- not implemented");
        }
        System.out.println();
    }

    // =========================================================================
    // 2b: NAME_IDENTIFIER allows digit-starting tokens, so $.m.1 uses
    //     string key "1" instead of integer key 1
    // =========================================================================
    static void test2b_NameIdentifierTooPermissive(Session session, DataSet set) {
        System.out.println(SEPARATOR);
        System.out.println("TEST 2b: NAME_IDENTIFIER treats bare integers as string map keys");
        System.out.println(SEPARATOR);
        System.out.println();
        System.out.println("Grammar defines: NAME_IDENTIFIER: [a-zA-Z0-9_]+");
        System.out.println("Spec says:       Identifiers must match ^[A-Za-z]\\w*$ (start with letter)");
        System.out.println("Impact:          $.m.1 looks up string key \"1\" instead of integer key 1");
        System.out.println();

        // Record 2 has map with integer keys {1: "val_from_int_key_1", ...}
        // $.m.1 should look up integer key 1 per the spec
        System.out.println("  [A] Map with only integer keys: {1: \"val_from_int_key_1\", 2: ...}");
        System.out.println("      $.m.1 per spec should access integer key 1");
        try {
            RecordStream rs = session.query(set.id(2))
                    .bin("result").selectFrom("$.m.1.get(type: STRING)")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            String result = rec.getString("result");
            System.out.println("      Expected:    val_from_int_key_1 (integer key 1)");
            System.out.println("      Actual:      " + (result == null ? "null (key not found)" : result));
            check("2b-int-key-lookup", "val_from_int_key_1".equals(result),
                    "AEL looks up string key \"1\" instead of integer key 1");
        } catch (Exception e) {
            System.out.println("      Expected:    val_from_int_key_1");
            System.out.println("      Actual:      " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2b-int-key-lookup", false, "failed to access integer key");
        }

        System.out.println();

        // Record 3 has both integer key 1 and string key "1" with different values
        System.out.println("  [B] Map with both integer key 1 and string key \"1\":");
        System.out.println("      {1(int): \"INTEGER_KEY_1\", \"1\"(str): \"STRING_KEY_1\"}");
        try {
            RecordStream rs = session.query(set.id(3))
                    .bin("result").selectFrom("$.m.1.get(type: STRING)")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            String result = rec.getString("result");
            System.out.println("      $.m.1 per spec should return: INTEGER_KEY_1 (integer key 1)");
            System.out.println("      $.m.1 actually returns:       " + result);
            check("2b-ambiguous-key", "INTEGER_KEY_1".equals(result),
                    "AEL resolves to string key instead of integer key");
        } catch (Exception e) {
            System.out.println("      Actual: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2b-ambiguous-key", false, "failed to access key");
        }
        System.out.println();
    }

    // =========================================================================
    // 2c: >> operator performs logical right shift (zero-fill) instead of
    //     arithmetic right shift (sign-preserving) as per spec and Java convention
    // =========================================================================
    static void test2c_RightShiftReversed(Session session, DataSet set) {
        System.out.println(SEPARATOR);
        System.out.println("TEST 2c: >> operator semantics reversed");
        System.out.println(SEPARATOR);
        System.out.println();
        System.out.println("Java and spec convention:");
        System.out.println("  >>  = arithmetic right shift (sign-preserving)");
        System.out.println("  >>> = logical right shift (zero-fill)");
        System.out.println("AEL implementation maps >> to Exp.rshift() which is logical right shift.");
        System.out.println();

        // -8 in binary (64-bit): 1111...1111 1000
        // Arithmetic >> 1:       1111...1111 1100 = -4  (sign bit preserved)
        // Logical   >>> 1:       0111...1111 1100 = 9223372036854775804 (huge positive)
        long expectedArithmetic = -8L >> 1;   // -4 in Java
        long expectedLogical = -8L >>> 1;     // 9223372036854775804 in Java

        System.out.println("  [A] Negative number: -8 >> 1");
        System.out.printf("      Java -8L >> 1  (arithmetic): %d%n", expectedArithmetic);
        System.out.printf("      Java -8L >>> 1 (logical):    %d%n", expectedLogical);
        try {
            RecordStream rs = session.query(set.id(1))
                    .bin("result").selectFrom("$.intBin >> 1")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            long result = rec.getLong("result");
            System.out.println("      Expected (spec):     " + expectedArithmetic + " (arithmetic, sign preserved)");
            System.out.println("      Actual AEL >> :      " + result);
            boolean isCorrect = (result == expectedArithmetic);
            if (!isCorrect && result == expectedLogical) {
                System.out.println("      Matches logical >>> : yes -- >> is wired to Exp.rshift() (logical)");
                System.out.println("                            Should be wired to Exp.arshift() (arithmetic)");
            }
            check("2c-rshift-negative", isCorrect,
                    ">> performs logical right shift instead of arithmetic");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2c-rshift-negative", false, "right shift evaluation failed");
        }

        System.out.println();

        // Positive numbers: arithmetic and logical right shift produce the same result,
        // so this should work correctly either way (sanity check)
        long posExpected = 16L >> 1; // 8
        System.out.println("  [B] Positive number (sanity check): 16 >> 1");
        try {
            RecordStream rs = session.query(set.id(1))
                    .bin("result").selectFrom("$.posInt >> 1")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            long result = rec.getLong("result");
            System.out.println("      Expected:    " + posExpected);
            System.out.println("      Actual:      " + result);
            System.out.println("      Note: positive values shift identically for both variants");
            check("2c-rshift-positive", result == posExpected, "positive shift works");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2c-rshift-positive", false, "positive right shift failed");
        }

        System.out.println();

        // >>> (logical right shift) is defined in the spec but not in the grammar.
        // The grammar only defines >>. So >>> should fail to parse.
        System.out.println("  [C] Testing >>> (logical right shift from spec):");
        System.out.println("      Spec defines >>> as logical right shift, but the grammar only has >>.");
        System.out.println("      Attempting: $.intBin >>> 1");
        System.out.printf("      Java -8L >>> 1 = %d%n", expectedLogical);
        try {
            RecordStream rs = session.query(set.id(1))
                    .bin("result").selectFrom("$.intBin >>> 1")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            long result = rec.getLong("result");
            System.out.println("      Expected (spec):     " + expectedLogical + " (logical, zero-fill)");
            System.out.println("      Actual AEL >>> :     " + result);
            check("2c-logical-rshift", result == expectedLogical,
                    ">>> should be supported per spec");
        } catch (Exception e) {
            System.out.println("      Expected (spec):     " + expectedLogical + " (logical, zero-fill)");
            System.out.println("      Actual:              " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("      >>> is not in the grammar -- AEL only has >> (wired to logical)");
            System.out.println("      Net result: >> does logical when it should do arithmetic,");
            System.out.println("                  and >>> doesn't exist at all.");
            check("2c-logical-rshift", false,
                    ">>> not supported -- grammar only defines >>");
        }
        System.out.println();
    }

    // =========================================================================
    // 2d: exists() path function is in the grammar but the visitor does not
    //     implement visitPathFunctionExists, so it silently falls through
    //     and the exists() call is dropped from the expression.
    // =========================================================================
    static void test2d_ExistsSilentlyIgnored(Session session, DataSet set) {
        System.out.println(SEPARATOR);
        System.out.println("TEST 2d: exists() path function silently ignored");
        System.out.println(SEPARATOR);
        System.out.println();
        System.out.println("Record 4 has: binA=42, flag=true (binB does NOT exist)");
        System.out.println("$.binA.exists() should evaluate to true (bin exists)");
        System.out.println("$.binB.exists() should evaluate to false (bin missing)");
        System.out.println("But exists() is not implemented -- the visitor drops it,");
        System.out.println("so the expression degrades to just reading the bin value.");
        System.out.println();

        // Test A: $.binA.exists() used in a where clause with `and true`
        // If exists() worked: filter = true (binA exists) AND true = true -> record returned
        // With exists() dropped: filter = $.binA (as bool) AND true
        //   binA=42 is not boolean, so this relies on truthy int->bool conversion
        System.out.println("  [A] Filter: $.binA.exists() and $.flag");
        System.out.println("      Per spec: exists() checks bin existence -> true, combined with flag -> passes");
        System.out.println("      Actual:   exists() dropped; becomes $.binA and $.flag");
        System.out.println("                binA=42 treated as boolean (truthy), so filter passes but for wrong reason");
        try {
            RecordStream rs = session.query(set.id(4))
                    .where("$.binA.exists() and $.flag")
                    .execute();
            boolean found = rs.getFirst().isPresent();
            System.out.println("      Filter passed: " + found);
            System.out.println("      Note: appears correct, but exists() was silently dropped.");
            System.out.println("            The filter works only because int 42 is truthy as a bool.");
            check("2d-exists-present", true, "result looks correct but for the wrong reason");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2d-exists-present", false, "exists on present bin errored");
        }

        System.out.println();

        // Test B: $.binB.exists() where binB does NOT exist
        // If exists() worked: should evaluate to false
        // With exists() dropped: expression becomes $.binB which reads a missing bin
        System.out.println("  [B] Filter: $.binB.exists()");
        System.out.println("      Per spec: should evaluate to false (binB does not exist)");
        System.out.println("      Actual:   exists() dropped; becomes $.binB as boolean read.");
        System.out.println("                Since binB doesn't exist, this will error on the server.");
        try {
            RecordStream rs = session.query(set.id(4))
                    .where("$.binB.exists()")
                    .execute();
            boolean found = rs.getFirst().isPresent();
            System.out.println("      Filter passed: " + found);
            if (!found) {
                System.out.println("      Record filtered out - but was it because exists() returned false,");
                System.out.println("      or because reading missing bin as bool filtered the record?");
            }
            check("2d-exists-missing", false,
                    "cannot distinguish exists()==false from missing-bin filter behavior");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("      This error occurs because exists() is silently ignored and");
            System.out.println("      the server tries to read non-existent binB as a boolean.");
            check("2d-exists-missing", false, "exists() on missing bin causes error instead of returning false");
        }

        System.out.println();

        // Test C: Use exists() in a read expression to show it returns the bin value, not a boolean
        System.out.println("  [C] Read expression: $.binA.exists()");
        System.out.println("      Per spec: should return true (boolean)");
        System.out.println("      Actual:   exists() dropped; returns raw bin value 42");
        try {
            RecordStream rs = session.query(set.id(4))
                    .bin("result").selectFrom("$.binA.exists()")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    true (boolean)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            boolean isBoolean = result instanceof Boolean;
            check("2d-exists-read-expr", isBoolean && Boolean.TRUE.equals(result),
                    "returns raw bin value instead of boolean existence check");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("2d-exists-read-expr", false, "exists() as read expression failed");
        }
        System.out.println();
    }

    // =========================================================================
    // 2e: Mutation path functions (remove, sort, clear, insert, set, append,
    //     increment) are defined in the grammar but have no visitor
    //     implementation -- they are silently ignored or cause errors.
    // =========================================================================
    static void test2e_MutationOperationsIgnored(Session session, DataSet set) {
        System.out.println(SEPARATOR);
        System.out.println("TEST 2e: Mutation path functions silently ignored");
        System.out.println(SEPARATOR);
        System.out.println();
        System.out.println("Record 5 has: listBin = [50, 10, 40, 20, 30]");
        System.out.println("Path functions remove(), sort(), clear(), insert(), set(),");
        System.out.println("append(), increment() are in the grammar but have no visitor");
        System.out.println("implementation. They should perform CDT mutations but are dropped.");
        System.out.println();

        // Show original data
        try {
            RecordStream rs = session.query(set.id(5)).execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            System.out.println("  Original listBin: " + rec.getList("listBin"));
        } catch (Exception e) {
            System.out.println("  Could not read original data: " + e.getMessage());
        }
        System.out.println();

        // Test A: sort() via write expression
        System.out.println("  [A] Write expression: $.listBin.[].sort()");
        System.out.println("      Per spec: should sort the list in place");
        try {
            session.upsert(set.id(5))
                    .bin("sortedList").upsertFrom("$.listBin.[].sort()")
                    .execute();

            RecordStream rs = session.query(set.id(5)).execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object sortedResult = rec.bins.get("sortedList");
            List<?> originalList = rec.getList("listBin");
            System.out.println("      Original:    " + originalList);
            System.out.println("      Expected:    [10, 20, 30, 40, 50] (sorted list)");
            System.out.println("      Actual:      " + sortedResult);

            boolean isSorted = sortedResult instanceof List<?> sorted
                    && sorted.size() == 5
                    && sorted.get(0).equals(10L);
            check("2e-sort", isSorted, "sort() has no visitor implementation");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("      sort() is in the grammar but not implemented in the visitor.");
            check("2e-sort", false, "sort() not implemented");
        }

        System.out.println();

        // Test B: remove() via write expression
        System.out.println("  [B] Write expression: $.listBin.[=30].remove()");
        System.out.println("      Per spec: should remove elements with value 30 from list");
        try {
            session.upsert(set.id(5))
                    .bin("removedList").upsertFrom("$.listBin.[=30].remove()")
                    .execute();

            RecordStream rs = session.query(set.id(5)).execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object removedResult = rec.bins.get("removedList");
            System.out.println("      Expected:    [50, 10, 40, 20] (list without 30)");
            System.out.println("      Actual:      " + removedResult);

            boolean isRemoved = removedResult instanceof List<?> list
                    && list.size() == 4;
            check("2e-remove", isRemoved, "remove() has no visitor implementation");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("      remove() is in the grammar but not implemented in the visitor.");
            check("2e-remove", false, "remove() not implemented");
        }

        System.out.println();

        // Test C: clear() via write expression
        System.out.println("  [C] Write expression: $.listBin.[].clear()");
        System.out.println("      Per spec: should clear all items from the list");
        try {
            session.upsert(set.id(5))
                    .bin("clearedList").upsertFrom("$.listBin.[].clear()")
                    .execute();

            RecordStream rs = session.query(set.id(5)).execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object clearedResult = rec.bins.get("clearedList");
            System.out.println("      Expected:    [] (empty list)");
            System.out.println("      Actual:      " + clearedResult);

            boolean isCleared = clearedResult instanceof List<?> list && list.isEmpty();
            check("2e-clear", isCleared, "clear() has no visitor implementation");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("      clear() is in the grammar but not implemented in the visitor.");
            check("2e-clear", false, "clear() not implemented");
        }

        System.out.println();

        // Verify original data is unchanged (mutations should have affected it but didn't)
        System.out.println("  [D] Verify original listBin is unchanged (mutations had no effect):");
        try {
            RecordStream rs = session.query(set.id(5)).execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            List<?> listBin = rec.getList("listBin");
            System.out.println("      listBin now:  " + listBin);
            System.out.println("      Original was: [50, 10, 40, 20, 30]");
            boolean unchanged = listBin != null && listBin.size() == 5;
            if (unchanged) {
                System.out.println("      Original data is unchanged -- confirms mutations are no-ops");
            }
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getMessage());
        }
        System.out.println();
    }

    // =========================================================================
    // Point 12 (AI rules): asInt() and asFloat() casting for mixed-type
    //     arithmetic. Both operands must be the same type; the spec provides
    //     asInt() and asFloat() path functions for explicit conversion.
    // =========================================================================
    static void testCasting_AsIntAsFloat(Session session, DataSet set) {
        System.out.println(SEPARATOR);
        System.out.println("TEST: asInt() / asFloat() type casting for arithmetic");
        System.out.println(SEPARATOR);
        System.out.println();
        System.out.println("Record 7 has: intBin=10 (INT), floatBin=3.5 (FLOAT)");
        System.out.println("Spec rule 12: Both operands in arithmetic must be the same type.");
        System.out.println("Use asInt() or asFloat() to convert before combining.");
        System.out.println();

        // Test A: Mixed-type addition without casting
        // Aerospike expressions require operands of the same type for arithmetic.
        // Adding INT + FLOAT without casting should fail or produce an error.
        System.out.println("  [A] Mixed types without casting: $.intBin + $.floatBin");
        System.out.println("      intBin is INT (10), floatBin is FLOAT (3.5)");
        System.out.println("      Per spec: both operands must be the same type -> this should error");
        try {
            RecordStream rs = session.query(set.id(7))
                    .bin("result").selectFrom("$.intBin + $.floatBin")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    error (type mismatch)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            System.out.println("      If no error: the AEL may be silently promoting types");
            check("cast-mixed-no-cast", false, "mixed-type arithmetic should require explicit cast");
        } catch (Exception e) {
            System.out.println("      Actual:      " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("      Correct! Mixed-type arithmetic without casting produces an error.");
            check("cast-mixed-no-cast", true, "mixed types correctly rejected without cast");
        }

        System.out.println();

        // Test B: Convert float to int with asInt(), then do integer arithmetic
        // $.intBin + $.floatBin.asInt() -> 10 + 3 = 13 (truncated)
        System.out.println("  [B] Float-to-int cast: $.intBin + $.floatBin.asInt()");
        System.out.println("      floatBin (3.5) cast to int -> 3 (truncated)");
        System.out.println("      Expected result: 10 + 3 = 13");
        try {
            RecordStream rs = session.query(set.id(7))
                    .bin("result").selectFrom("$.intBin + $.floatBin.asInt()")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    13 (integer)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            boolean correct = result instanceof Long && (Long) result == 13L;
            check("cast-asInt", correct, "asInt() casts float to int for arithmetic");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("cast-asInt", false, "asInt() cast failed");
        }

        System.out.println();

        // Test C: Convert int to float with asFloat(), then do float arithmetic
        // $.intBin.asFloat() + $.floatBin -> 10.0 + 3.5 = 13.5
        System.out.println("  [C] Int-to-float cast: $.intBin.asFloat() + $.floatBin");
        System.out.println("      intBin (10) cast to float -> 10.0");
        System.out.println("      Expected result: 10.0 + 3.5 = 13.5");
        try {
            RecordStream rs = session.query(set.id(7))
                    .bin("result").selectFrom("$.intBin.asFloat() + $.floatBin")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    13.5 (double)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            boolean correct = result instanceof Double && Math.abs((Double) result - 13.5) < 0.001;
            check("cast-asFloat", correct, "asFloat() casts int to float for arithmetic");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("cast-asFloat", false, "asFloat() cast failed");
        }

        System.out.println();

        // Test D: Both casts on the same value for comparison
        // $.floatBin.asInt().asFloat() -> 3.5 -> 3 -> 3.0 (round-trip loses precision)
        System.out.println("  [D] Round-trip cast: $.floatBin.asInt().asFloat()");
        System.out.println("      3.5 -> asInt() -> 3 -> asFloat() -> 3.0");
        System.out.println("      Demonstrates precision loss from truncation");
        try {
            RecordStream rs = session.query(set.id(7))
                    .bin("result").selectFrom("$.floatBin.asInt().asFloat()")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    3.0 (precision lost from 3.5)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            boolean correct = result instanceof Double && Math.abs((Double) result - 3.0) < 0.001;
            check("cast-round-trip", correct, "round-trip cast loses fractional part");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("cast-round-trip", false, "round-trip cast failed");
        }

        System.out.println();

        // Test E: asInt() on an already-integer value (no-op)
        System.out.println("  [E] No-op cast: $.intBin.asInt()");
        System.out.println("      Casting int to int should be a no-op");
        try {
            RecordStream rs = session.query(set.id(7))
                    .bin("result").selectFrom("$.intBin.asInt()")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    10 (unchanged)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            boolean correct = result instanceof Long && (Long) result == 10L;
            check("cast-noop-int", correct, "asInt() on int is a no-op");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("cast-noop-int", false, "asInt() on int failed");
        }

        System.out.println();

        // Test F: asFloat() on an already-float value (no-op)
        System.out.println("  [F] No-op cast: $.floatBin.asFloat()");
        System.out.println("      Casting float to float should be a no-op");
        try {
            RecordStream rs = session.query(set.id(7))
                    .bin("result").selectFrom("$.floatBin.asFloat()")
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object result = rec.bins.get("result");
            System.out.println("      Expected:    3.5 (unchanged)");
            System.out.println("      Actual:      " + result + " (type: " + (result == null ? "null" : result.getClass().getSimpleName()) + ")");
            boolean correct = result instanceof Double && Math.abs((Double) result - 3.5) < 0.001;
            check("cast-noop-float", correct, "asFloat() on float is a no-op");
        } catch (Exception e) {
            System.out.println("      ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            check("cast-noop-float", false, "asFloat() on float failed");
        }
        System.out.println();
    }

    private static void check(String testId, boolean passed, String description) {
        totalTests++;
        String status;
        if (passed) {
            status = PASS;
        } else {
            status = FAIL;
            failedTests++;
        }
        System.out.printf("      [%s] %s - %s%n", status, testId, description);
    }
}
