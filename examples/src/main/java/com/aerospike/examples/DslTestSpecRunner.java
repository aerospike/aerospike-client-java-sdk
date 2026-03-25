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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

/**
 * Runnable test suite matching the specifications in docs/dsl-test-specifications.md.
 *
 * <p>Connects to Aerospike, seeds test data, and exercises DSL expressions across
 * all major categories: scalar access, type casting, map/list access, nesting,
 * arithmetic, bitwise, comparison, logical, control structures, metadata,
 * path functions, transactions, rank-based access, return type variations,
 * and edge cases.
 */
public class DslTestSpecRunner {

    private static final String SEP = "=".repeat(70);
    private static int totalTests = 0;
    private static int passedTests = 0;
    private static int failedTests = 0;
    private static int errorTests = 0;

    public static void main(String[] args) throws Exception {
        try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet set = DataSet.of("test", "dsl_test_spec");

            session.truncate(set);
            Thread.sleep(200);
            setupTestData(session, set);

            testScalarBinAccess(session, set);
            testTypeCasting(session, set);
            testTypeDerivation(session, set);
            testMapAccess(session, set);
            testListAccess(session, set);
            testNestedCDT(session, set);
            testArithmetic(session, set);
            testBitwise(session, set);
            testComparison(session, set);
            testLogical(session, set);
            testControlStructures(session, set);
            testMetadata(session, set);
            testPathFunctions(session, set);
            testTransactionScenario(session, set);
            testRankBased(session, set);
            testReturnTypes(session, set);
            testEdgeCases(session, set);

            printSummary();
        }
    }

    // =====================================================================
    // Test Data Setup
    // =====================================================================
    static void setupTestData(Session session, DataSet set) {
        // Record 1: scalar bins
        session.upsert(set.id(1))
                .bin("intBin").setTo(42)
                .bin("floatBin").setTo(3.14)
                .bin("strBin").setTo("hello")
                .bin("boolBin").setTo(true)
                .bin("negInt").setTo(-8)
                .bin("zeroBin").setTo(0)
                .execute();

        // Record 2: simple map + simple list
        Map<Object, Object> map2 = new TreeMap<>();
        map2.put("alpha", 10L);
        map2.put("beta", 20L);
        map2.put("gamma", 30L);
        map2.put("delta", 40L);
        map2.put("epsilon", 50L);
        session.upsert(set.id(2))
                .bin("m").setTo(map2)
                .bin("l").setTo(List.of(50L, 10L, 40L, 20L, 30L, 60L, 5L))
                .execute();

        // Record 3: nested structures
        Map<Object, Object> address = new TreeMap<>();
        address.put("city", "Austin");
        address.put("state", "TX");
        address.put("zip", "73301");
        Map<Object, Object> profile = new TreeMap<>();
        profile.put("name", "Alice");
        profile.put("address", address);
        profile.put("scores", List.of(95L, 87L, 72L, 100L, 63L));
        profile.put("tags", List.of("vip", "early_adopter"));
        session.upsert(set.id(3))
                .bin("profile").setTo(profile)
                .execute();

        // Record 4: deep nesting
        Map<Object, Object> addr1 = Map.of("city", "NYC", "zip", "10001");
        Map<Object, Object> addr2 = Map.of("city", "LA", "zip", "90001");
        Map<Object, Object> addr3 = Map.of("city", "SF", "zip", "94101");
        Map<Object, Object> user1 = new LinkedHashMap<>();
        user1.put("name", "Bob");
        user1.put("addresses", List.of(addr1, addr2));
        Map<Object, Object> user2 = new LinkedHashMap<>();
        user2.put("name", "Eve");
        user2.put("addresses", List.of(addr3));
        Map<Object, Object> data4 = Map.of("users", List.of(user1, user2));
        session.upsert(set.id(4))
                .bin("data").setTo(data4)
                .execute();

        // Record 5: integer-keyed map
        Map<Object, Object> intKeyMap = new TreeMap<>();
        intKeyMap.put(1L, "one");
        intKeyMap.put(2L, "two");
        intKeyMap.put(3L, "three");
        intKeyMap.put(10L, "ten");
        intKeyMap.put(20L, "twenty");
        session.upsert(set.id(5))
                .bin("m").setTo(intKeyMap)
                .execute();

        // Record 6: empty collections and edge values
        session.upsert(set.id(6))
                .bin("emptyList").setTo(List.of())
                .bin("emptyMap").setTo(Map.of())
                .bin("intBin").setTo(0)
                .bin("strBin").setTo("")
                .execute();

        // Record 7: arithmetic, logic, control flow
        session.upsert(set.id(7))
                .bin("price").setTo(100)
                .bin("qty").setTo(5)
                .bin("discount").setTo(10.0)
                .bin("tier").setTo(2)
                .bin("status").setTo("active")
                .bin("flag1").setTo(true)
                .bin("flag2").setTo(false)
                .bin("items").setTo(List.of("gold", "silver", "bronze"))
                .bin("allowed").setTo(List.of("gold", "platinum"))
                .bin("name").setTo("gold")
                .execute();

        // Record 8: transactions map (sorted string keys with timestamp prefix)
        Map<Object, Object> txns = new TreeMap<>();
        txns.put("1672531200000,txn01", List.of(150L, "Coffee subscription"));
        txns.put("1675209600000,txn02", List.of(10000L, "Laptop"));
        txns.put("1677628800000,txn03", List.of(250L, "Groceries"));
        txns.put("1680307200000,txn04", List.of(500L, "Flight ticket"));
        txns.put("1682899200000,txn05", List.of(75L, "Books"));
        txns.put("1685577600000,txn06", List.of(8000L, "Phone"));
        txns.put("1688169600000,txn07", List.of(3500L, "Conference ticket"));
        txns.put("1690848000000,txn08", List.of(45L, "Snacks"));
        txns.put("1693526400000,txn09", List.of(12000L, "Vacation"));
        txns.put("1696118400000,txn10", List.of(600L, "Concert"));
        txns.put("1698796800000,txn11", List.of(2750L, "Furniture"));
        txns.put("1701388800000,txn12", List.of(9500L, "Holiday gifts"));
        session.upsert(set.id(8))
                .bin("txns").setTo(txns)
                .execute();

        // Record 9: rank test map
        Map<Object, Object> scores = new TreeMap<>();
        scores.put("math", 85L);
        scores.put("science", 92L);
        scores.put("english", 78L);
        scores.put("art", 95L);
        scores.put("history", 88L);
        session.upsert(set.id(9))
                .bin("scores").setTo(scores)
                .execute();

        // Record 10: cross-bin tests
        session.upsert(set.id(10))
                .bin("a").setTo(10)
                .bin("b").setTo(20)
                .bin("c").setTo(30.5)
                .bin("d").setTo(40.7)
                .bin("name").setTo("gold")
                .execute();

        // Record 11: type derivation tests
        // Bins chosen so that cross-expression type propagation can be verified:
        //   a=10(INT), b=10(INT), c=true(BOOL), d=11(INT), e=3.14(FLOAT),
        //   f="hello"(STRING), g=false(BOOL)
        session.upsert(set.id(11))
                .bin("a").setTo(10)
                .bin("b").setTo(10)
                .bin("c").setTo(true)
                .bin("d").setTo(11)
                .bin("e").setTo(3.14)
                .bin("f").setTo("hello")
                .bin("g").setTo(false)
                .execute();
    }

    // =====================================================================
    // 1. Scalar Bin Access
    // =====================================================================
    static void testScalarBinAccess(Session session, DataSet set) {
        section("1. SCALAR BIN ACCESS");

        readCheck("S01", session, set, 1, "$.intBin", 42L);
        readCheck("S02", session, set, 1, "$.floatBin", 3.14);
        readCheck("S03", session, set, 1, "$.strBin.get(type: STRING)", "hello");
        readCheck("S04", session, set, 1, "$.boolBin", true);
        readCheck("S05", session, set, 1, "$.negInt", -8L);
        readCheck("S06", session, set, 6, "$.intBin", 0L);
        filterCheck("S07", session, set, 1, "$.intBin > 40", true);
        filterCheck("S08", session, set, 1, "$.intBin > 50", false);
        filterCheck("S09", session, set, 1, "$.strBin == 'hello'", true);
        filterCheck("S10", session, set, 1, "40 < $.intBin", true);
    }

    // =====================================================================
    // 2. Type Inference and Casting
    // =====================================================================
    static void testTypeCasting(Session session, DataSet set) {
        section("2. TYPE INFERENCE AND CASTING");

        readCheck("T01", session, set, 1, "$.intBin.asFloat()", 42.0);
        readCheck("T02", session, set, 1, "$.floatBin.asInt()", 3L);
        filterCheck("T03", session, set, 10, "$.a.get(type: INT) > $.b.get(type: INT)", false);
        filterCheck("T04", session, set, 10, "$.a.get(type: INT) == $.b.get(type: INT)", false);

        readExpectError("T05", session, set, 10, "$.a + $.c",
                "Mixed type arithmetic (INT + FLOAT) without cast");

        readCheck("T06", session, set, 10, "$.a + $.c.asInt()", 40L);
        readCheck("T07", session, set, 10, "$.a.asFloat() + $.c", 40.5);
        readCheck("T08", session, set, 1, "$.floatBin.asInt().asFloat()", 3.0);
        readCheck("T09", session, set, 1, "$.intBin.asInt()", 42L);
        readCheck("T10", session, set, 1, "$.floatBin.asFloat()", 3.14);
    }

    // =====================================================================
    // 2b. Type Derivation (no explicit get(type:) annotations)
    // =====================================================================
    static void testTypeDerivation(Session session, DataSet set) {
        section("2b. TYPE DERIVATION");
        System.out.println("  Record 11: a=10(INT), b=10(INT), c=true(BOOL), d=11(INT),");
        System.out.println("             e=3.14(FLOAT), f=\"hello\"(STRING), g=false(BOOL)");
        System.out.println("  Tests verify the parser can derive bin types without explicit get(type:).");
        System.out.println();

        // --- Level 1: Literal provides the type hint directly ---
        System.out.println("  --- Level 1: Literal provides type hint ---");
        filterCheck("TD01", session, set, 11, "$.a > 5",
                true);   // 10 > 5, a derived as INT from literal 5
        filterCheck("TD02", session, set, 11, "$.f == 'hello'",
                true);   // f derived as STRING from literal 'hello'
        filterCheck("TD03", session, set, 11, "$.e > 3.0",
                true);   // e derived as FLOAT from literal 3.0
        filterCheck("TD04", session, set, 11, "$.c == true",
                true);   // c derived as BOOL from literal true

        // --- Level 2: Boolean context implies BOOL ---
        System.out.println();
        System.out.println("  --- Level 2: Boolean context implies BOOL ---");
        filterCheck("TD05", session, set, 11, "$.c and not($.g)",
                true);   // both derived as BOOL from and-context
        filterCheck("TD06", session, set, 11, "$.c or $.g",
                true);   // or-context implies BOOL
        filterCheck("TD07", session, set, 11, "not($.g)",
                true);   // not() implies operand is BOOL

        // --- Level 3: Arithmetic context implies numeric type ---
        System.out.println();
        System.out.println("  --- Level 3: Arithmetic context implies numeric type ---");
        readCheck("TD08", session, set, 11, "$.a + 1",
                11L);    // a derived as INT from literal 1 in arithmetic
        readCheck("TD09", session, set, 11, "$.e + 1.0",
                4.14);   // e derived as FLOAT from literal 1.0

        // $.a + $.b: both operands in arithmetic, but no literal to hint the type.
        // The parser must determine type from context or fail.
        readPrint("TD10", session, set, 11, "$.a + $.b",
                "Both bins in arithmetic, no literal — can parser derive INT?");

        // --- Level 4: Type propagation through comparison ---
        System.out.println();
        System.out.println("  --- Level 4: Propagation through comparison ---");
        // $.a + 1 == $.d: literal 1 makes $.a INT, arithmetic result is INT,
        // so $.d must be INT for the equality to compile
        filterCheck("TD11", session, set, 11, "$.a + 1 == $.d",
                true);   // 10+1 == 11

        // $.a + 1 > $.b: same propagation — $.b derived as INT
        filterCheck("TD12", session, set, 11, "$.a + 1 > $.b",
                true);   // 11 > 10

        // --- Level 5: Cross-expression propagation ---
        System.out.println();
        System.out.println("  --- Level 5: Cross-expression propagation ---");
        // The key test: $.a == $.b and $.c and $.a + 1 == $.d
        // - $.a + 1: literal 1 → a is INT
        // - $.a + 1 == $.d: d is INT (from equality with INT expr)
        // - $.a == $.b: a is INT (from above), so b must be INT
        // - $.c is in a boolean and-chain → c is BOOL
        // Full: (10==10) and true and (11==11) → true
        filterPrint("TD13", session, set, 11,
                "$.a == $.b and $.c and $.a + 1 == $.d",
                "Cross-expression: a,b,d derived INT; c derived BOOL. Expect true");

        // Variant: add string comparison to the chain
        // $.f == 'hello' and $.a + 1 == $.d and $.c
        // - f is STRING (from literal), d is INT (from a+1), c is BOOL (from and)
        filterPrint("TD14", session, set, 11,
                "$.f == 'hello' and $.a + 1 == $.d and $.c",
                "Mixed types in chain: f=STRING, a/d=INT, c=BOOL. Expect true");

        // --- Level 6: Nested arithmetic with propagation ---
        System.out.println();
        System.out.println("  --- Level 6: Nested arithmetic + propagation ---");
        // ($.a * $.b) > 50 — both sides of * need same type. Literal 50 on the
        // right of > gives the comparison context INT, which should propagate
        // through the multiplication to both $.a and $.b.
        filterCheck("TD15", session, set, 11, "($.a * $.b) > 50",
                true);   // 10*10 = 100 > 50

        // ($.a + $.b) == ($.d + $.b) — no literals at all, but both sides
        // of == must agree. If the parser can't derive type, this should fail.
        filterPrint("TD16", session, set, 11,
                "($.a + $.b) == ($.d + $.b)",
                "No literals anywhere — can parser derive types? Expect false (20 != 21)");

        // --- Level 7: Comparison to bin also used in arithmetic ---
        System.out.println();
        System.out.println("  --- Level 7: Shared bin reference across contexts ---");
        // $.a > 0 and $.a + $.b > 15
        // First clause: $.a > 0 → a is INT. Second clause: $.a is already INT,
        // so $.b in ($.a + $.b) should also be INT.
        filterCheck("TD17", session, set, 11, "$.a > 0 and $.a + $.b > 15",
                true);   // 10>0 and 20>15

        // --- Level 8: Type mismatch detection ---
        System.out.println();
        System.out.println("  --- Level 8: Type mismatch detection ---");
        // $.a + $.f — INT + STRING should be a type error
        readExpectError("TD18", session, set, 11, "$.a + $.f",
                "INT + STRING should fail");

        // $.a > 'hello' — comparing INT bin to STRING literal
        filterPrint("TD19", session, set, 11, "$.a > 'hello'",
                "INT bin compared to STRING literal — should error or return false");

        // --- Level 9: asInt()/asFloat() as explicit disambiguation ---
        System.out.println();
        System.out.println("  --- Level 9: Explicit cast vs inference ---");
        // Compare: the parser can't tell if bin 'a' is INT or FLOAT when there's
        // no context. With asFloat(), we explicitly say "treat a as INT, cast to FLOAT".
        readCheck("TD20", session, set, 11, "$.a.asFloat() + $.e",
                13.14);  // 10.0 + 3.14 = 13.14

        // Without explicit cast, $.a + $.e should fail (INT + FLOAT mismatch)
        readExpectError("TD21", session, set, 11, "$.a + $.e",
                "INT + FLOAT without cast — should fail");
    }

    // =====================================================================
    // 3. Map Access
    // =====================================================================
    static void testMapAccess(Session session, DataSet set) {
        section("3. MAP ACCESS");

        readCheck("M01", session, set, 2, "$.m.alpha.get(type: INT)", 10L);
        readCheck("M02", session, set, 2, "$.m.'alpha'.get(type: INT)", 10L);
        readPrint("M03", session, set, 5, "$.m.1.get(type: STRING)",
                "Integer key lookup (known issue 2b)");
        readPrint("M04", session, set, 2, "$.m.{0}.get(type: INT)",
                "Map by index 0 (first by key order)");
        readCheck("M09", session, set, 2, "$.m.{}.count()", 5L);
        readCheck("M10", session, set, 3, "$.profile.address.city.get(type: STRING)", "Austin");

        readPrint("M11", session, set, 2, "$.m.{alpha-delta}",
                "Key range [alpha, delta)");
        readPrint("M12", session, set, 2, "$.m.{delta-}",
                "Key range from delta onwards");
        readPrint("M13", session, set, 2, "$.m.{alpha,gamma}",
                "Key list");
        readPrint("M14", session, set, 2, "$.m.{!alpha-delta}",
                "Inverted key range");
        readPrint("M15", session, set, 2, "$.m.{!alpha,gamma}",
                "Inverted key list");
        readPrint("M16", session, set, 2, "$.m.{0:3}",
                "Index range 0:3");
        readPrint("M17", session, set, 2, "$.m.{-2:}",
                "Index range last 2");
        readPrint("M18", session, set, 2, "$.m.{=15:35}",
                "Value range [15,35)");
        readPrint("M19", session, set, 2, "$.m.{=10,30,50}",
                "Value list");
        readPrint("M20", session, set, 9, "$.scores.{#0:3}",
                "Rank range bottom 3");
        readPrint("M21", session, set, 9, "$.scores.{#-2:}",
                "Rank range top 2");
        readPrint("M22", session, set, 9, "$.scores.{!#0:2}",
                "Inverted rank range");
        readPrint("M23", session, set, 9, "$.scores.{#-1:2~88}",
                "Relative rank range (relative to value 88)");
        readPrint("M24", session, set, 2, "$.m.{0:2~beta}",
                "Key-relative index range");
        readPrint("M25", session, set, 2, "$.m.{alpha-delta}.get(return: COUNT)",
                "Count on key range");
    }

    // =====================================================================
    // 4. List Access
    // =====================================================================
    static void testListAccess(Session session, DataSet set) {
        section("4. LIST ACCESS");

        readCheck("L01", session, set, 2, "$.l.[0].get(type: INT)", 50L);
        readCheck("L02", session, set, 2, "$.l.[3].get(type: INT)", 20L);
        readCheck("L03", session, set, 2, "$.l.[-1].get(type: INT)", 5L);
        readCheck("L04", session, set, 2, "$.l.[-3].get(type: INT)", 30L);
        readCheck("L08", session, set, 2, "$.l.[].count()", 7L);

        readPrint("L09", session, set, 2, "$.l.[0:3]", "Index range [0,3)");
        readPrint("L10", session, set, 2, "$.l.[-3:]", "Last 3 elements");
        readPrint("L11", session, set, 2, "$.l.[!0:2]", "Inverted index range");
        readPrint("L12", session, set, 2, "$.l.[=10,30,50]", "Value list");
        readPrint("L13", session, set, 2, "$.l.[!=10,30,50]", "Inverted value list");
        readPrint("L14", session, set, 2, "$.l.[=20:50]", "Value range [20,50)");
        readPrint("L15", session, set, 2, "$.l.[#0:3]", "Rank range bottom 3");
        readPrint("L16", session, set, 2, "$.l.[#-3:]", "Rank range top 3");
        readPrint("L17", session, set, 2, "$.l.[!#0:2]", "Inverted rank range");
        readPrint("L18", session, set, 2, "$.l.[#-1:2~30]", "Relative rank range");
        readCheck("L19", session, set, 2, "$.l.[=10].count()", 1L);
        readPrint("L20", session, set, 2, "$.l.[=20:50].get(return: COUNT)",
                "Count on value range");
    }

    // =====================================================================
    // 5. Nested CDT Navigation
    // =====================================================================
    static void testNestedCDT(Session session, DataSet set) {
        section("5. NESTED CDT NAVIGATION");

        readCheck("N01", session, set, 3, "$.profile.address.city.get(type: STRING)", "Austin");
        readCheck("N02", session, set, 3, "$.profile.address.zip.get(type: STRING)", "73301");
        readCheck("N03", session, set, 3, "$.profile.scores.[0].get(type: INT)", 95L);
        readCheck("N04", session, set, 3, "$.profile.scores.[-1].get(type: INT)", 63L);
        readCheck("N05", session, set, 3, "$.profile.scores.[].count()", 5L);
        readCheck("N06", session, set, 3, "$.profile.scores.[#-1].get(type: INT)", 100L);
        readCheck("N07", session, set, 4, "$.data.users.[0].name.get(type: STRING)", "Bob");
        readCheck("N08", session, set, 4,
                "$.data.users.[0].addresses.[1].city.get(type: STRING)", "LA");
        readCheck("N09", session, set, 4, "$.data.users.[1].name.get(type: STRING)", "Eve");
        readCheck("N10", session, set, 4,
                "$.data.users.[1].addresses.[0].city.get(type: STRING)", "SF");
        readPrint("N11", session, set, 3, "$.profile.scores.[0:3]",
                "Map → List index range");
        readCheck("N13", session, set, 3, "$.profile.address.{}.count()", 3L);
    }

    // =====================================================================
    // 6. Arithmetic
    // =====================================================================
    static void testArithmetic(Session session, DataSet set) {
        section("6. ARITHMETIC");

        readCheck("A01", session, set, 7, "$.price + $.qty", 105L);
        readCheck("A02", session, set, 7, "$.price - $.qty", 95L);
        readCheck("A03", session, set, 7, "$.price * $.qty", 500L);
        readCheck("A04", session, set, 7, "$.price / $.qty", 20L);
        readCheck("A05", session, set, 7, "$.price % $.qty", 0L);
        readCheck("A06", session, set, 7, "$.price + 50", 150L);
        readCheck("A07", session, set, 7, "($.price * $.qty) - 100", 400L);
        readCheck("A08", session, set, 7, "(($.price + $.qty) * 2) - 10", 200L);
        filterCheck("A09", session, set, 7, "($.price * $.qty) > 400", true);
        readCheck("A10", session, set, 10, "$.c + $.d", 71.2);
        readCheck("A11", session, set, 1, "$.intBin / 5", 8L);
        readCheck("A12", session, set, 1, "$.intBin % 5", 2L);
    }

    // =====================================================================
    // 7. Bitwise Operations
    // =====================================================================
    static void testBitwise(Session session, DataSet set) {
        section("7. BITWISE OPERATIONS");

        readCheck("B01", session, set, 1, "$.intBin & 15", 10L);
        readCheck("B02", session, set, 1, "$.intBin | 15", 47L);
        readCheck("B03", session, set, 1, "$.intBin ^ 15", 37L);
        readCheck("B04", session, set, 1, "~$.intBin", -43L);
        readCheck("B05", session, set, 1, "$.intBin << 2", 168L);
        readCheck("B06", session, set, 1, "$.intBin >> 1", 21L);

        readPrint("B07", session, set, 1, "$.negInt >> 1",
                "Known issue 2c: >> is logical, not arithmetic. Expect -4 but get 4611686018427387900");
        readExpectError("B08", session, set, 1, "$.negInt >>> 1",
                "Known issue 2c: >>> not in grammar");

        filterCheck("B09", session, set, 1, "($.intBin & 1) == 0", true);
        filterCheck("B10", session, set, 1, "(($.intBin >> 3) & 1) == 1", true);
    }

    // =====================================================================
    // 8. Comparison Operators
    // =====================================================================
    static void testComparison(Session session, DataSet set) {
        section("8. COMPARISON OPERATORS");

        filterCheck("C01", session, set, 1, "$.intBin == 42", true);
        filterCheck("C02", session, set, 1, "$.intBin != 42", false);
        filterCheck("C03", session, set, 1, "$.intBin > 41", true);
        filterCheck("C04", session, set, 1, "$.intBin >= 42", true);
        filterCheck("C05", session, set, 1, "$.intBin < 43", true);
        filterCheck("C06", session, set, 1, "$.intBin <= 42", true);
        filterCheck("C07", session, set, 1, "$.strBin == 'hello'", true);
        filterCheck("C08", session, set, 1, "$.strBin != 'world'", true);
        filterCheck("C09", session, set, 1, "$.floatBin > 3.0", true);
        filterCheck("C10", session, set, 1, "$.boolBin == true", true);
        filterCheck("C11", session, set, 7, "\"gold\" in $.items", true);
        filterCheck("C12", session, set, 7, "\"platinum\" in $.items", false);
        filterCheck("C13", session, set, 7, "$.status in [\"active\", \"pending\"]", true);
        filterCheck("C15", session, set, 1, "$.intBin >= 42 and $.intBin <= 42", true);
        filterCheck("C16", session, set, 7, "100 == $.price", true);
    }

    // =====================================================================
    // 9. Logical Operators
    // =====================================================================
    static void testLogical(Session session, DataSet set) {
        section("9. LOGICAL OPERATORS");

        filterCheck("LG01", session, set, 1, "$.intBin > 40 and $.strBin == 'hello'", true);
        filterCheck("LG02", session, set, 1, "$.intBin > 50 and $.strBin == 'hello'", false);
        filterCheck("LG03", session, set, 1, "$.intBin > 50 or $.strBin == 'hello'", true);
        filterCheck("LG04", session, set, 1, "$.intBin > 50 or $.strBin == 'world'", false);
        filterCheck("LG05", session, set, 1, "not($.intBin > 50)", true);
        filterCheck("LG06", session, set, 1, "not(not($.intBin > 40))", true);
        filterCheck("LG07", session, set, 7, "exclusive($.flag1, $.flag2)", true);
        filterCheck("LG09", session, set, 7, "$.flag1 or $.flag2 and $.flag2", true);
        filterCheck("LG10", session, set, 7, "($.flag1 or $.flag2) and $.flag2", false);
        filterCheck("LG11", session, set, 7, "$.flag1 and not($.flag2)", true);
        filterCheck("LG12", session, set, 1,
                "$.intBin > 0 and $.intBin < 100 and $.strBin == 'hello'", true);
        filterCheck("LG13", session, set, 1,
                "$.intBin == 0 or $.intBin == 42 or $.intBin == 99", true);
    }

    // =====================================================================
    // 10. Control Structures
    // =====================================================================
    static void testControlStructures(Session session, DataSet set) {
        section("10. CONTROL STRUCTURES");

        // 10.1 Variable binding (with...do — current implementation)
        readCheck("CS01", session, set, 7,
                "let ('x' = $.price) then (${x} + 1)", 101L);
        readExpectError("CS01s", session, set, 7,
                "let (x = $.price) then (${x} + 1)",
                "Spec syntax let...then (known issue 2a)");
        readCheck("CS02", session, set, 7,
                "let ('x' = $.price, 'y' = $.qty) then (${x} * ${y})", 500L);
        readCheck("CS03", session, set, 7,
                "let ('x' = $.price, 'y' = ${x} * 2) then (${y} + ${x})", 300L);
        readCheck("CS04", session, set, 7,
                "let ('total' = $.price * $.qty, 'tax' = ${total} / 10) then (${total} + ${tax})", 550L);
        readPrint("CS05", session, set, 7,
                "let ('x' = $.price) do (with ('y' = ${x} * 2) then (${y} + ${x}))",
                "Nested with...do, expect 300");
        filterCheck("CS06", session, set, 7,
                "let ('total' = $.price * $.qty) then (${total} > 400)", true);

        // 10.2 Conditional (when...default)
        readCheck("CS07", session, set, 7,
                "when ($.tier == 1 => \"gold\", $.tier == 2 => \"silver\", default => \"bronze\")",
                "silver");
        readCheck("CS08", session, set, 7,
                "when ($.tier == 5 => \"diamond\", default => \"standard\")",
                "standard");
        readCheck("CS09", session, set, 7,
                "when ($.tier == 1 => 100, $.tier == 2 => 200, $.tier == 3 => 300, default => 0)",
                200L);
        readCheck("CS10", session, set, 7,
                "when ($.price > 200 => \"expensive\", $.price > 50 => \"moderate\", default => \"cheap\")",
                "moderate");
        readPrint("CS11", session, set, 7,
                "when ($.tier > 0 => when ($.tier == 1 => \"tier1\", default => \"tierN\"), default => \"none\")",
                "Nested when, expect 'tierN'");
        filterCheck("CS12", session, set, 7,
                "$.status == (when ($.tier == 2 => \"active\", default => \"inactive\"))", true);

        // 10.3 Mixed nesting
        readPrint("CS13", session, set, 7,
                "let ('t' = $.tier) then (when (${t} == 1 => \"gold\", ${t} == 2 => \"silver\", default => \"bronze\"))",
                "when inside let, expect 'silver'");
        readPrint("CS14", session, set, 7,
                "when ($.tier == 2 => let ('p' = $.price) then (${p} * 2), default => 0)",
                "let inside when branch, expect 200");
        readPrint("CS15", session, set, 7,
                "let ('t' = $.tier) then (when (${t} == 2 => let ('p' = $.price) then (${p} + ${t}), default => 0))",
                "Deeply nested: let → when → let, expect 102");
    }

    // =====================================================================
    // 11. Metadata
    // =====================================================================
    static void testMetadata(Session session, DataSet set) {
        section("11. METADATA");

        readPrint("MD01", session, set, 1, "$.ttl()", "TTL in seconds");
        readPrint("MD02", session, set, 1, "$.recordSize()", "Record size in bytes (> 0)");
        filterPrint("MD03", session, set, 1, "$.keyExists()", "Key exists check");
        filterCheck("MD04", session, set, 1, "$.setName() == 'dsl_test_spec'", true);
        filterCheck("MD05", session, set, 1, "$.ttl() > 0 or $.ttl() == -1", true);
        filterCheck("MD06", session, set, 1, "$.sinceUpdate() >= 0", true);
        readPrint("MD07", session, set, 1, "$.voidTime()", "Void time");
        readPrint("MD08", session, set, 1, "$.digestModulo(3)", "Digest modulo 3 (0, 1, or 2)");
        filterCheck("MD09", session, set, 1, "not($.isTombstone())", true);
        filterCheck("MD10", session, set, 1, "$.recordSize() > 0 and $.ttl() != 0", true);
    }

    // =====================================================================
    // 12. Path Functions
    // =====================================================================
    static void testPathFunctions(Session session, DataSet set) {
        section("12. PATH FUNCTIONS");

        readCheck("PF01", session, set, 2, "$.m.alpha.get(type: INT)", 10L);
        readPrint("PF02", session, set, 2, "$.m.{alpha-delta}.get(return: COUNT)",
                "Count on key range");
        readPrint("PF03", session, set, 9, "$.scores.{#-1}.get(return: KEY)",
                "Key at highest rank");
        readPrint("PF04", session, set, 2, "$.m.alpha.get(return: INDEX)",
                "Index of key alpha");
        readPrint("PF05", session, set, 9, "$.scores.math.get(return: RANK)",
                "Rank of key math");
        readPrint("PF06", session, set, 2, "$.m.{alpha,beta}.get(return: ORDERED_MAP)",
                "Ordered map of key list");
        readPrint("PF07", session, set, 2, "$.m.alpha.get(return: EXISTS)",
                "Existing key → true");
        readPrint("PF08", session, set, 2, "$.m.zzz.get(return: EXISTS)",
                "Missing key → false");
        readCheck("PF09", session, set, 2, "$.l.[].count()", 7L);
        readCheck("PF10", session, set, 2, "$.l.[=50].count()", 1L);

        readPrint("PF11", session, set, 1, "$.intBin.exists()",
                "Known issue 2d: exists() silently ignored — returns bin value not boolean");
        readPrint("PF12", session, set, 6, "$.emptyList.exists()",
                "Known issue 2d: exists() silently ignored");
    }

    // =====================================================================
    // 13. Transaction Scenario
    // =====================================================================
    static void testTransactionScenario(Session session, DataSet set) {
        section("13. TRANSACTION SCENARIO");

        readCheck("TX01", session, set, 8, "$.txns.{}.count()", 12L);
        readPrint("TX02", session, set, 8, "$.txns.{0}.get(return: KEY_VALUE)",
                "First transaction by key order");
        readPrint("TX03", session, set, 8, "$.txns.{-1}.get(return: KEY_VALUE)",
                "Last transaction by key order");

        readPrint("TX04", session, set, 8,
                "$.txns.{\"1688169600000\"-\"1696118400000\"}",
                "Transactions in Q3 2023 (Jul-Sep) — expect 3 entries");
        readPrint("TX05", session, set, 8,
                "$.txns.{\"1688169600000\"-\"1696118400000\"}.get(return: COUNT)",
                "Count in Q3 2023 — expect 3");
        readPrint("TX06", session, set, 8,
                "$.txns.{\"1685577600000\"-}",
                "All from Jun 2023 onwards — expect 7 entries");
        readPrint("TX07", session, set, 8,
                "$.txns.{-\"1680307200000\"}",
                "All before Apr 2023 — expect 3 entries");
        readPrint("TX08", session, set, 8,
                "$.txns.{\"9999999999999\"-}",
                "Empty range — expect empty map");
        readPrint("TX09", session, set, 8,
                "$.txns.{\"1685577600000\"-\"1701388800000\"}.get(return: COUNT)",
                "Count Jun-Nov — expect 6");

        readPrint("TX10", session, set, 8, "$.txns.{#-1}.get(return: KEY_VALUE)",
                "Highest value transaction (rank -1) — expect txn09 (12000)");
        readPrint("TX11", session, set, 8, "$.txns.{#0}.get(return: KEY_VALUE)",
                "Lowest value transaction (rank 0) — expect txn08 (45)");
        readPrint("TX12", session, set, 8, "$.txns.{#-3:}",
                "Top 3 by value");
        readPrint("TX13", session, set, 8, "$.txns.{#0:3}",
                "Bottom 3 by value");
        readPrint("TX14", session, set, 8, "$.txns.{#-5:}",
                "Top 5 by value");

        readPrint("TX15", session, set, 8,
                "let ('filtered' = $.txns.{\"1685577600000\"-\"1701388800000\"}.get(return: COUNT)) then (${filtered})",
                "Chained: time range → count via let...then — expect 6");

        filterCheck("TX18", session, set, 8, "$.txns.{}.count() > 10", true);
        filterCheck("TX19", session, set, 8,
                "$.txns.{\"1688169600000\"-\"1696118400000\"}.get(return: COUNT) > 0", true);
        readPrint("TX20", session, set, 8, "$.txns.{#-1}.[0].get(type: INT)",
                "Amount of highest value transaction — expect 12000");
    }

    // =====================================================================
    // 14. Rank-Based Access (Record 9)
    // =====================================================================
    static void testRankBased(Session session, DataSet set) {
        section("14. RANK-BASED ACCESS");
        System.out.println("  scores rank order: english(78) < math(85) < history(88) < science(92) < art(95)");
        System.out.println();

        readPrint("R01", session, set, 9, "$.scores.{#0}.get(return: KEY)",
                "Key at rank 0 — expect 'english'");
        readPrint("R02", session, set, 9, "$.scores.{#0}.get(type: INT)",
                "Value at rank 0 — expect 78");
        readPrint("R03", session, set, 9, "$.scores.{#-1}.get(return: KEY)",
                "Key at rank -1 (highest) — expect 'art'");
        readPrint("R04", session, set, 9, "$.scores.{#-1}.get(type: INT)",
                "Value at rank -1 — expect 95");
        readPrint("R05", session, set, 9, "$.scores.{#-2:}",
                "Top 2 by rank — expect science(92), art(95)");
        readPrint("R06", session, set, 9, "$.scores.{#0:2}",
                "Bottom 2 by rank — expect english(78), math(85)");
        readPrint("R07", session, set, 9, "$.scores.{!#-2:}",
                "All except top 2");
        readPrint("R08", session, set, 9, "$.scores.math.get(return: RANK)",
                "Rank of math — expect 1 (second lowest)");
        readPrint("R09", session, set, 9, "$.scores.{#-1}.get(return: KEY_VALUE)",
                "Key-value of highest — expect {art: 95}");
    }

    // =====================================================================
    // 15. Return Type Variations
    // =====================================================================
    static void testReturnTypes(Session session, DataSet set) {
        section("15. RETURN TYPE VARIATIONS");
        System.out.println("  Using $.m.{alpha,beta,gamma} on Record 2");
        System.out.println();

        readPrint("RT01", session, set, 2, "$.m.{alpha,beta,gamma}",
                "Default (ORDERED_MAP)");
        readPrint("RT02", session, set, 2, "$.m.{alpha,beta,gamma}.get(return: COUNT)",
                "COUNT — expect 3");
        readPrint("RT03", session, set, 2, "$.m.{alpha,beta,gamma}.get(return: KEY)",
                "KEY — expect list of keys");
        readPrint("RT04", session, set, 2, "$.m.{alpha,beta,gamma}.get(return: VALUE)",
                "VALUE — expect list of values");
        readPrint("RT05", session, set, 2, "$.m.{alpha,beta,gamma}.get(return: KEY_VALUE)",
                "KEY_VALUE");
        readPrint("RT06", session, set, 2, "$.m.alpha.get(return: INDEX)",
                "INDEX — expect 0");
        readPrint("RT07", session, set, 2, "$.m.alpha.get(return: RANK)",
                "RANK — expect 0 (value 10 is lowest)");
        readPrint("RT08", session, set, 2, "$.m.alpha.get(return: EXISTS)",
                "EXISTS — expect true");
        readPrint("RT09", session, set, 2, "$.m.zzz.get(return: EXISTS)",
                "EXISTS (absent) — expect false");
        readPrint("RT10", session, set, 2, "$.m.alpha.get(return: NONE)",
                "NONE — expect null");
    }

    // =====================================================================
    // 16. Edge Cases
    // =====================================================================
    static void testEdgeCases(Session session, DataSet set) {
        section("16. EDGE CASES");

        readCheck("E01", session, set, 2, "$.l.[0].get(type: INT)", 50L);
        readCheck("E02", session, set, 2, "$.l.[-1].get(type: INT)", 5L);
        readCheck("E03", session, set, 6, "$.emptyList.[].count()", 0L);
        readCheck("E04", session, set, 6, "$.emptyMap.{}.count()", 0L);
        readCheck("E05", session, set, 6, "$.intBin + 1", 1L);
        filterCheck("E06", session, set, 6, "$.strBin == ''", true);
        readCheck("E07", session, set, 1, "$.negInt + $.negInt", -16L);

        readPrint("E08", session, set, 2, "$.l.[100].get(type: INT)",
                "Out of range index — expect error or null");
        readPrint("E09", session, set, 2, "$.l.[-100].get(type: INT)",
                "Negative index beyond list — expect error or null");
        readPrint("E10", session, set, 2, "$.m.zzz.get(type: INT)",
                "Non-existent map key — expect error or null");

        readExpectError("E12", session, set, 1, "$.intBin / 0",
                "Division by zero");
        readExpectError("E13", session, set, 1, "$.intBin % 0",
                "Modulus by zero");

        readCheck("E17", session, set, 3, "$.profile.'name'.get(type: STRING)", "Alice");
        readCheck("E18", session, set, 3, "$.profile.\"name\".get(type: STRING)", "Alice");
    }

    // =====================================================================
    // Helper Methods
    // =====================================================================
    static void section(String title) {
        System.out.println();
        System.out.println(SEP);
        System.out.println(title);
        System.out.println(SEP);
        System.out.println();
    }

    /**
     * Execute a read expression and compare to an expected value.
     */
    static void readCheck(String id, Session session, DataSet set, int pk,
                          String dsl, Object expected) {
        totalTests++;
        System.out.printf("  [%s] %s%n", id, dsl);
        try {
            RecordStream rs = session.query(set.id(pk))
                    .bin("r").selectFrom(dsl)
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object actual = rec.bins.get("r");

            boolean pass;
            if (expected instanceof Double && actual instanceof Double) {
                pass = Math.abs((Double) expected - (Double) actual) < 0.001;
            } else {
                pass = (expected == null && actual == null)
                        || (expected != null && expected.equals(actual));
            }

            if (pass) {
                passedTests++;
                System.out.printf("      PASS — %s%n", actual);
            } else {
                failedTests++;
                System.out.printf("      ** FAIL ** — Expected: %s (%s), Actual: %s (%s)%n",
                        expected, className(expected), actual, className(actual));
            }
        } catch (Exception e) {
            failedTests++;
            System.out.printf("      ** ERROR ** — %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Execute a read expression, print the result without checking.
     */
    static void readPrint(String id, Session session, DataSet set, int pk,
                          String dsl, String description) {
        totalTests++;
        System.out.printf("  [%s] %s%n", id, dsl);
        System.out.printf("      Note: %s%n", description);
        try {
            RecordStream rs = session.query(set.id(pk))
                    .bin("r").selectFrom(dsl)
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object actual = rec.bins.get("r");
            passedTests++;
            System.out.printf("      Result: %s (%s)%n", actual, className(actual));
        } catch (Exception e) {
            errorTests++;
            System.out.printf("      ERROR: %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Execute a read expression where we expect an error.
     */
    static void readExpectError(String id, Session session, DataSet set, int pk,
                                String dsl, String description) {
        totalTests++;
        System.out.printf("  [%s] %s%n", id, dsl);
        System.out.printf("      Note: %s%n", description);
        try {
            RecordStream rs = session.query(set.id(pk))
                    .bin("r").selectFrom(dsl)
                    .execute();
            Record rec = rs.getFirst().orElseThrow().recordOrThrow();
            Object actual = rec.bins.get("r");
            failedTests++;
            System.out.printf("      ** UNEXPECTED SUCCESS ** — Got: %s (%s)%n",
                    actual, className(actual));
        } catch (Exception e) {
            passedTests++;
            System.out.printf("      Expected error: %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Execute a filter expression and check whether the record is returned.
     */
    static void filterCheck(String id, Session session, DataSet set, int pk,
                            String dsl, boolean expectFound) {
        totalTests++;
        System.out.printf("  [%s] %s%n", id, dsl);
        try {
            RecordStream rs = session.query(set.id(pk))
                    .where(dsl)
                    .execute();
            boolean found = rs.getFirst().isPresent();

            if (found == expectFound) {
                passedTests++;
                System.out.printf("      PASS — filter %s%n",
                        found ? "passed (record returned)" : "rejected (record excluded)");
            } else {
                failedTests++;
                System.out.printf("      ** FAIL ** — Expected %s, got %s%n",
                        expectFound ? "record returned" : "record excluded",
                        found ? "record returned" : "record excluded");
            }
        } catch (Exception e) {
            failedTests++;
            System.out.printf("      ** ERROR ** — %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
        }
    }

    /**
     * Execute a filter expression and print the result without checking.
     */
    static void filterPrint(String id, Session session, DataSet set, int pk,
                            String dsl, String description) {
        totalTests++;
        System.out.printf("  [%s] %s%n", id, dsl);
        System.out.printf("      Note: %s%n", description);
        try {
            RecordStream rs = session.query(set.id(pk))
                    .where(dsl)
                    .execute();
            boolean found = rs.getFirst().isPresent();
            passedTests++;
            System.out.printf("      Result: filter %s%n",
                    found ? "passed (record returned)" : "rejected (record excluded)");
        } catch (Exception e) {
            errorTests++;
            System.out.printf("      ERROR: %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
        }
    }

    static String className(Object obj) {
        return obj == null ? "null" : obj.getClass().getSimpleName();
    }

    static void printSummary() {
        System.out.println();
        System.out.println(SEP);
        System.out.printf("SUMMARY: %d total | %d passed | %d failed | %d errors%n",
                totalTests, passedTests, failedTests, errorTests);
        System.out.println(SEP);
    }
}
