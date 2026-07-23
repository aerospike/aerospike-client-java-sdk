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
package com.aerospike.examples.fixtures;

import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.examples.ExampleContext;
import com.aerospike.examples.ExampleFixture;

public final class ExampleFixtures {
    private static final String COMMON_INDEX_NAME = "ageidx";

    private ExampleFixtures() {
    }

    // =====================================================================
    // Generic builders
    //
    // Most fixtures follow the same shape: truncate one or more sets before
    // and after the example, then assert something in verify. These builders
    // capture that shape so each fixture only has to express what varies (the
    // sets it owns and the verification), keeping the doc-facing examples free
    // of setup/teardown noise.
    // =====================================================================

    /**
     * Fixture that truncates the sets resolved from {@code sets} before and after the example
     * runs, and executes {@code verify} once the example completes.
     */
    private static ExampleFixture truncating(
        Function<ExampleContext, List<DataSet>> sets,
        Consumer<ExampleContext> verify
    ) {
        return new ExampleFixture() {
            @Override
            public void setup(ExampleContext context) {
                truncateAll(context, sets);
            }

            @Override
            public void verify(ExampleContext context) {
                verify.accept(context);
            }

            @Override
            public void cleanup(ExampleContext context) {
                truncateAll(context, sets);
            }
        };
    }

    /** Convenience builder for a fixture that owns a single named set. */
    private static ExampleFixture truncating(String set, Consumer<ExampleContext> verify) {
        return truncating(context -> List.of(context.dataSet(set)), verify);
    }

    private static void truncateAll(ExampleContext context, Function<ExampleContext, List<DataSet>> sets) {
        Session session = context.session();

        for (DataSet dataSet : sets.apply(context)) {
            ExampleAssertions.truncate(session, dataSet);
        }
    }

    // =====================================================================
    // Fixtures
    // =====================================================================

    public static ExampleFixture batchExample() {
        return truncating(
            context -> List.of(context.dataSet()),
            context -> {
                Session session = context.session();
                DataSet dataSet = context.dataSet();

                ExampleAssertions.assertCount(session, dataSet, 7);
                ExampleAssertions.assertRecordMissing(session, dataSet, 1);
                ExampleAssertions.assertBinEquals(session, dataSet, 2, "value", 15L);
                ExampleAssertions.assertBinEquals(session, dataSet, 6, "name", "Wilma");
            });
    }

    public static ExampleFixture commonExample() {
        // Not expressed via truncating(...) because this example also creates a
        // secondary index that must be dropped in setup and cleanup.
        return new ExampleFixture() {
            @Override
            public void setup(ExampleContext context) {
                Session session = context.session();
                DataSet dataSet = context.dataSet();

                dropIndexIfExists(session, dataSet, COMMON_INDEX_NAME);
                ExampleAssertions.truncate(session, dataSet);
            }

            @Override
            public void verify(ExampleContext context) {
                Session session = context.session();
                DataSet dataSet = context.dataSet();

                ExampleAssertions.assertRecordMissing(session, dataSet, 118);
                ExampleAssertions.assertBinEquals(session, dataSet, 1, "name", "Tim");
                ExampleAssertions.assertBinEquals(session, dataSet, 1, "writeBin", 342L);
            }

            @Override
            public void cleanup(ExampleContext context) {
                Session session = context.session();
                DataSet dataSet = context.dataSet();

                dropIndexIfExists(session, dataSet, COMMON_INDEX_NAME);
                ExampleAssertions.truncate(session, dataSet);
            }
        };
    }

    public static ExampleFixture yamlConfigConnectionExample() {
        return truncating(
            "yaml-config-demo",
            context -> {
                Session session = context.session();
                DataSet dataSet = context.dataSet("yaml-config-demo");

                ExampleAssertions.assertCount(session, dataSet, 4);
                ExampleAssertions.assertBinEquals(session, dataSet, "user-001", "name", "Alice");
                ExampleAssertions.assertBinEquals(session, dataSet, "user-004", "age", 28L);
            });
    }

    public static ExampleFixture studentScoresExample() {
        return truncating(
            "class10a",
            context -> {
                Session session = context.session();
                DataSet dataSet = context.dataSet("class10a");

                ExampleAssertions.assertCount(session, dataSet, 30);
                ExampleAssertions.assertBinEquals(session, dataSet, "student-1", "name", "Student 1");
                ExampleAssertions.assertBinEquals(session, dataSet, "student-30", "name", "Student 30");
            });
    }

    public static ExampleFixture mapRemoveByKeyRangeTest() {
        return truncating(
            "map_remove_test",
            context -> {
                TreeMap<String, Long> expected = new TreeMap<>();
                expected.put("a", 1L);
                expected.put("b", 2L);
                expected.put("c", 3L);
                expected.put("d", 4L);
                expected.put("e", 5L);

                Session session = context.session();
                DataSet dataSet = context.dataSet("map_remove_test");

                ExampleAssertions.assertCount(session, dataSet, 1);
                ExampleAssertions.assertBinEquals(session, dataSet, 1, "m", expected);
            });
    }

    public static ExampleFixture transactionProcessingExample() {
        return truncating(
            context -> List.of(
                context.dataSet("customers"),
                context.dataSet("accounts"),
                context.dataSet("txns")),
            context -> {
                Session session = context.session();
                DataSet customers = context.dataSet("customers");
                DataSet accounts = context.dataSet("accounts");
                DataSet txns = context.dataSet("txns");

                ExampleAssertions.assertCount(session, customers, 1);
                ExampleAssertions.assertCount(session, accounts, 1);
                ExampleAssertions.assertCount(session, txns, 1);
                ExampleAssertions.assertBinEquals(session, customers, "CUST-10042", "totalSpend", 45000L);
                ExampleAssertions.assertBinEquals(session, customers, "CUST-10042", "statusLevel", "GOLD");
                ExampleAssertions.assertBinEquals(session, accounts, "4532015112830366", "balanceCents", 45000L);
                ExampleAssertions.assertBinEquals(session, txns, "TXN-00001", "desc", "Car repairs");
            });
    }

    public static ExampleFixture completeYamlConfigExample() {
        return truncating(
            "complete-yaml-demo",
            context -> ExampleAssertions.assertCount(context.session(), context.dataSet("complete-yaml-demo"), 0));
    }

    public static ExampleFixture aelTestSpecRunner() {
        return truncating(
            "ael_test_spec",
            context -> ExampleAssertions.assertCount(context.session(), context.dataSet("ael_test_spec"), 11));
    }

    public static ExampleFixture operationDifferences() {
        return truncating(
            "ael_diff_test",
            context -> ExampleAssertions.assertCount(context.session(), context.dataSet("ael_diff_test"), 7));
    }

    public static ExampleFixture queryExamples() {
        return truncating(
            context -> List.of(context.dataSet("person"), context.dataSet("users")),
            context -> {
                Session session = context.session();
                DataSet person = context.dataSet("person");

                ExampleAssertions.assertRecordExists(session, person, 999);
                ExampleAssertions.assertRecordExists(session, person, 500);
            });
    }

    public static ExampleFixture ecommerceExample() {
        return truncating(
            context -> List.of(
                context.dataSet("customers"),
                context.dataSet("products"),
                context.dataSet("orders")),
            context -> {
                Session session = context.session();
                DataSet customers = context.dataSet("customers");
                DataSet products = context.dataSet("products");
                DataSet orders = context.dataSet("orders");

                ExampleAssertions.assertCount(session, customers, 20);
                ExampleAssertions.assertCount(session, products, 95);
                ExampleAssertions.assertCount(session, orders, 55);
                ExampleAssertions.assertBinEquals(session, customers, "C-100", "balance", 89999L);
                ExampleAssertions.assertBinEquals(session, products, "SKU-LAP01", "stock", 24L);
                ExampleAssertions.assertBinEquals(session, orders, "ORD-2001", "status", "CONFIRMED");
            });
    }

    private static void dropIndexIfExists(Session session, DataSet dataSet, String indexName) {
        try {
            session.dropIndex(dataSet, indexName).waitTillComplete();
        }
        catch (AerospikeException ignored) {
            // The cleanup path should be idempotent for local reruns and CI retries.
        }
    }
}
