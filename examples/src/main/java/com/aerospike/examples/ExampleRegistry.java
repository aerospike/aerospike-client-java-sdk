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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.aerospike.examples.ecommerce.EcommerceExample;
import com.aerospike.examples.fixtures.ExampleFixtures;

public final class ExampleRegistry {
    private static final Map<String, ExampleDefinition> EXAMPLES = createExamples();

    private ExampleRegistry() {
    }

    public static Collection<ExampleDefinition> all() {
        return EXAMPLES.values();
    }

    public static Optional<ExampleDefinition> find(String name) {
        return Optional.ofNullable(EXAMPLES.get(name));
    }

    public static String[] names() {
        return EXAMPLES.keySet().toArray(String[]::new);
    }

    private static Map<String, ExampleDefinition> createExamples() {
        Map<String, ExampleDefinition> examples = new LinkedHashMap<>();

        register(examples, "CommonExample", CommonExample.class, ExampleFixtures.commonExample(), "smoke", "records");
        register(examples, "BatchExample", BatchExample.class, ExampleFixtures.batchExample(), "smoke", "records");
        register(examples, "BehaviorHierarchicalExample", BehaviorHierarchicalExample.class, ExampleFixture.NONE, "config");
        register(examples, "BehaviorYamlExample", BehaviorYamlExample.class, ExampleFixture.NONE, "config");
        register(
            examples,
            "CompleteYamlConfigExample",
            CompleteYamlConfigExample.class,
            ExampleFixtures.completeYamlConfigExample(),
            "config",
            "records");
        register(examples, "YamlConfigExample", YamlConfigExample.class, ExampleFixture.NONE, "config");
        register(
            examples,
            "YamlConfigConnectionExample",
            YamlConfigConnectionExample.class,
            ExampleFixtures.yamlConfigConnectionExample(),
            "smoke",
            "config",
            "records");
        register(examples, "StudentScoresExample", StudentScoresExample.class, ExampleFixtures.studentScoresExample(), "smoke", "records");
        register(
            examples,
            "MapRemoveByKeyRangeTest",
            MapRemoveByKeyRangeTest.class,
            ExampleFixtures.mapRemoveByKeyRangeTest(),
            "smoke",
            "records",
            "ael");
        register(
            examples,
            "TransactionProcessingExample",
            TransactionProcessingExample.class,
            ExampleFixtures.transactionProcessingExample(),
            "smoke",
            "records");
        register(examples, "AelTestSpecRunner", AelTestSpecRunner.class, ExampleFixtures.aelTestSpecRunner(), "ael", "records");
        register(
            examples,
            "OperationDifferences",
            OperationDifferences.class,
            ExampleFixtures.operationDifferences(),
            "diagnostic",
            "ael",
            "records");
        register(
            examples,
            "CdtPathExpressionExample",
            CdtPathExpressionExample.class,
            ExampleFixtures.cdtPathExpressionExample(),
            "extended",
            "records",
            "server-specific");
        register(
            examples,
            "StringOperationsExample",
            StringOperationsExample.class,
            ExampleFixtures.stringOperationsExample(),
            "extended",
            "records",
            "server-specific");
        register(
            examples,
            "TypedMappingExamples",
            TypedMappingExamples.class,
            ExampleFixtures.typedMappingExamples(),
            "extended",
            "mapping",
            "records");
        register(examples, "QueryExamples", QueryExamples.class, ExampleFixtures.queryExamples(), "extended", "records");
        register(examples, "EcommerceExample", EcommerceExample.class, ExampleFixtures.ecommerceExample(), "extended", "records");
        register(examples, "RosterExample", RosterExample.class, ExampleFixture.NONE, "server-specific");
        register(
            examples,
            "VectorTopKQueryExample",
            VectorTopKQueryExample.class,
            ExampleFixtures.vectorTopKQueryExample(),
            "extended",
            "records",
            "server-specific");

        return Collections.unmodifiableMap(examples);
    }

    private static void register(
        Map<String, ExampleDefinition> examples,
        String name,
        Class<? extends Example> exampleClass,
        ExampleFixture fixture,
        String... tags
    ) {
        examples.put(name, new ExampleDefinition(name, exampleClass, fixture, Set.copyOf(List.of(tags))));
    }
}
