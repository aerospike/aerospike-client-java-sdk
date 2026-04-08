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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.SystemSettingsRegistry;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.BehaviorYamlLoader;

/**
 * Example demonstrating how to load behaviors and system settings from a YAML file.
 *
 * <p>The YAML configuration uses a map-based structure:</p>
 * <pre>
 * behaviors:
 *   DEFAULT:
 *     allOperations:
 *       abandonCallAfter: 30s
 *   production:
 *     parent: DEFAULT
 *     allOperations:
 *       abandonCallAfter: 15s
 *
 * system:
 *   DEFAULT:
 *     connections:
 *       minimumConnectionsPerNode: 10
 *   production:
 *     connections:
 *       minimumConnectionsPerNode: 50
 * </pre>
 */
public class YamlConfigExample extends Example {

    public YamlConfigExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster clusterNotUsed, Args args) throws Exception {
        // Example 1: Load from a file
        loadFromFile();

        // Example 2: Load from classpath resource
        loadFromClasspath();

        // Example 3: Load from string (useful for testing)
        loadFromString();

        // Example 4: Use loaded behaviors with a cluster
        useWithCluster();
    }
    /**
     * Load configuration from a file path.
     */
    private void loadFromFile() throws IOException {
        System.out.println("=== Loading from file ===");

        File configFile = new File("src/main/resources/example-config.yml");
        if (configFile.exists()) {
            Map<String, Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromFile(configFile);

            System.out.println("Loaded " + behaviors.size() + " behaviors:");
            for (String name : behaviors.keySet()) {
                System.out.println("  - " + name);
            }

            // System settings are automatically registered in SystemSettingsRegistry
            SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
            System.out.println("Default system settings: " + registry.getDefaultSettings());
            System.out.println("Production cluster settings: " + registry.getClusterSettings("production"));
        } else {
            System.out.println("Config file not found: " + configFile.getAbsolutePath());
        }
    }

    /**
     * Load configuration from a classpath resource.
     */
    private void loadFromClasspath() throws IOException {
        System.out.println("\n=== Loading from classpath ===");

        // Copy resource to temp file (BehaviorYamlLoader.loadBehaviorsFromFile requires a File)
        try (InputStream is = YamlConfigExample.class.getResourceAsStream("/example-config.yml")) {
            if (is != null) {
                Path tempFile = Files.createTempFile("config", ".yml");
                Files.copy(is, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

                Map<String, Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromFile(tempFile.toFile());
                System.out.println("Loaded " + behaviors.size() + " behaviors from classpath");

                Files.delete(tempFile);
            } else {
                System.out.println("Resource not found in classpath");
            }
        }
    }

    /**
     * Load configuration from a YAML string (useful for testing or embedded configs).
     */
    private void loadFromString() throws IOException {
        System.out.println("\n=== Loading from string ===");

        String yaml = """
            behaviors:
              myCustomBehavior:
                allOperations:
                  abandonCallAfter: 5s
                  maximumNumberOfCallAttempts: 3
                batchReads:
                  maxConcurrentServers: 8
                  allowInlineMemoryAccess: true

            system:
              DEFAULT:
                connections:
                  minimumConnectionsPerNode: 20
                  maximumConnectionsPerNode: 200
            """;

        Map<String, Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromString(yaml);

        System.out.println("Loaded behaviors from string:");
        for (Map.Entry<String, Behavior> entry : behaviors.entrySet()) {
            Behavior behavior = entry.getValue();
            System.out.println("  " + entry.getKey() + " (parent: " +
                (behavior.getParent() != null ? behavior.getParent().name() : "none") + ")");
        }
    }

    /**
     * Demonstrates using loaded behaviors with a Cluster.
     */
    private void useWithCluster() throws IOException {
        System.out.println("\n=== Using with Cluster ===");

        // First, load the configuration
        String yaml = """
            behaviors:
              production:
                allOperations:
                  abandonCallAfter: 10s
                  maximumNumberOfCallAttempts: 5

            system:
              DEFAULT:
                connections:
                  minimumConnectionsPerNode: 50
                  maximumConnectionsPerNode: 200
            """;

        Map<String, Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromString(yaml);
        Behavior productionBehavior = behaviors.get("production");

        // Now create a cluster and use the loaded behavior
        // Note: This requires an actual Aerospike server to connect to
        System.out.println("Would create cluster with production behavior...");
        System.out.println("Production behavior abandonCallAfter: " +
            productionBehavior.getSettings(
                Behavior.OpKind.READ,
                Behavior.OpShape.POINT,
                Behavior.Mode.AP
            ).getAbandonCallAfterMs() + "ms");

        // Example of how you would use it with a real cluster:
        /*
        try (Cluster cluster = new ClusterDefinition("localhost", 3000)
                .connect()) {

            // Create a session using the loaded behavior
            Session session = cluster.createSession(productionBehavior);

            // Use the session for operations
            DataSet myDataSet = DataSet.of("test", "mySet");
            session.upsert(myDataSet.id(1))
                .bin("name").setTo("example")
                .execute();
        }
        */
    }
}
