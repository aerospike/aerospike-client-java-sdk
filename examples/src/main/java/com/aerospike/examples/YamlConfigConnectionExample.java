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

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.util.Util;

/**
 * Example demonstrating how to connect to an Aerospike cluster using a custom
 * YAML configuration file specified via the AEROSPIKE_CLIENT_CONFIG_URL environment variable.
 *
 * <p>This example shows:
 * <ul>
 *   <li>Setting up the configuration file path via environment variable</li>
 *   <li>Connecting to the cluster with automatic configuration loading</li>
 *   <li>Using behaviors defined in the YAML configuration</li>
 *   <li>Performing basic CRUD operations with the configured session</li>
 * </ul>
 *
 * <h2>Running with run_examples script</h2>
 *
 * <p>Basic usage (uses default settings):</p>
 * <pre>{@code
 * ./run_examples YamlConfigConnectionExample -h localhost -p 3000
 * }</pre>
 *
 * <p>With custom host and port:</p>
 * <pre>{@code
 * ./run_examples YamlConfigConnectionExample --host db1.example.com --port 3100 --servicesAlternate
 * }</pre>
 *
 * <h2>Using a custom configuration file (optional)</h2>
 *
 * <p>To specify a custom YAML configuration file with behaviors and settings,
 * set the {@code AEROSPIKE_CLIENT_CONFIG_URL} environment variable before running:</p>
 * <pre>{@code
 * export AEROSPIKE_CLIENT_CONFIG_URL=/path/to/client-config-example.yml
 * ./run_examples YamlConfigConnectionExample -h localhost -p 3000
 * }</pre>
 *
 * <p>Or with file:// prefix:</p>
 * <pre>{@code
 * export AEROSPIKE_CLIENT_CONFIG_URL=file:///path/to/client-config-example.yml
 * ./run_examples YamlConfigConnectionExample -h localhost -p 3000
 * }</pre>
 *
 * <p>If the environment variable is not set, the example will use default settings.</p>
 */
public class YamlConfigConnectionExample extends Example {

    private static final String ENV_CONFIG_URL = "AEROSPIKE_CLIENT_CONFIG_URL";

    public YamlConfigConnectionExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster clusterNotUsed, Args args) throws Exception {
        // Check if the configuration environment variable is set
        String configPath = System.getenv(ENV_CONFIG_URL);

        console.write("=== Aerospike SDK - YAML Configuration Example ===\n");

        if (configPath == null || configPath.isEmpty()) {
            console.write("Note: " + ENV_CONFIG_URL + " environment variable is not set.");
            console.write("The client will use default settings.\n");
            console.write("To use a custom configuration, set the environment variable:");
            console.write("  export " + ENV_CONFIG_URL + "=/path/to/client-config-example.yml\n");
        } else {
            console.write("Configuration file: " + configPath);
            console.write("The client will load behaviors and settings from this file.\n");
        }

        console.write("Connection settings: " + args);
        console.write("Connecting to Aerospike cluster at " + args.host + ":" + args.port);
        if (args.useServicesAlternate) {
            console.write("Using alternate services for cluster discovery");
        }

        // Create cluster definition
        // When connect() is called, it automatically checks for AEROSPIKE_CLIENT_CONFIG_URL
        // and loads the configuration if the environment variable is set
        ClusterDefinition clusterDef = new ClusterDefinition(args.host, args.port)
            .appId("yaml-config-example")
            .failIfNotConnected(true);

        if (args.useServicesAlternate) {
            clusterDef.usingServicesAlternate();
        }

        try (Cluster cluster = clusterDef.connect()) {
            console.write("Successfully connected to cluster!\n");

            // Show if behaviors were loaded from YAML
            demonstrateBehaviorLoading();

            // Create a session - use a behavior from YAML if available, otherwise DEFAULT
            Behavior behavior = getBehaviorOrDefault("fast-operations");
            Session session = cluster.createSession(behavior);

            console.write("Using behavior: " + behavior.name() + "\n");

            // Perform example operations
            performExampleOperations(session, args);

            // Demonstrate using different behaviors for different operations
            demonstrateBehaviorSwitching(cluster);

        } catch (Throwable t) {
            console.error("Error: " + Util.getErrorMessage(t));
            t.printStackTrace();
        } finally {
            // Clean up monitoring if it was started
            if (Behavior.isMonitoring()) {
                Behavior.shutdownMonitor();
                console.write("\nBehavior monitoring stopped.");
            }
        }
    }

    /**
     * Demonstrate that behaviors were loaded from YAML configuration
     */
    private void demonstrateBehaviorLoading() {
        console.write("=== Behavior Configuration Status ===");
        console.write("Monitoring active: " + Behavior.isMonitoring());

        // Check for behaviors defined in our example YAML
        String[] expectedBehaviors = {"fast-operations", "safe-operations", "batch-fast"};

        for (String behaviorName : expectedBehaviors) {
            Behavior behavior = Behavior.getBehavior(behaviorName);
            if (behavior != Behavior.DEFAULT) {
                console.write("  Found behavior: " + behaviorName);

                // Show some settings from the behavior
                ResolvedSettings settings = behavior.getSettings(
                    Behavior.OpKind.READ,
                    Behavior.OpShape.POINT,
                    Behavior.Mode.AP
                );
                if (settings != null) {
                    console.write("    - abandonCallAfter: " + settings.getAbandonCallAfterMs() + "ms");
                    console.write("    - maxRetries: " + settings.getMaximumNumberOfCallAttempts());
                }
            } else {
                console.write("  Behavior not found (using DEFAULT): " + behaviorName);
            }
        }
        console.write("");
    }

    /**
     * Get a named behavior or fall back to DEFAULT if not found
     */
    private Behavior getBehaviorOrDefault(String behaviorName) {
        Behavior behavior = Behavior.getBehavior(behaviorName);
        if (behavior == Behavior.DEFAULT) {
            console.write("Behavior '" + behaviorName + "' not found in configuration, using DEFAULT");
        }
        return behavior;
    }

    /**
     * Perform example CRUD operations using the session
     */
    private void performExampleOperations(Session session, Args args) {
        console.write("=== Performing Example Operations ===");

        DataSet dataSet = DataSet.of(args.namespace, "yaml-config-demo");

        try {
            // Write a record
            console.write("Writing a test record...");
            session.upsert(dataSet)
                .bins("name", "email", "age")
                .id("user-001").values("Alice", "alice@example.com", 30)
                .execute();
            console.write("  Record written successfully.");

            // Read the record back
            console.write("Reading the record back...");
            RecordStream stream = session.query(dataSet.ids("user-001")).execute();

            if (stream.hasNext()) {
                Record record = stream.next().recordOrThrow();
                console.write("  Retrieved: " + record);
            }

            // Write multiple records
            console.write("Writing batch of records...");
            session.upsert(dataSet)
                .bins("name", "email", "age")
                .id("user-002").values("Bob", "bob@example.com", 25)
                .id("user-003").values("Charlie", "charlie@example.com", 35)
                .id("user-004").values("Diana", "diana@example.com", 28)
                .execute();
            console.write("  Batch written successfully.");

            // Check if records exist
            console.write("Checking record existence...");
            var existsResults = session.exists(dataSet.ids("user-001", "user-002", "user-999")).execute();
            console.write("  Exists results: " + existsResults);

            // Clean up - delete test records
            console.write("Cleaning up test records...");
            session.delete(dataSet.ids("user-001", "user-002", "user-003", "user-004")).execute();
            console.write("  Records deleted.");

        } catch (Exception e) {
            console.error("Operation failed: " + e.getMessage());
        }

        console.write("");
    }

    /**
     * Demonstrate using different behaviors for different types of operations
     */
    private void demonstrateBehaviorSwitching(Cluster cluster) {
        console.write("=== Demonstrating Behavior Switching ===");
        console.write("Different behaviors can be used for different operation types:\n");

        // Fast operations for real-time queries
        Behavior fastBehavior = getBehaviorOrDefault("fast-operations");
        console.write("1. fast-operations: For real-time, latency-sensitive queries");
        showBehaviorSettings(fastBehavior);

        // Safe operations for critical writes
        Behavior safeBehavior = getBehaviorOrDefault("safe-operations");
        console.write("2. safe-operations: For critical operations requiring high reliability");
        showBehaviorSettings(safeBehavior);

        // Batch-optimized operations
        Behavior batchBehavior = getBehaviorOrDefault("batch-fast");
        console.write("3. batch-fast: Optimized for batch operations (inherits from fast-operations)");
        showBehaviorSettings(batchBehavior);

        // Show inheritance
        if (batchBehavior != Behavior.DEFAULT && batchBehavior.getParent() != null) {
            console.write("   (Parent: " + batchBehavior.getParent().name() + ")");
        }

        console.write("\nYou can create different sessions for different operation types:");
        console.write("  Session fastSession = cluster.createSession(fastBehavior);");
        console.write("  Session safeSession = cluster.createSession(safeBehavior);");
    }

    /**
     * Show settings from a behavior for display purposes
     */
    private void showBehaviorSettings(Behavior behavior) {
        ResolvedSettings readSettings = behavior.getSettings(
            Behavior.OpKind.READ,
            Behavior.OpShape.POINT,
            Behavior.Mode.AP
        );

        ResolvedSettings writeSettings = behavior.getSettings(
            Behavior.OpKind.WRITE_RETRYABLE,
            Behavior.OpShape.POINT,
            Behavior.Mode.AP
        );

        if (readSettings != null) {
            console.write("   Read timeout: " + readSettings.getAbandonCallAfterMs());
            console.write("   Max retries: " + readSettings.getMaximumNumberOfCallAttempts());
        }

        if (writeSettings != null) {
            console.write("   Durable delete: " + writeSettings.getUseDurableDelete());
        }

        console.write("");
    }
}
