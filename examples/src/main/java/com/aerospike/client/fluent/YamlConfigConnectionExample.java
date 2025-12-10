/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.fluent;

import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.util.Util;

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
 * ./run_examples YamlConfigConnectionExample --host db1.example.com --port 3100 --use-alternate
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
public class YamlConfigConnectionExample {
    
    private static final String ENV_CONFIG_URL = "AEROSPIKE_CLIENT_CONFIG_URL";
    
    public static void main(String[] args) {
        // Parse command line arguments
        Args arguments = Args.parse(args);
        
        // Check if the configuration environment variable is set
        String configPath = System.getenv(ENV_CONFIG_URL);
        
        System.out.println("=== Aerospike Fluent Client - YAML Configuration Example ===\n");
        
        if (configPath == null || configPath.isEmpty()) {
            System.out.println("Note: " + ENV_CONFIG_URL + " environment variable is not set.");
            System.out.println("The client will use default settings.\n");
            System.out.println("To use a custom configuration, set the environment variable:");
            System.out.println("  export " + ENV_CONFIG_URL + "=/path/to/client-config-example.yml\n");
        } else {
            System.out.println("Configuration file: " + configPath);
            System.out.println("The client will load behaviors and settings from this file.\n");
        }
        
        System.out.println("Connection settings: " + arguments);
        System.out.println("Connecting to Aerospike cluster at " + arguments.getHost() + ":" + arguments.getPort());
        if (arguments.isUseAlternate()) {
            System.out.println("Using alternate services for cluster discovery");
        }
        
        // Create cluster definition
        // When connect() is called, it automatically checks for AEROSPIKE_CLIENT_CONFIG_URL
        // and loads the configuration if the environment variable is set
        ClusterDefinition clusterDef = new ClusterDefinition(arguments.getHost(), arguments.getPort())
            .appId("yaml-config-example")
            .failIfNotConnected(true);
        
        if (arguments.isUseAlternate()) {
            clusterDef.usingServicesAlternate();
        }
        
        try (Cluster cluster = clusterDef.connect()) {
            System.out.println("Successfully connected to cluster!\n");
            
            // Show if behaviors were loaded from YAML
            demonstrateBehaviorLoading();
            
            // Create a session - use a behavior from YAML if available, otherwise DEFAULT
            Behavior behavior = getBehaviorOrDefault("fast-operations");
            Session session = cluster.createSession(behavior);
            
            System.out.println("Using behavior: " + behavior.name() + "\n");
            
            // Perform example operations
            performExampleOperations(session);
            
            // Demonstrate using different behaviors for different operations
            demonstrateBehaviorSwitching(cluster);
            
        } catch (Throwable t) {
            System.err.println("Error: " + Util.getErrorMessage(t));
            t.printStackTrace();
        } finally {
            // Clean up monitoring if it was started
            if (Behavior.isMonitoring()) {
                Behavior.shutdownMonitor();
                System.out.println("\nBehavior monitoring stopped.");
            }
        }
    }
    
    /**
     * Demonstrate that behaviors were loaded from YAML configuration
     */
    private static void demonstrateBehaviorLoading() {
        System.out.println("=== Behavior Configuration Status ===");
        System.out.println("Monitoring active: " + Behavior.isMonitoring());
        
        // Check for behaviors defined in our example YAML
        String[] expectedBehaviors = {"fast-operations", "safe-operations", "batch-fast"};
        
        for (String behaviorName : expectedBehaviors) {
            Behavior behavior = Behavior.getBehavior(behaviorName);
            if (behavior != Behavior.DEFAULT) {
                System.out.println("  Found behavior: " + behaviorName);
                
                // Show some settings from the behavior
                Settings settings = behavior.getSettings(
                    Behavior.OpKind.READ, 
                    Behavior.OpShape.POINT, 
                    Behavior.Mode.AP
                );
                if (settings != null) {
                    System.out.println("    - abandonCallAfter: " + settings.getAbandonCallAfterMs() + "ms");
                    System.out.println("    - maxRetries: " + settings.getMaximumNumberOfCallAttempts());
                }
            } else {
                System.out.println("  Behavior not found (using DEFAULT): " + behaviorName);
            }
        }
        System.out.println();
    }
    
    /**
     * Get a named behavior or fall back to DEFAULT if not found
     */
    private static Behavior getBehaviorOrDefault(String behaviorName) {
        Behavior behavior = Behavior.getBehavior(behaviorName);
        if (behavior == Behavior.DEFAULT) {
            System.out.println("Behavior '" + behaviorName + "' not found in configuration, using DEFAULT");
        }
        return behavior;
    }
    
    /**
     * Perform example CRUD operations using the session
     */
    private static void performExampleOperations(Session session) {
        System.out.println("=== Performing Example Operations ===");
        
        DataSet dataSet = DataSet.of("test", "yaml-config-demo");
        
        try {
            // Write a record
            System.out.println("Writing a test record...");
            session.upsert(dataSet.ids("user-001"))
                .bins("name", "email", "age")
                .values("Alice", "alice@example.com", 30)
                .execute();
            System.out.println("  Record written successfully.");
            
            // Read the record back
            System.out.println("Reading the record back...");
            RecordStream stream = session.query(dataSet.ids("user-001")).execute();
            
            if (stream.hasNext()) {
                Record record = stream.next().recordOrThrow();
                System.out.println("  Retrieved: " + record);
            }
            
            // Write multiple records
            System.out.println("Writing batch of records...");
            session.upsert(dataSet.ids("user-002", "user-003", "user-004"))
                .bins("name", "email", "age")
                .values("Bob", "bob@example.com", 25)
                .values("Charlie", "charlie@example.com", 35)
                .values("Diana", "diana@example.com", 28)
                .execute();
            System.out.println("  Batch written successfully.");
            
            // Check if records exist
            System.out.println("Checking record existence...");
            var existsResults = session.exists(dataSet.ids("user-001", "user-002", "user-999")).execute();
            System.out.println("  Exists results: " + existsResults);
            
            // Clean up - delete test records
            System.out.println("Cleaning up test records...");
            session.delete(dataSet.ids("user-001", "user-002", "user-003", "user-004")).execute();
            System.out.println("  Records deleted.");
            
        } catch (Exception e) {
            System.err.println("Operation failed: " + e.getMessage());
        }
        
        System.out.println();
    }
    
    /**
     * Demonstrate using different behaviors for different types of operations
     */
    private static void demonstrateBehaviorSwitching(Cluster cluster) {
        System.out.println("=== Demonstrating Behavior Switching ===");
        System.out.println("Different behaviors can be used for different operation types:\n");
        
        // Fast operations for real-time queries
        Behavior fastBehavior = getBehaviorOrDefault("fast-operations");
        System.out.println("1. fast-operations: For real-time, latency-sensitive queries");
        showBehaviorSettings(fastBehavior);
        
        // Safe operations for critical writes
        Behavior safeBehavior = getBehaviorOrDefault("safe-operations");
        System.out.println("2. safe-operations: For critical operations requiring high reliability");
        showBehaviorSettings(safeBehavior);
        
        // Batch-optimized operations
        Behavior batchBehavior = getBehaviorOrDefault("batch-fast");
        System.out.println("3. batch-fast: Optimized for batch operations (inherits from fast-operations)");
        showBehaviorSettings(batchBehavior);
        
        // Show inheritance
        if (batchBehavior != Behavior.DEFAULT && batchBehavior.getParent() != null) {
            System.out.println("   (Parent: " + batchBehavior.getParent().name() + ")");
        }
        
        System.out.println("\nYou can create different sessions for different operation types:");
        System.out.println("  Session fastSession = cluster.createSession(fastBehavior);");
        System.out.println("  Session safeSession = cluster.createSession(safeBehavior);");
    }
    
    /**
     * Show settings from a behavior for display purposes
     */
    private static void showBehaviorSettings(Behavior behavior) {
        Settings readSettings = behavior.getSettings(
            Behavior.OpKind.READ, 
            Behavior.OpShape.POINT, 
            Behavior.Mode.AP
        );
        
        Settings writeSettings = behavior.getSettings(
            Behavior.OpKind.WRITE_RETRYABLE, 
            Behavior.OpShape.POINT, 
            Behavior.Mode.AP
        );
        
        if (readSettings != null) {
            System.out.println("   Read timeout: " + readSettings.getAbandonCallAfterMs());
            System.out.println("   Max retries: " + readSettings.getMaximumNumberOfCallAttempts());
        }
        
        if (writeSettings != null) {
            System.out.println("   Durable delete: " + writeSettings.getUseDurableDelete());
        }
        
        System.out.println();
    }
}

