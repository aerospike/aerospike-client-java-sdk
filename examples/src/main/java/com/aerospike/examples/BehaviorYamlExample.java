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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Settings;

/**
 * Example demonstrating how to load Behavior configurations from YAML files
 * using the new selector-based API.
 *
 * <p>This example shows:
 * <ul>
 *   <li>Loading behaviors from YAML files using Behavior.startMonitoring()</li>
 *   <li>Accessing resolved settings for specific operation types</li>
 *   <li>Using the new getSettings() API</li>
 *   <li>Working with behavior hierarchies (parent-child relationships)</li>
 * </ul>
 */
public class BehaviorYamlExample extends Example {

    public BehaviorYamlExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        String yamlFilePath = "src/main/resources/behavior-example.yml";

        try (Closeable monitor = Behavior.startMonitoringWithResource(yamlFilePath)) {
            // Wait a moment for initial load
            Thread.sleep(500);

            // Get all loaded behaviors
            Set<Behavior> behaviors = Behavior.getAllBehaviors();

            console.write("=== Loaded Behaviors ===");
            console.write("Total behaviors loaded: " + behaviors.size());
            console.write("");

            // Demonstrate each loaded behavior
            for (Behavior behavior : behaviors) {
                console.write("--- Behavior: " + behavior.name() + " ---");

                // Show parent relationship
                if (behavior.getParent() != null) {
                    console.write("  Parent: " + behavior.getParent().name());
                }

                // Get settings for different operation types using the new API
                demonstrateSettings(behavior);
                console.write("");
            }

            // Demonstrate a specific behavior in detail
            Behavior highPerf = Behavior.getBehavior("high-performance");
            if (highPerf != Behavior.DEFAULT) {
                console.write("=== Detailed Example: high-performance Behavior ===");

                // Show resolved settings for retryable writes
                Settings writeSettings = highPerf.getSettings(
                    Behavior.OpKind.WRITE_RETRYABLE,
                    Behavior.OpShape.POINT,
                    Behavior.Mode.AP
                );

                if (writeSettings != null) {
                    console.write("Retryable Write Settings (POINT, AP):");
                    console.write("  abandonCallAfter: " + writeSettings.getAbandonCallAfterMs() + "ms");
                    console.write("  maximumNumberOfCallAttempts: " + writeSettings.getMaximumNumberOfCallAttempts());
                    console.write("  useDurableDelete: " + writeSettings.getUseDurableDelete());
                    console.write("  delayBetweenRetries: " + writeSettings.getDelayBetweenRetriesMs() + "ms");
                }
                console.write("");

                // Show resolved settings for query operations
                Settings querySettings = highPerf.getSettings(
                    Behavior.OpKind.READ,
                    Behavior.OpShape.QUERY,
                    Behavior.Mode.AP
                );

                if (querySettings != null) {
                    console.write("Query Settings (QUERY, AP):");
                    console.write("  recordQueueSize: " + querySettings.getRecordQueueSize());
                    console.write("  maximumNumberOfCallAttempts: " + querySettings.getMaximumNumberOfCallAttempts());
                    console.write("  abandonCallAfter: " + querySettings.getAbandonCallAfterMs() + "ms");
                }
                console.write("");
            }

            // Demonstrate batch-optimized behavior (child of high-performance)
            Behavior batchOpt = Behavior.getBehavior("batch-optimized");
            if (batchOpt != Behavior.DEFAULT) {
                console.write("=== Inheritance Example: batch-optimized (child of high-performance) ===");

                Settings batchReadSettings = batchOpt.getSettings(
                    Behavior.OpKind.READ,
                    Behavior.OpShape.BATCH,
                    Behavior.Mode.AP
                );

                if (batchReadSettings != null) {
                    console.write("Batch Read Settings (inherited + overridden):");
                    console.write("  maxConcurrentNodes: " + batchReadSettings.getMaxConcurrentNodes() + " (overridden from parent)");
                    console.write("  allowInlineMemoryAccess: " + batchReadSettings.getAllowInlineMemoryAccess() + " (overridden from parent)");
                    console.write("  abandonCallAfter: " + batchReadSettings.getAbandonCallAfterMs() + "ms (inherited from parent)");
                    console.write("  maximumNumberOfCallAttempts: " + batchReadSettings.getMaximumNumberOfCallAttempts() + " (inherited from parent)");
                }
            }

        } catch (IOException e) {
            console.error("Error loading behavior from YAML: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            console.write("Interrupted");
        }
    }

    /**
     * Demonstrate getting settings for various operation types
     */
    private void demonstrateSettings(Behavior behavior) {
        // Read operations
        Settings readBatchAp = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
        if (readBatchAp != null) {
            console.write("  Batch reads (AP): maxConcurrentNodes=" + readBatchAp.getMaxConcurrentNodes());
        }

        // Write operations
        Settings writeRetryable = behavior.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
        if (writeRetryable != null) {
            console.write("  Retryable writes: useDurableDelete=" + writeRetryable.getUseDurableDelete());
        }

        // Query operations
        Settings query = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);
        if (query != null) {
            console.write("  Query: recordQueueSize=" + query.getRecordQueueSize());
        }
    }
}
