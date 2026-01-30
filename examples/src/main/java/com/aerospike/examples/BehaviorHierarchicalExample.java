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
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;

/**
 * Example demonstrating the hierarchical YAML configuration system with dynamic reloading.
 *
 * <p>This example monitors a YAML configuration file and displays current behavior settings
 * periodically. When you modify the YAML file, changes will be automatically detected and
 * the new values will be displayed on the next refresh cycle.
 *
 * <h2>Running with run_examples script</h2>
 * <pre>{@code
 * ./run_examples BehaviorHierarchicalExample
 * }</pre>
 *
 * <p>To see dynamic reloading in action:
 * <ol>
 *   <li>Run this example</li>
 *   <li>Edit the behavior-hierarchical-example.yml file</li>
 *   <li>Change a value (e.g., abandonCallAfter, maximumNumberOfCallAttempts)</li>
 *   <li>Save the file</li>
 *   <li>Watch the console output to see the updated values</li>
 * </ol>
 */
public class BehaviorHierarchicalExample extends Example {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/behavior-hierarchical-example.yml";

    public BehaviorHierarchicalExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        demonstrateDynamicReloading(DEFAULT_CONFIG_PATH);
    }

    private void demonstrateDynamicReloading(String yamlFilePath) {
        int refreshDelayMs = 5000;
        int reloadDelayMs = 2000;
        console.write("=== Behavior Hierarchical Example with Dynamic Reloading ===\n");

        try (Closeable monitor = Behavior.startMonitoringWithResource(yamlFilePath, reloadDelayMs)) {
            console.write("Monitoring YAML file: " + yamlFilePath);
            console.write("Reload delay: " + reloadDelayMs +", refresh delay: " + refreshDelayMs);
            console.write("Monitoring active: " + Behavior.isMonitoring());

            // Wait a moment for initial load
            Thread.sleep(1000);

            // Show initial state
            console.write("\n" + getTimestamp() + " === Initial Configuration ===");
            displayCurrentSettings();

            console.write("\n" + "=".repeat(70));
            console.write("Monitoring for changes... Modify the YAML file to see dynamic reloading.");
            console.write("Settings will refresh every 5 seconds. Press Ctrl+C to stop.");
            console.write("=".repeat(70));

            // Keep running and show current settings periodically
            while (true) {
                Thread.sleep(refreshDelayMs);
                console.write("\n" + getTimestamp() + " === Current Configuration ===");
                displayCurrentSettings();
            }

        } catch (IOException e) {
            console.error("Error starting monitoring: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            console.write("\nMonitoring interrupted");
        }
        console.write("Monitoring stopped");
    }

    /**
     * Display current settings for key behaviors to show dynamic updates
     */
    private void displayCurrentSettings() {
        // Display high-performance behavior settings
        displayBehaviorSettings("high-performance");

        // Display high-reliability behavior settings
        displayBehaviorSettings("high-reliability");

        // Display batch-optimized behavior settings (inherits from high-performance)
        displayBehaviorSettings("batch-optimized");

        // Display development behavior settings
        displayBehaviorSettings("development");
    }

    /**
     * Display settings for a specific behavior
     */
    private void displayBehaviorSettings(String behaviorName) {
        Behavior behavior = Behavior.getBehavior(behaviorName);

        if (behavior == Behavior.DEFAULT) {
            console.write("  " + behaviorName + ": (not found, using DEFAULT)");
            return;
        }

        // Get settings for different operation types
        Settings allOpsSettings = behavior.getSettings(OpKind.READ, OpShape.POINT, Behavior.Mode.AP);
        Settings writeSettings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Behavior.Mode.AP);
        Settings batchReadSettings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Behavior.Mode.AP);
        Settings batchWriteSettings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Behavior.Mode.AP);
        Settings querySettings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Behavior.Mode.AP);
        Settings cpReadSettings = behavior.getSettings(OpKind.READ, OpShape.POINT, Behavior.Mode.CP);

        console.write("\n  " + behaviorName + ":");

        // Show parent if exists
        if (behavior.getParent() != null && behavior.getParent() != Behavior.DEFAULT) {
            console.write("    parent: " + behavior.getParent().getName());
        }

        // All operations settings (from point read as baseline)
        if (allOpsSettings != null) {
            console.write("    allOperations:");
            console.write("      abandonCallAfter: " + allOpsSettings.getAbandonCallAfterMs() + "ms");
            console.write("      maximumNumberOfCallAttempts: " + allOpsSettings.getMaximumNumberOfCallAttempts());
            console.write("      delayBetweenRetries: " + allOpsSettings.getDelayBetweenRetriesMs() + "ms");
            console.write("      waitForCallToComplete: " + allOpsSettings.getWaitForCallToCompleteMs() + "ms");
        }

        // Write-specific settings (retryableWrites)
        if (writeSettings != null) {
            console.write("    retryableWrites:");
            console.write("      useDurableDelete: " + writeSettings.getUseDurableDelete());
            console.write("      maximumNumberOfCallAttempts: " + writeSettings.getMaximumNumberOfCallAttempts());
            console.write("      delayBetweenRetries: " + writeSettings.getDelayBetweenRetriesMs() + "ms");
        }

        // Batch read settings
        if (batchReadSettings != null) {
            console.write("    batchReads:");
            console.write("      maxConcurrentServers: " + batchReadSettings.getMaxConcurrentNodes());
            console.write("      allowInlineMemoryAccess: " + batchReadSettings.getAllowInlineMemoryAccess());
            console.write("      allowInlineSsdAccess: " + batchReadSettings.getAllowInlineSsdAccess());
        }

        // Batch write settings
        if (batchWriteSettings != null) {
            console.write("    batchWrites:");
            console.write("      maxConcurrentServers: " + batchWriteSettings.getMaxConcurrentNodes());
            console.write("      abandonCallAfter: " + batchWriteSettings.getAbandonCallAfterMs() + "ms");
        }

        // Query-specific settings
        if (querySettings != null) {
            console.write("    query:");
            console.write("      recordQueueSize: " + querySettings.getRecordQueueSize());
            console.write("      maxConcurrentServers: " + querySettings.getMaxConcurrentNodes());
        }

        // Consistency mode reads (CP mode)
        if (cpReadSettings != null) {
            console.write("    consistencyModeReads:");
            console.write("      readConsistency: " + cpReadSettings.getReadModeSC());
            console.write("      abandonCallAfter: " + cpReadSettings.getAbandonCallAfterMs() + "ms");
        }
    }

    /**
     * Get current timestamp for logging
     */
    private String getTimestamp() {
        return "[" + LocalTime.now().format(TIME_FORMAT) + "]";
    }
}
