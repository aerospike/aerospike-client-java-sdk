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
package com.aerospike.examples;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.SystemSettings;
import com.aerospike.client.fluent.SystemSettingsRegistry;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.util.Util;

/**
 * Demonstrates all available YAML configuration options including system settings,
 * behaviors, and inheritance.
 *
 * <p>Usage: {@code ./run_examples CompleteYamlConfigExample -h localhost -p 3000}</p>
 */
public class CompleteYamlConfigExample extends Example {

    private static final String CONFIG_FILE = "src/main/resources/complete-config-example.yml";

    public CompleteYamlConfigExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        console.write("=== Complete YAML Configuration Example ===\n");

        try (Closeable monitor = Behavior.startMonitoringWithResource(CONFIG_FILE)) {
            Thread.sleep(500);

            displaySystemSettings();
            displayAllBehaviors();
            demonstrateBehaviorInheritance();

            if (args.host != null) {
                performClusterOperations(args);
            } else {
                console.write("\n=== Skipping Cluster Operations ===");
                console.write("No host specified. Add -h <host> to perform actual operations.\n");
            }

        } catch (IOException e) {
            console.error("Error loading configuration: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            console.write("Interrupted");
        }
    }

    private void displaySystemSettings() {
        console.write("=== SYSTEM SETTINGS ===\n");

        SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();

        console.write("--- DEFAULT System Settings ---");
        displaySystemSettingsDetails(registry.getDefaultSettings());

        String[] clusterNames = {"production", "development", "bncluster"};
        for (String clusterName : clusterNames) {
            SystemSettings clusterSettings = registry.getClusterSettings(clusterName);
            if (clusterSettings != null) {
                console.write("--- Cluster '" + clusterName + "' System Settings ---");
                displaySystemSettingsDetails(clusterSettings);
            }
        }
    }

    private void displaySystemSettingsDetails(SystemSettings settings) {
        if (settings == null) {
            console.write("  (not configured)");
            return;
        }

        console.write("  Connections:");
        console.write("    minimumConnectionsPerNode: " + settings.getMinimumConnectionsPerNode());
        console.write("    maximumConnectionsPerNode: " + settings.getMaximumConnectionsPerNode());
        console.write("    maximumSocketIdleTime: " + formatDuration(settings.getMaximumSocketIdleTime()));

        console.write("  Circuit Breaker:");
        console.write("    numTendIntervalsInErrorWindow: " + settings.getNumTendIntervalsInErrorWindow());
        console.write("    maximumErrorsInErrorWindow: " + settings.getMaximumErrorsInErrorWindow());

        console.write("  Refresh:");
        console.write("    tendInterval: " + formatDuration(settings.getTendInterval()));
        console.write("");
    }

    private void displayAllBehaviors() {
        console.write("=== BEHAVIOR DEFINITIONS ===\n");

        Set<Behavior> behaviors = Behavior.getAllBehaviors();
        console.write("Total behaviors loaded: " + behaviors.size() + "\n");

        String[] behaviorNames = {
            "high-performance", "high-reliability", "batch-optimized",
            "development", "analytics", "real-time", "cache-refresh"
        };

        for (String name : behaviorNames) {
            Behavior behavior = Behavior.getBehavior(name);
            if (behavior != Behavior.DEFAULT) {
                displayBehaviorDetails(behavior);
            }
        }
    }

    private void displayBehaviorDetails(Behavior behavior) {
        console.write("--- Behavior: " + behavior.name() + " ---");

        if (behavior.getParent() != null && behavior.getParent() != Behavior.DEFAULT) {
            console.write("  Parent: " + behavior.getParent().name());
        }

        Settings readAP = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);
        if (readAP != null) {
            console.write("  Point Reads (AP):");
            console.write("    abandonCallAfter: " + readAP.getAbandonCallAfterMs() + "ms");
            console.write("    maximumNumberOfCallAttempts: " + readAP.getMaximumNumberOfCallAttempts());
            console.write("    delayBetweenRetries: " + readAP.getDelayBetweenRetriesMs() + "ms");
            console.write("    resetTtlOnReadAtPercent: " + readAP.getResetTtlOnReadAtPercent() + "%");
        }

        Settings readCP = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.CP);
        if (readCP != null) {
            console.write("  Point Reads (CP):");
            console.write("    abandonCallAfter: " + readCP.getAbandonCallAfterMs() + "ms");
            console.write("    readModeSC: " + readCP.getReadModeSC());
        }

        Settings writeRetryable = behavior.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
        if (writeRetryable != null) {
            console.write("  Retryable Writes:");
            console.write("    abandonCallAfter: " + writeRetryable.getAbandonCallAfterMs() + "ms");
            console.write("    maximumNumberOfCallAttempts: " + writeRetryable.getMaximumNumberOfCallAttempts());
            console.write("    useDurableDelete: " + writeRetryable.getUseDurableDelete());
        }

        Settings writeNonRetryable = behavior.getSettings(Behavior.OpKind.WRITE_NON_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
        if (writeNonRetryable != null) {
            console.write("  Non-Retryable Writes:");
            console.write("    abandonCallAfter: " + writeNonRetryable.getAbandonCallAfterMs() + "ms");
            console.write("    useDurableDelete: " + writeNonRetryable.getUseDurableDelete());
        }

        Settings batchRead = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
        if (batchRead != null) {
            console.write("  Batch Reads:");
            console.write("    abandonCallAfter: " + batchRead.getAbandonCallAfterMs() + "ms");
            console.write("    maxConcurrentNodes: " + batchRead.getMaxConcurrentNodes());
            console.write("    allowInlineMemoryAccess: " + batchRead.getAllowInlineMemoryAccess());
            console.write("    allowInlineSsdAccess: " + batchRead.getAllowInlineSsdAccess());
        }

        Settings batchWrite = behavior.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.BATCH, Behavior.Mode.AP);
        if (batchWrite != null) {
            console.write("  Batch Writes:");
            console.write("    abandonCallAfter: " + batchWrite.getAbandonCallAfterMs() + "ms");
            console.write("    maxConcurrentNodes: " + batchWrite.getMaxConcurrentNodes());
        }

        Settings query = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);
        if (query != null) {
            console.write("  Query:");
            console.write("    abandonCallAfter: " + query.getAbandonCallAfterMs() + "ms");
            console.write("    recordQueueSize: " + query.getRecordQueueSize());
        }

        console.write("");
    }

    private void demonstrateBehaviorInheritance() {
        console.write("=== BEHAVIOR INHERITANCE ===\n");

        Behavior parent = Behavior.getBehavior("high-performance");
        Behavior child = Behavior.getBehavior("batch-optimized");

        if (parent != Behavior.DEFAULT && child != Behavior.DEFAULT) {
            console.write("Comparing 'batch-optimized' (child) with 'high-performance' (parent):\n");

            Settings parentBatch = parent.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
            Settings childBatch = child.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);

            console.write("Batch Reads - maxConcurrentNodes:");
            console.write("  high-performance (parent): " + parentBatch.getMaxConcurrentNodes());
            console.write("  batch-optimized (child):   " + childBatch.getMaxConcurrentNodes() + " (overridden)");

            Settings parentPoint = parent.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);
            Settings childPoint = child.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);

            console.write("\nPoint Reads - abandonCallAfter:");
            console.write("  high-performance (parent): " + parentPoint.getAbandonCallAfterMs() + "ms");
            console.write("  batch-optimized (child):   " + childPoint.getAbandonCallAfterMs() + "ms (inherited)");
        }

        Behavior analytics = Behavior.getBehavior("analytics");
        Behavior reliability = Behavior.getBehavior("high-reliability");

        if (analytics != Behavior.DEFAULT && reliability != Behavior.DEFAULT) {
            console.write("\nComparing 'analytics' (child) with 'high-reliability' (parent):\n");

            Settings parentQuery = reliability.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);
            Settings childQuery = analytics.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);

            console.write("Query - recordQueueSize:");
            console.write("  high-reliability (parent): " + parentQuery.getRecordQueueSize());
            console.write("  analytics (child):         " + childQuery.getRecordQueueSize() + " (overridden)");
        }

        Behavior cacheRefresh = Behavior.getBehavior("cache-refresh");
        if (cacheRefresh != Behavior.DEFAULT) {
            console.write("\nDemonstrating 'cache-refresh' with resetTtlOnReadAtPercent:\n");

            Settings cacheRead = cacheRefresh.getSettings(Behavior.OpKind.READ, Behavior.OpShape.POINT, Behavior.Mode.AP);
            if (cacheRead != null) {
                console.write("  resetTtlOnReadAtPercent: " + cacheRead.getResetTtlOnReadAtPercent() + "%");
            }
        }

        console.write("");
    }

    private void performClusterOperations(Args args) {
        console.write("=== CLUSTER OPERATIONS ===\n");

        ClusterDefinition clusterDef = new ClusterDefinition(args.host, args.port)
            .appId("complete-yaml-example")
            .failIfNotConnected(true);

        if (args.useServicesAlternate) {
            clusterDef.usingServicesAlternate();
        }

        try (Cluster cluster = clusterDef.connect()) {
            console.write("Connected to cluster at " + args.host + ":" + args.port + "\n");

            DataSet dataSet = DataSet.of(args.namespace, "complete-yaml-demo");

            testWithBehavior(cluster, "high-performance", dataSet);
            testWithBehavior(cluster, "high-reliability", dataSet);
            testWithBehavior(cluster, "batch-optimized", dataSet);
            testWithBehavior(cluster, "real-time", dataSet);

            console.write("All operations completed successfully!\n");

        } catch (Throwable t) {
            console.error("Cluster operation failed: " + Util.getErrorMessage(t));
            t.printStackTrace();
        }
    }

    private void testWithBehavior(Cluster cluster, String behaviorName, DataSet dataSet) {
        Behavior behavior = Behavior.getBehavior(behaviorName);

        if (behavior == Behavior.DEFAULT) {
            console.write("Behavior '" + behaviorName + "' not found, skipping...");
            return;
        }

        console.write("Testing with behavior: " + behaviorName);

        try {
            Session session = cluster.createSession(behavior);
            String testKey = "test-" + behaviorName;

            session.upsert(dataSet.ids(testKey))
                .bins("behavior", "timestamp", "value")
                .values(behaviorName, System.currentTimeMillis(), 42)
                .execute();
            console.write("  Write: OK");

            RecordStream stream = session.query(dataSet.ids(testKey)).execute();
            if (stream.hasNext()) {
                Record record = stream.next().recordOrThrow();
                console.write("  Read: OK - " + record.getString("behavior"));
            }

            var existsResults = session.exists(dataSet.ids(testKey, testKey + "-2")).execute();
            console.write("  Batch exists: OK - " + existsResults);

            session.delete(dataSet.ids(testKey)).execute();
            console.write("  Delete: OK");

        } catch (Exception e) {
            console.error("  Error: " + e.getMessage());
        }

        console.write("");
    }

    private String formatDuration(Duration duration) {
        if (duration == null) {
            return "null";
        }
        long millis = duration.toMillis();
        if (millis >= 1000 && millis % 1000 == 0) {
            return (millis / 1000) + "s";
        }
        return millis + "ms";
    }
}
