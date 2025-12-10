package com.aerospike.client.fluent;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

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
public class BehaviorHierarchicalExample {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/behavior-hierarchical-example.yml";

    public static void main(String[] args) {
        Args.parse(args); // Parse args for --help support
        demonstrateDynamicReloading(DEFAULT_CONFIG_PATH);
    }

    private static void demonstrateDynamicReloading(String yamlFilePath) {
        int refreshDelayMs = 5000;
        int reloadDelayMs = 2000;
        System.out.println("=== Behavior Hierarchical Example with Dynamic Reloading ===\n");

        try (Closeable monitor = Behavior.startMonitoringWithResource(yamlFilePath, reloadDelayMs)) {
            System.out.println("Monitoring YAML file: " + yamlFilePath);
            System.out.println("Reload delay: " + reloadDelayMs +", refresh delay: " + refreshDelayMs);
            System.out.println("Monitoring active: " + Behavior.isMonitoring());

            // Wait a moment for initial load
            Thread.sleep(1000);

            // Show initial state
            System.out.println("\n" + getTimestamp() + " === Initial Configuration ===");
            displayCurrentSettings();

            System.out.println("\n" + "=".repeat(70));
            System.out.println("Monitoring for changes... Modify the YAML file to see dynamic reloading.");
            System.out.println("Settings will refresh every 5 seconds. Press Ctrl+C to stop.");
            System.out.println("=".repeat(70));

            // Keep running and show current settings periodically
            while (true) {
                Thread.sleep(refreshDelayMs);
                System.out.println("\n" + getTimestamp() + " === Current Configuration ===");
                displayCurrentSettings();
            }

        } catch (IOException e) {
            System.err.println("Error starting monitoring: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("\nMonitoring interrupted");
        }
        System.out.println("Monitoring stopped");
    }

    /**
     * Display current settings for key behaviors to show dynamic updates
     */
    private static void displayCurrentSettings() {
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
    private static void displayBehaviorSettings(String behaviorName) {
        Behavior behavior = Behavior.getBehavior(behaviorName);

        if (behavior == Behavior.DEFAULT) {
            System.out.println("  " + behaviorName + ": (not found, using DEFAULT)");
            return;
        }

        // Get settings for different operation types
        Settings allOpsSettings = behavior.getSettings(OpKind.READ, OpShape.POINT, Behavior.Mode.AP);
        Settings writeSettings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Behavior.Mode.AP);
        Settings batchReadSettings = behavior.getSettings(OpKind.READ, OpShape.BATCH, Behavior.Mode.AP);
        Settings batchWriteSettings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Behavior.Mode.AP);
        Settings querySettings = behavior.getSettings(OpKind.READ, OpShape.QUERY, Behavior.Mode.AP);
        Settings cpReadSettings = behavior.getSettings(OpKind.READ, OpShape.POINT, Behavior.Mode.CP);

        System.out.println("\n  " + behaviorName + ":");

        // Show parent if exists
        if (behavior.getParent() != null && behavior.getParent() != Behavior.DEFAULT) {
            System.out.println("    parent: " + behavior.getParent().getName());
        }

        // All operations settings (from point read as baseline)
        if (allOpsSettings != null) {
            System.out.println("    allOperations:");
            System.out.println("      abandonCallAfter: " + allOpsSettings.getAbandonCallAfterMs() + "ms");
            System.out.println("      maximumNumberOfCallAttempts: " + allOpsSettings.getMaximumNumberOfCallAttempts());
            System.out.println("      delayBetweenRetries: " + allOpsSettings.getDelayBetweenRetriesMs() + "ms");
            System.out.println("      waitForCallToComplete: " + allOpsSettings.getWaitForCallToCompleteMs() + "ms");
        }

        // Write-specific settings (retryableWrites)
        if (writeSettings != null) {
            System.out.println("    retryableWrites:");
            System.out.println("      useDurableDelete: " + writeSettings.getUseDurableDelete());
            System.out.println("      maximumNumberOfCallAttempts: " + writeSettings.getMaximumNumberOfCallAttempts());
            System.out.println("      delayBetweenRetries: " + writeSettings.getDelayBetweenRetriesMs() + "ms");
        }

        // Batch read settings
        if (batchReadSettings != null) {
            System.out.println("    batchReads:");
            System.out.println("      maxConcurrentServers: " + batchReadSettings.getMaxConcurrentNodes());
            System.out.println("      allowInlineMemoryAccess: " + batchReadSettings.getAllowInlineMemoryAccess());
            System.out.println("      allowInlineSsdAccess: " + batchReadSettings.getAllowInlineSsdAccess());
        }

        // Batch write settings
        if (batchWriteSettings != null) {
            System.out.println("    batchWrites:");
            System.out.println("      maxConcurrentServers: " + batchWriteSettings.getMaxConcurrentNodes());
            System.out.println("      abandonCallAfter: " + batchWriteSettings.getAbandonCallAfterMs() + "ms");
        }

        // Query-specific settings
        if (querySettings != null) {
            System.out.println("    query:");
            System.out.println("      recordQueueSize: " + querySettings.getRecordQueueSize());
            System.out.println("      maxConcurrentServers: " + querySettings.getMaxConcurrentNodes());
        }

        // Consistency mode reads (CP mode)
        if (cpReadSettings != null) {
            System.out.println("    consistencyModeReads:");
            System.out.println("      readConsistency: " + cpReadSettings.getReadModeSC());
            System.out.println("      abandonCallAfter: " + cpReadSettings.getAbandonCallAfterMs() + "ms");
        }
    }

    /**
     * Get current timestamp for logging
     */
    private static String getTimestamp() {
        return "[" + LocalTime.now().format(TIME_FORMAT) + "]";
    }
}
