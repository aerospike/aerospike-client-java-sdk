package com.aerospike.client.fluent.policy;

import java.io.File;
import java.util.Map;

/**
 * Example demonstrating how to load Behavior configurations from YAML files
 * using the new selector-based API.
 * 
 * <p>This example shows:
 * <ul>
 *   <li>Loading behaviors from YAML files</li>
 *   <li>Accessing resolved settings for specific operation types</li>
 *   <li>Using the new getSettings() API</li>
 *   <li>Working with behavior hierarchies (parent-child relationships)</li>
 * </ul>
 */
public class BehaviorYamlExample {
    
    // TODO: move to unit tests
    public static void main(String[] args) {
        try {
            // Load all behaviors from YAML file
            File yamlFile = new File("src/main/resources/behavior-example.yml");
            Map<String, Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromFile(yamlFile);
            
            System.out.println("=== Loaded Behaviors ===");
            System.out.println("Total behaviors loaded: " + behaviors.size());
            System.out.println();
            
            // Demonstrate each loaded behavior
            for (Map.Entry<String, Behavior> entry : behaviors.entrySet()) {
                Behavior behavior = entry.getValue();
                System.out.println("--- Behavior: " + behavior.name() + " ---");
                
                // Show parent relationship
                if (behavior.getParent() != null) {
                    System.out.println("  Parent: " + behavior.getParent().name());
                }
                
                // Get settings for different operation types using the new API
                demonstrateSettings(behavior);
                System.out.println();
            }
            
            // Demonstrate a specific behavior in detail
            if (behaviors.containsKey("high-performance")) {
                System.out.println("=== Detailed Example: high-performance Behavior ===");
                Behavior highPerf = behaviors.get("high-performance");
                
                // Show resolved settings for retryable writes
                Settings writeSettings = highPerf.getSettings(
                    Behavior.OpKind.WRITE_RETRYABLE, 
                    Behavior.OpShape.POINT, 
                    Behavior.Mode.AP
                );
                
                if (writeSettings != null) {
                    System.out.println("Retryable Write Settings (POINT, AP):");
                    System.out.println("  abandonCallAfter: " + writeSettings.abandonCallAfter);
                    System.out.println("  maximumNumberOfCallAttempts: " + writeSettings.maximumNumberOfCallAttempts);
                    System.out.println("  useDurableDelete: " + writeSettings.useDurableDelete);
                    System.out.println("  delayBetweenRetries: " + writeSettings.delayBetweenRetries);
                }
                System.out.println();
                
                // Show resolved settings for query operations
                Settings querySettings = highPerf.getSettings(
                    Behavior.OpKind.READ, 
                    Behavior.OpShape.QUERY, 
                    Behavior.Mode.AP
                );
                
                if (querySettings != null) {
                    System.out.println("Query Settings (QUERY, AP):");
                    System.out.println("  recordQueueSize: " + querySettings.recordQueueSize);
                    System.out.println("  maximumNumberOfCallAttempts: " + querySettings.maximumNumberOfCallAttempts);
                    System.out.println("  abandonCallAfter: " + querySettings.abandonCallAfter);
                }
                System.out.println();
            }
            
            // Demonstrate batch-optimized behavior (child of high-performance)
            if (behaviors.containsKey("batch-optimized")) {
                System.out.println("=== Inheritance Example: batch-optimized (child of high-performance) ===");
                Behavior batchOpt = behaviors.get("batch-optimized");
                
                Settings batchReadSettings = batchOpt.getSettings(
                    Behavior.OpKind.READ, 
                    Behavior.OpShape.BATCH, 
                    Behavior.Mode.AP
                );
                
                if (batchReadSettings != null) {
                    System.out.println("Batch Read Settings (inherited + overridden):");
                    System.out.println("  maxConcurrentNodes: " + batchReadSettings.maxConcurrentNodes + " (overridden from parent)");
                    System.out.println("  allowInlineMemoryAccess: " + batchReadSettings.allowInlineMemoryAccess + " (overridden from parent)");
                    System.out.println("  abandonCallAfter: " + batchReadSettings.abandonCallAfter + " (inherited from parent)");
                    System.out.println("  maximumNumberOfCallAttempts: " + batchReadSettings.maximumNumberOfCallAttempts + " (inherited from parent)");
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error loading behavior from YAML: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Demonstrate getting settings for various operation types
     */
    private static void demonstrateSettings(Behavior behavior) {
        // Read operations
        Settings readBatchAp = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.BATCH, Behavior.Mode.AP);
        if (readBatchAp != null && readBatchAp.maxConcurrentNodes != null) {
            System.out.println("  Batch reads (AP): maxConcurrentNodes=" + readBatchAp.maxConcurrentNodes);
        }
        
        // Write operations
        Settings writeRetryable = behavior.getSettings(Behavior.OpKind.WRITE_RETRYABLE, Behavior.OpShape.POINT, Behavior.Mode.AP);
        if (writeRetryable != null && writeRetryable.useDurableDelete != null) {
            System.out.println("  Retryable writes: useDurableDelete=" + writeRetryable.useDurableDelete);
        }
        
        // Query operations
        Settings query = behavior.getSettings(Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.AP);
        if (query != null && query.recordQueueSize != null) {
            System.out.println("  Query: recordQueueSize=" + query.recordQueueSize);
        }
    }
} 