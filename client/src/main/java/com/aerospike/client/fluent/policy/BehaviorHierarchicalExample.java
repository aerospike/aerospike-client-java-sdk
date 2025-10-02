package com.aerospike.client.fluent.policy;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Example demonstrating the hierarchical YAML configuration system with dynamic reloading
 */
public class BehaviorHierarchicalExample {
    
    public static void main(String[] args) {
        // Example 1: Using try-with-resources (recommended)
        demonstrateTryWithResources();
        
        // Example 2: Manual monitoring (legacy approach)
        demonstrateManualMonitoring();
    }
    
    private static void demonstrateTryWithResources() {
        System.out.println("=== Example 1: Using try-with-resources ===");
        
        String yamlFilePath = "src/main/resources/behavior-example.yml";
        
        try (Closeable monitor = Behavior.startMonitoringWithResource(yamlFilePath, 2000)) {
            System.out.println("Started monitoring: " + yamlFilePath);
            System.out.println("Monitoring active: " + Behavior.isMonitoring());
            
            // Wait a moment for initial load
            Thread.sleep(1000);
            
            // Demonstrate behavior lookup
            demonstrateBehaviorLookup();
            
            // Demonstrate tree traversal
            demonstrateTreeTraversal();
            
            // Demonstrate policy inheritance
            demonstratePolicyInheritance();
            
            // Keep running to demonstrate dynamic reloading
            System.out.println("\nMonitoring for changes... (modify the YAML file to see dynamic reloading)");
            System.out.println("Press Ctrl+C to stop");
            
            // Keep the application running
            while (true) {
                Thread.sleep(5000);
                System.out.println("Still monitoring... (" + Behavior.isMonitoring() + ")");
            }
            
        } catch (IOException e) {
            System.err.println("Error starting monitoring: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("Monitoring interrupted");
        }
        // Monitor is automatically closed when exiting try block
        System.out.println("Monitoring automatically stopped");
    }
    
    private static void demonstrateManualMonitoring() {
        System.out.println("\n=== Example 2: Manual monitoring ===");
        
        try {
            // Start monitoring the YAML file for changes
            String yamlFilePath = "src/main/resources/behavior-example.yml";
            Behavior.startMonitoring(yamlFilePath, 2000); // 2 second reload delay
            
            System.out.println("Started monitoring: " + yamlFilePath);
            System.out.println("Monitoring active: " + Behavior.isMonitoring());
            
            // Wait a moment for initial load
            Thread.sleep(1000);
            
            // Demonstrate behavior lookup
            demonstrateBehaviorLookup();
            
            // Demonstrate tree traversal
            demonstrateTreeTraversal();
            
            // Demonstrate policy inheritance
            demonstratePolicyInheritance();
            
            // Keep running to demonstrate dynamic reloading
            System.out.println("\nMonitoring for changes... (modify the YAML file to see dynamic reloading)");
            System.out.println("Press Ctrl+C to stop");
            
            // Keep the application running
            while (true) {
                Thread.sleep(5000);
                System.out.println("Still monitoring... (" + Behavior.isMonitoring() + ")");
            }
            
        } catch (IOException e) {
            System.err.println("Error starting monitoring: " + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("Monitoring interrupted");
        } finally {
            // Cleanup
            Behavior.shutdownMonitor();
            System.out.println("Monitoring stopped");
        }
    }
    
    private static void demonstrateBehaviorLookup() {
        System.out.println("\n=== Behavior Lookup Examples ===");
        
        // Get behaviors by name
        Behavior highPerf = Behavior.getBehavior("high-performance");
        Behavior highRel = Behavior.getBehavior("high-reliability");
        Behavior batchOpt = Behavior.getBehavior("batch-optimized");
        Behavior dev = Behavior.getBehavior("development");
        
        System.out.println("High Performance behavior: " + highPerf.getName());
        System.out.println("High Reliability behavior: " + highRel.getName());
        System.out.println("Batch Optimized behavior: " + batchOpt.getName());
        System.out.println("Development behavior: " + dev.getName());
        
        // Get non-existent behavior (returns DEFAULT)
        Behavior nonExistent = Behavior.getBehavior("non-existent");
        System.out.println("Non-existent behavior returns: " + nonExistent.getName());
    }
    
    private static void demonstrateTreeTraversal() {
        System.out.println("\n=== Tree Traversal Examples ===");
        
        // Find behaviors in the tree using Optional
        Optional<Behavior> found = Behavior.DEFAULT.findBehavior("high-performance");
        System.out.println("Found 'high-performance' from DEFAULT: " + 
            found.map(Behavior::getName).orElse("not found"));
        
        found = Behavior.DEFAULT.findBehavior("batch-optimized");
        System.out.println("Found 'batch-optimized' from DEFAULT: " + 
            found.map(Behavior::getName).orElse("not found"));
        
        // Find from a specific node
        Behavior highPerf = Behavior.getBehavior("high-performance");
        found = highPerf.findBehavior("batch-optimized");
        System.out.println("Found 'batch-optimized' from 'high-performance': " + 
            found.map(Behavior::getName).orElse("not found"));
        
        // Show children
        System.out.println("DEFAULT children: " + Behavior.DEFAULT.getChildren().size());
        System.out.println("High Performance children: " + highPerf.getChildren().size());
        
        // Demonstrate Optional usage patterns
        found.ifPresent(behavior -> 
            System.out.println("Found behavior: " + behavior.getName()));
        
        // Or use orElse for default behavior
        Behavior behavior = found.orElse(Behavior.DEFAULT);
        System.out.println("Using behavior: " + behavior.getName());
    }
    
    private static void demonstratePolicyInheritance() {
        System.out.println("\n=== Policy Inheritance Examples ===");
        
        // Get policies from different behaviors
        var defaultWritePolicy = Behavior.DEFAULT.getSharedPolicy(Behavior.CommandType.WRITE_RETRYABLE);
        var highPerfWritePolicy = Behavior.getBehavior("high-performance").getSharedPolicy(Behavior.CommandType.WRITE_RETRYABLE);
        var batchOptWritePolicy = Behavior.getBehavior("batch-optimized").getSharedPolicy(Behavior.CommandType.WRITE_RETRYABLE);
        
        System.out.println("DEFAULT write policy max retries: " + defaultWritePolicy.maxRetries);
        System.out.println("High Performance write policy max retries: " + highPerfWritePolicy.maxRetries);
        System.out.println("Batch Optimized write policy max retries: " + batchOptWritePolicy.maxRetries);
        
        System.out.println("DEFAULT write policy sleep between retries: " + defaultWritePolicy.sleepBetweenRetries);
        System.out.println("High Performance write policy sleep between retries: " + highPerfWritePolicy.sleepBetweenRetries);
        System.out.println("Batch Optimized write policy sleep between retries: " + batchOptWritePolicy.sleepBetweenRetries);
        
        // Show inheritance chain
        Behavior batchOpt = Behavior.getBehavior("batch-optimized");
        System.out.println("Batch Optimized parent: " + batchOpt.getParent().getName());
        System.out.println("Batch Optimized grandparent: " + batchOpt.getParent().getParent().getName());
    }
} 