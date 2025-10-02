package com.aerospike.client.fluent.policy;

import java.io.File;

/**
 * Example demonstrating how to load Behavior configurations from YAML files
 */
public class BehaviorYamlExample {

    // TODO: move to unit tests
    public static void main(String[] args) {
        try {
            // Load behavior from YAML file
            File yamlFile = new File("src/main/resources/behavior-example.yml");
            Behavior customBehavior = BehaviorYamlLoader.loadFromFile(yamlFile);

            System.out.println("Loaded behavior: " + customBehavior.getName());
            System.out.println("Exception policy: " + customBehavior.getExceptionPolicy());
            System.out.println("Send key: " + customBehavior.getSendKey());
            System.out.println("Use compression: " + customBehavior.getUseCompression());

            // Get policies from the loaded behavior
            var writePolicy = customBehavior.getSharedPolicy(Behavior.CommandType.WRITE_RETRYABLE);
            System.out.println("Write policy max retries: " + writePolicy.maxRetries);
            System.out.println("Write policy sleep between retries: " + writePolicy.sleepBetweenRetries);

            QueryPolicy queryPolicy = customBehavior.getSharedPolicy(Behavior.CommandType.QUERY);
            System.out.println("Query policy record queue size: " + queryPolicy.recordQueueSize);
            System.out.println("Query policy max concurrent nodes: " + queryPolicy.maxConcurrentNodes);

        } catch (Exception e) {
            System.err.println("Error loading behavior from YAML: " + e.getMessage());
            e.printStackTrace();
        }
    }
}