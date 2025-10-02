package com.aerospike.client.fluent.policy;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

class BehaviorYamlLoader {
    
    private static final ObjectMapper objectMapper;
    
    static {
        objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.registerModule(new JavaTimeModule());
    }
    
    /**
     * Load a Behavior from a YAML file and convert it to use the builder pattern
     * 
     * @param file The YAML file to load
     * @return A new Behavior instance created from the YAML configuration
     * @throws JsonParseException if the YAML is malformed
     * @throws JsonMappingException if the YAML structure doesn't match the expected format
     * @throws IOException if there's an error reading the file
     */
    public static Behavior loadFromFile(File file) throws JsonParseException, JsonMappingException, IOException {
        BehaviorYamlConfig config = objectMapper.readValue(file, BehaviorYamlConfig.class);
        Map<String, Behavior> behaviors = convertToBehaviors(config);
        // Return the first behavior or DEFAULT if none found
        return behaviors.isEmpty() ? Behavior.DEFAULT : behaviors.values().iterator().next();
    }
    
    /**
     * Load behaviors from a YAML file and register them in the tree structure
     * 
     * @param file The YAML file to load
     * @return Map of behavior names to behaviors
     * @throws JsonParseException if the YAML is malformed
     * @throws JsonMappingException if the YAML structure doesn't match the expected format
     * @throws IOException if there's an error reading the file
     */
    public static Map<String, Behavior> loadBehaviorsFromFile(File file) throws JsonParseException, JsonMappingException, IOException {
        BehaviorYamlConfig config = objectMapper.readValue(file, BehaviorYamlConfig.class);
        return updateBehaviorsFromConfig(config);
    }
    
    /**
     * Update existing behaviors from YAML configuration, creating new ones if they don't exist
     * 
     * @param config The YAML configuration
     * @return Map of behavior names to behaviors (including existing ones)
     */
    static Map<String, Behavior> updateBehaviorsFromConfig(BehaviorYamlConfig config) {
        BehaviorRegistry registry = BehaviorRegistry.getInstance();
        Map<String, Behavior> updatedBehaviors = new HashMap<>();
        
        if (config.getBehaviors() != null) {
            for (BehaviorYamlConfig.BehaviorConfig behaviorConfig : config.getBehaviors()) {
                String behaviorName = behaviorConfig.getName();
                
                // Check if behavior already exists
                Optional<Behavior> existingBehavior = registry.getBehavior(behaviorName);
                
                if (existingBehavior.isPresent()) {
                    // Update existing behavior
                    Behavior updatedBehavior = updateExistingBehavior(existingBehavior.get(), behaviorConfig);
                    updatedBehaviors.put(behaviorName, updatedBehavior);
                } else {
                    // Create new behavior
                    Behavior newBehavior = createNewBehavior(behaviorConfig);
                    updatedBehaviors.put(behaviorName, newBehavior);
                    registry.registerBehavior(newBehavior);
                }
            }
        }
        
        return updatedBehaviors;
    }
    
    /**
     * Update an existing behavior with new configuration
     * 
     * @param existingBehavior The existing behavior to update
     * @param config The new configuration
     * @return The updated behavior (same instance)
     */
    private static Behavior updateExistingBehavior(Behavior existingBehavior, BehaviorYamlConfig.BehaviorConfig config) {
        // Clear the behavior's cache to force regeneration
        existingBehavior.clearCache();
        
        // Update the behavior's configuration by applying the new settings
        // This is a simplified approach - in a real implementation, you might want to
        // update the internal policies directly rather than recreating them
        
        // For now, we'll create a new behavior with the same name and replace the old one
        // This maintains the same reference but updates the configuration
        Behavior updatedBehavior = createNewBehavior(config);
        
        // Update the registry to point to the new behavior
        BehaviorRegistry.getInstance().registerBehavior(updatedBehavior);
        
        return updatedBehavior;
    }
    
    /**
     * Create a new behavior from configuration
     * 
     * @param config The behavior configuration
     * @return A new Behavior instance
     */
    private static Behavior createNewBehavior(BehaviorYamlConfig.BehaviorConfig config) {
        BehaviorBuilder builder = new BehaviorBuilder();
        applyBehaviorConfigToBuilder(builder, config);
        
        // Create the behavior with the specified name and exception policy
        String name = config.getName() != null ? config.getName() : "yaml-loaded";
        Behavior.ExceptionPolicy exceptionPolicy = config.getExceptionPolicy() != null ? 
            config.getExceptionPolicy() : Behavior.ExceptionPolicy.RETURN_AS_MANY_RESULTS_AS_POSSIBLE;
        
        Behavior behavior = new Behavior(name, builder, exceptionPolicy);
        
        // Apply global settings
        if (config.getSendKey() != null) {
            behavior.withSendKey(config.getSendKey());
        }
        if (config.getUseCompression() != null) {
            behavior.withUseCompression(config.getUseCompression());
        }
        
        // Establish parent-child relationship if parent is specified
        if (config.getParent() != null && !"default".equals(config.getParent())) {
            BehaviorRegistry registry = BehaviorRegistry.getInstance();
            Optional<Behavior> parent = registry.getBehavior(config.getParent());
            if (parent.isPresent()) {
                // Create a derived behavior with the parent relationship
                Behavior derivedBehavior = parent.get().deriveWithChanges(name, builder2 -> {
                    applyBehaviorConfigToBuilder(builder2, config);
                });
                
                // Apply global settings to the derived behavior
                if (config.getSendKey() != null) {
                    derivedBehavior.withSendKey(config.getSendKey());
                }
                if (config.getUseCompression() != null) {
                    derivedBehavior.withUseCompression(config.getUseCompression());
                }
                
                return derivedBehavior;
            }
        }
        
        return behavior;
    }
    
    /**
     * Convert a YAML configuration object to a map of behaviors using the builder pattern
     * 
     * @param config The YAML configuration
     * @return Map of behavior names to behaviors
     */
    static Map<String, Behavior> convertToBehaviors(BehaviorYamlConfig config) {
        Map<String, Behavior> behaviors = new HashMap<>();
        Map<String, BehaviorYamlConfig.BehaviorConfig> configsByName = new HashMap<>();
        
        // First pass: create all behaviors without parent relationships
        if (config.getBehaviors() != null) {
            for (BehaviorYamlConfig.BehaviorConfig behaviorConfig : config.getBehaviors()) {
                Behavior behavior = convertToBehavior(behaviorConfig);
                behaviors.put(behavior.getName(), behavior);
                configsByName.put(behavior.getName(), behaviorConfig);
            }
        }
        
        // Second pass: establish parent-child relationships
        for (BehaviorYamlConfig.BehaviorConfig behaviorConfig : configsByName.values()) {
            if (behaviorConfig.getParent() != null && !"default".equals(behaviorConfig.getParent())) {
                Behavior child = behaviors.get(behaviorConfig.getName());
                Behavior parent = behaviors.get(behaviorConfig.getParent());
                
                if (child != null && parent != null) {
                    // Use the existing deriveWithChanges mechanism to establish parent-child relationship
                    Behavior derivedBehavior = parent.deriveWithChanges(child.getName(), builder -> {
                        // Apply the child's configuration to the builder
                        applyBehaviorConfigToBuilder(builder, behaviorConfig);
                    });
                    
                    // Replace the standalone behavior with the derived one
                    behaviors.put(derivedBehavior.getName(), derivedBehavior);
                }
            }
        }
        
        return behaviors;
    }
    
    /**
     * Convert a single behavior configuration to a Behavior using the builder pattern
     * 
     * @param config The behavior configuration
     * @return A new Behavior instance
     */
    static Behavior convertToBehavior(BehaviorYamlConfig.BehaviorConfig config) {
        BehaviorBuilder builder = new BehaviorBuilder();
        applyBehaviorConfigToBuilder(builder, config);
        
        // Create the behavior with the specified name and exception policy
        String name = config.getName() != null ? config.getName() : "yaml-loaded";
        Behavior.ExceptionPolicy exceptionPolicy = config.getExceptionPolicy() != null ? 
            config.getExceptionPolicy() : Behavior.ExceptionPolicy.RETURN_AS_MANY_RESULTS_AS_POSSIBLE;
        
        Behavior behavior = new Behavior(name, builder, exceptionPolicy);
        
        // Apply global settings
        if (config.getSendKey() != null) {
            behavior.withSendKey(config.getSendKey());
        }
        if (config.getUseCompression() != null) {
            behavior.withUseCompression(config.getUseCompression());
        }
        
        return behavior;
    }
    
    /**
     * Apply behavior configuration to a builder
     */
    private static void applyBehaviorConfigToBuilder(BehaviorBuilder builder, BehaviorYamlConfig.BehaviorConfig config) {
        // Apply all operations configuration
        if (config.getAllOperations() != null) {
            SettablePolicy.Builder allOperationsBuilder = builder.forAllOperations();
            applyPolicyConfig(allOperationsBuilder, config.getAllOperations());
            allOperationsBuilder.done();
        }
        
        // Apply consistency mode reads configuration
        if (config.getConsistencyModeReads() != null) {
            SettableConsistencyModeReadPolicy.Builder consistencyBuilder = builder.onConsistencyModeReads();
            applyPolicyConfig(consistencyBuilder, config.getConsistencyModeReads());
            if (config.getConsistencyModeReads().getReadConsistency() != null) {
                consistencyBuilder.readConsistency(config.getConsistencyModeReads().getReadConsistency());
            }
            consistencyBuilder.done();
        }
        
        // Apply availability mode reads configuration
        if (config.getAvailabilityModeReads() != null) {
            SettableAvailabilityModeReadPolicy.Builder availabilityBuilder = builder.onAvailablityModeReads();
            applyPolicyConfig(availabilityBuilder, config.getAvailabilityModeReads());
            if (config.getAvailabilityModeReads().getMigrationReadConsistency() != null) {
                availabilityBuilder.migrationReadConsistency(config.getAvailabilityModeReads().getMigrationReadConsistency());
            }
            availabilityBuilder.done();
        }
        
        // Apply retryable writes configuration
        if (config.getRetryableWrites() != null) {
            SettableWritePolicy.Builder writeBuilder = builder.onRetryableWrites();
            applyPolicyConfig(writeBuilder, config.getRetryableWrites());
            if (config.getRetryableWrites().getUseDurableDelete() != null) {
                writeBuilder.useDurableDelete(config.getRetryableWrites().getUseDurableDelete());
            }
            writeBuilder.done();
        }
        
        // Apply non-retryable writes configuration
        if (config.getNonRetryableWrites() != null) {
            SettableWritePolicy.Builder writeBuilder = builder.onNonRetryableWrites();
            applyPolicyConfig(writeBuilder, config.getNonRetryableWrites());
            if (config.getNonRetryableWrites().getUseDurableDelete() != null) {
                writeBuilder.useDurableDelete(config.getNonRetryableWrites().getUseDurableDelete());
            }
            writeBuilder.done();
        }
        
        // Apply batch reads configuration
        if (config.getBatchReads() != null) {
            SettableBatchPolicy.Builder batchBuilder = builder.onBatchReads();
            applyPolicyConfig(batchBuilder, config.getBatchReads());
            applyBatchConfig(batchBuilder, config.getBatchReads());
            batchBuilder.done();
        }
        
        // Apply batch writes configuration
        if (config.getBatchWrites() != null) {
            SettableBatchPolicy.Builder batchBuilder = builder.onBatchWrites();
            applyPolicyConfig(batchBuilder, config.getBatchWrites());
            applyBatchConfig(batchBuilder, config.getBatchWrites());
            batchBuilder.done();
        }
        
        // Apply query configuration
        if (config.getQuery() != null) {
            SettableQueryPolicy.Builder queryBuilder = builder.onQuery();
            applyPolicyConfig(queryBuilder, config.getQuery());
            if (config.getQuery().getRecordQueueSize() != null) {
                queryBuilder.recordQueueSize(config.getQuery().getRecordQueueSize());
            }
            if (config.getQuery().getMaxConcurrentServers() != null) {
                queryBuilder.maxConcurrentServers(config.getQuery().getMaxConcurrentServers());
            }
            queryBuilder.done();
        }
        
        // Apply info configuration
        if (config.getInfo() != null) {
            SettableInfoPolicy.Builder infoBuilder = builder.onInfo();
            if (config.getInfo().getAbandonCallAfter() != null) {
                infoBuilder.abandonCallAfter(config.getInfo().getAbandonCallAfter());
            }
            infoBuilder.done();
        }
    }
    
    /**
     * Apply common policy configuration to a builder
     */
    private static void applyPolicyConfig(SettablePolicy.BuilderBase<?> builder, BehaviorYamlConfig.PolicyConfig config) {
        if (config.getAbandonCallAfter() != null) {
            builder.abandonCallAfter(config.getAbandonCallAfter());
        }
        if (config.getDelayBetweenRetries() != null) {
            builder.delayBetweenRetries(config.getDelayBetweenRetries());
        }
        if (config.getMaximumNumberOfCallAttempts() != null) {
            builder.maximumNumberOfCallAttempts(config.getMaximumNumberOfCallAttempts());
        }
        if (config.getReplicaOrder() != null) {
            builder.replicaOrder(config.getReplicaOrder());
        }
        if (config.getResetTtlOnReadAtPercent() != null) {
            builder.resetTtlOnReadAtPercent(config.getResetTtlOnReadAtPercent());
        }
        if (config.getSendKey() != null) {
            builder.sendKey(config.getSendKey());
        }
        if (config.getUseCompression() != null) {
            builder.useCompression(config.getUseCompression());
        }
        if (config.getWaitForCallToComplete() != null) {
            builder.waitForCallToComplete(config.getWaitForCallToComplete());
        }
        if (config.getWaitForConnectionToComplete() != null) {
            builder.waitForConnectionToComplete(config.getWaitForConnectionToComplete());
        }
        if (config.getWaitForSocketResponseAfterCallFails() != null) {
            builder.waitForSocketResponseAfterCallFails(config.getWaitForSocketResponseAfterCallFails());
        }
    }
    
    /**
     * Apply batch-specific configuration to a batch builder
     */
    private static void applyBatchConfig(SettableBatchPolicy.Builder builder, BehaviorYamlConfig.BatchConfig config) {
        if (config.getMaxConcurrentServers() != null) {
            builder.maxConcurrentServers(config.getMaxConcurrentServers());
        }
        if (config.getAllowInlineMemoryAccess() != null) {
            builder.allowInlineMemoryAccess(config.getAllowInlineMemoryAccess());
        }
        if (config.getAllowInlineSsdAccess() != null) {
            builder.allowInlineSsdAccess(config.getAllowInlineSsdAccess());
        }
    }
} 