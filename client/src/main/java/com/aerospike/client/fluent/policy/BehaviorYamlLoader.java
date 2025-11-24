package com.aerospike.client.fluent.policy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

class BehaviorYamlLoader {
    
    private static final Yaml yaml;
    
    static {
        LoaderOptions loaderOptions = new LoaderOptions();
        CustomConstructor constructor = new CustomConstructor(BehaviorYamlConfig.class, loaderOptions);
        yaml = new Yaml(constructor);
    }
    
    /**
     * Custom SnakeYAML constructor that handles Duration parsing and property name mapping
     */
    private static class CustomConstructor extends Constructor {
        
        public CustomConstructor(Class<?> theRoot, LoaderOptions loadingConfig) {
            super(theRoot, loadingConfig);
            
            // Register Duration constructor
            this.yamlConstructors.put(new Tag(Duration.class), new DurationConstruct());
            
            // Use custom property utils to skip missing properties
            PropertyUtils propertyUtils = new PropertyUtils() {
                @Override
                public Property getProperty(Class<?> type, String name) {
                    try {
                        return super.getProperty(type, name);
                    } catch (Exception e) {
                        // If property not found, skip it
                        return null;
                    }
                }
            };
            propertyUtils.setSkipMissingProperties(true);
            this.setPropertyUtils(propertyUtils);
        }
        
        @Override
        protected Object constructObject(Node node) {
            if (node instanceof ScalarNode) {
                ScalarNode scalarNode = (ScalarNode) node;
                String value = scalarNode.getValue();
                
                // Try to detect if this is a Duration value based on format
                if (isDurationValue(value)) {
                    return DurationConstruct.parseDuration(value);
                }
            }
            return super.constructObject(node);
        }
        
        private boolean isDurationValue(String value) {
            if (value == null || value.isEmpty()) {
                return false;
            }
            // Check if it looks like a duration (number followed by time unit or ISO-8601)
            return value.matches("^\\d+\\s*[a-zA-Z]+$") || value.startsWith("PT") || value.startsWith("P");
        }
    }
    
    /**
     * Load a Behavior from a YAML file using the new selector-based API
     * 
     * @param file The YAML file to load
     * @return A new Behavior instance created from the YAML configuration
     * @throws IOException if there's an error reading or parsing the file
     */
    public static Behavior loadFromFile(File file) throws IOException {
        BehaviorYamlConfig config = loadYamlConfig(file);
        Map<String, Behavior> behaviors = convertToBehaviors(config);
        // Return the first behavior or DEFAULT if none found
        return behaviors.isEmpty() ? Behavior.DEFAULT : behaviors.values().iterator().next();
    }
    
    /**
     * Load behaviors from a YAML file and register them in the tree structure
     * 
     * @param file The YAML file to load
     * @return Map of behavior names to behaviors
     * @throws IOException if there's an error reading or parsing the file
     */
    public static Map<String, Behavior> loadBehaviorsFromFile(File file) throws IOException {
        BehaviorYamlConfig config = loadYamlConfig(file);
        return updateBehaviorsFromConfig(config);
    }
    
    /**
     * TODO: we might not want this methods for the public API
     * Load behaviors from a YAML string and register them in the tree structure
     * 
     * @param yamlString The YAML string to load
     * @return Map of behavior names to behaviors
     * @throws IOException if there's an error parsing the YAML
     */
    public static Map<String, Behavior> loadBehaviorsFromString(String yamlString) throws IOException {
        BehaviorYamlConfig config = loadYamlConfigFromString(yamlString);
        return updateBehaviorsFromConfig(config);
    }
    
    /**
     * Load YAML configuration from file using SnakeYAML
     */
    private static BehaviorYamlConfig loadYamlConfig(File file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file)) {
            return yaml.load(inputStream);
        } catch (Exception e) {
            throw new IOException("Failed to parse YAML file: " + file, e);
        }
    }
    
    /**
     * Load YAML configuration from string using SnakeYAML
     */
    private static BehaviorYamlConfig loadYamlConfigFromString(String yamlString) throws IOException {
        try (InputStream inputStream = new ByteArrayInputStream(yamlString.getBytes(StandardCharsets.UTF_8))) {
            return yaml.load(inputStream);
        } catch (Exception e) {
            throw new IOException("Failed to parse YAML string", e);
        }
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
     * @return The updated behavior (new instance with same name)
     */
    private static Behavior updateExistingBehavior(Behavior existingBehavior, BehaviorYamlConfig.BehaviorConfig config) {
        // Create a new behavior with the updated configuration
        // Note: This creates a new instance rather than modifying the existing one
        Behavior updatedBehavior = createNewBehavior(config);
        
        // Update the registry to point to the new behavior
        BehaviorRegistry.getInstance().registerBehavior(updatedBehavior);
        
        return updatedBehavior;
    }
    
    /**
     * Create a new behavior from configuration using the new selector-based API
     * 
     * @param config The behavior configuration
     * @return A new Behavior instance
     */
    private static Behavior createNewBehavior(BehaviorYamlConfig.BehaviorConfig config) {
        String name = config.getName() != null ? config.getName() : "yaml-loaded";
        
        // Determine parent behavior
        Behavior parent = Behavior.DEFAULT;
        if (config.getParent() != null && !"default".equalsIgnoreCase(config.getParent())) {
            BehaviorRegistry registry = BehaviorRegistry.getInstance();
            Optional<Behavior> parentOpt = registry.getBehavior(config.getParent());
            if (parentOpt.isPresent()) {
                parent = parentOpt.get();
            }
        }
        
        // Use deriveWithChanges to create the behavior with parent inheritance
        Behavior behavior = parent.deriveWithChanges(name, builder -> {
            applyBehaviorConfigToBuilder(builder, config);
        });
        
        return behavior;
    }
    
    /**
     * Convert a YAML configuration object to a map of behaviors using the new selector-based API
     * 
     * @param config The YAML configuration
     * @return Map of behavior names to behaviors
     */
    static Map<String, Behavior> convertToBehaviors(BehaviorYamlConfig config) {
        Map<String, Behavior> behaviors = new HashMap<>();
        
        // Create all behaviors (parent relationships are handled in createNewBehavior)
        if (config.getBehaviors() != null) {
            for (BehaviorYamlConfig.BehaviorConfig behaviorConfig : config.getBehaviors()) {
                Behavior behavior = createNewBehavior(behaviorConfig);
                behaviors.put(behavior.name(), behavior);
            }
        }
        
        return behaviors;
    }
    
    /**
     * Apply behavior configuration to a builder using the new selector-based API
     * 
     * @param builder The behavior builder
     * @param config The behavior configuration from YAML
     */
    private static void applyBehaviorConfigToBuilder(Behavior.BehaviorBuilder builder, BehaviorYamlConfig.BehaviorConfig config) {
        // Apply all operations configuration
        if (config.getAllOperations() != null) {
            builder.on(Behavior.Selectors.all(), ops -> {
                applyCommonConfig(ops, config.getAllOperations());
                // Apply read-specific properties if present
                if (config.getAllOperations().getResetTtlOnReadAtPercent() != null) {
                    ops.resetTtlOnReadAtPercent(config.getAllOperations().getResetTtlOnReadAtPercent());
                }
            });
        }
        
        // Apply consistency mode reads (CP reads)
        if (config.getConsistencyModeReads() != null) {
            builder.on(Behavior.Selectors.reads().cp(), ops -> {
                applyCommonConfig(ops, config.getConsistencyModeReads());
                if (config.getConsistencyModeReads().getReadConsistency() != null) {
                    ops.consistency(config.getConsistencyModeReads().getReadConsistency());
                }
            });
        }
        
        // Apply availability mode reads (AP reads)
        if (config.getAvailabilityModeReads() != null) {
            builder.on(Behavior.Selectors.reads().ap(), ops -> {
                applyCommonConfig(ops, config.getAvailabilityModeReads());
                if (config.getAvailabilityModeReads().getMigrationReadConsistency() != null) {
                    ops.readMode(config.getAvailabilityModeReads().getMigrationReadConsistency());
                }
            });
        }
        
        // Apply retryable writes configuration
        if (config.getRetryableWrites() != null) {
            builder.on(Behavior.Selectors.writes().retryable(), ops -> {
                applyCommonConfig(ops, config.getRetryableWrites());
                if (config.getRetryableWrites().getUseDurableDelete() != null) {
                    ops.useDurableDelete(config.getRetryableWrites().getUseDurableDelete());
                }
            });
        }
        
        // Apply non-retryable writes configuration
        if (config.getNonRetryableWrites() != null) {
            builder.on(Behavior.Selectors.writes().nonRetryable(), ops -> {
                applyCommonConfig(ops, config.getNonRetryableWrites());
                if (config.getNonRetryableWrites().getUseDurableDelete() != null) {
                    ops.useDurableDelete(config.getNonRetryableWrites().getUseDurableDelete());
                }
            });
        }
        
        // Apply batch reads configuration
        if (config.getBatchReads() != null) {
            builder.on(Behavior.Selectors.reads().batch(), ops -> {
                applyCommonConfig(ops, config.getBatchReads());
                applyBatchConfig(ops, config.getBatchReads());
            });
        }
        
        // Apply batch writes configuration
        if (config.getBatchWrites() != null) {
            builder.on(Behavior.Selectors.writes().batch(), ops -> {
                applyCommonConfig(ops, config.getBatchWrites());
                applyBatchConfig(ops, config.getBatchWrites());
            });
        }
        
        // Apply query configuration
        if (config.getQuery() != null) {
            builder.on(Behavior.Selectors.reads().query(), ops -> {
                applyCommonConfig(ops, config.getQuery());
                if (config.getQuery().getRecordQueueSize() != null) {
                    ops.recordQueueSize(config.getQuery().getRecordQueueSize());
                }
            });
        }
        
        // Apply system - txnVerify configuration
        if (config.getSystemTxnVerify() != null) {
            builder.on(Behavior.Selectors.system().txnVerify(), ops -> {
                applyCommonConfig(ops, config.getSystemTxnVerify());
                if (config.getSystemTxnVerify().getConsistency() != null) {
                    ops.consistency(config.getSystemTxnVerify().getConsistency());
                }
            });
        }
        
        // Apply system - txnRoll configuration
        if (config.getSystemTxnRoll() != null) {
            builder.on(Behavior.Selectors.system().txnRoll(), ops -> {
                applyCommonConfig(ops, config.getSystemTxnRoll());
            });
        }
        
        // Apply system - connections configuration
        if (config.getSystemConnections() != null) {
            builder.on(Behavior.Selectors.system().connections(), ops -> {
                BehaviorYamlConfig.SystemConnectionsConfig connConfig = config.getSystemConnections();
                if (connConfig.getMinimumConnectionsPerNode() != null) {
                    ops.minimumConnectionsPerNode(connConfig.getMinimumConnectionsPerNode());
                }
                if (connConfig.getMaximumConnectionsPerNode() != null) {
                    ops.maximumConnectionsPerNode(connConfig.getMaximumConnectionsPerNode());
                }
                if (connConfig.getMaximumSocketIdleTime() != null) {
                    ops.maximumSocketIdleTime(connConfig.getMaximumSocketIdleTime());
                }
            });
        }
        
        // Apply system - circuitBreaker configuration
        if (config.getSystemCircuitBreaker() != null) {
            builder.on(Behavior.Selectors.system().circuitBreaker(), ops -> {
                BehaviorYamlConfig.SystemCircuitBreakerConfig cbConfig = config.getSystemCircuitBreaker();
                if (cbConfig.getNumTendIntervalsInErrorWindow() != null) {
                    ops.numTendIntervalsInErrorWindow(cbConfig.getNumTendIntervalsInErrorWindow());
                }
                if (cbConfig.getMaximumErrorsInErrorWindow() != null) {
                    ops.maximumErrorsInErrorWindow(cbConfig.getMaximumErrorsInErrorWindow());
                }
            });
        }
        
        // Apply system - refresh configuration
        if (config.getSystemRefresh() != null) {
            builder.on(Behavior.Selectors.system().refresh(), ops -> {
                BehaviorYamlConfig.SystemRefreshConfig refreshConfig = config.getSystemRefresh();
                if (refreshConfig.getTendInterval() != null) {
                    ops.tendInterval(refreshConfig.getTendInterval());
                }
            });
        }
    }
    
    /**
     * Apply common policy configuration using the new API
     * 
     * @param tweaks The tweaks view (any common tweaks interface)
     * @param config The policy configuration from YAML
     */
    private static void applyCommonConfig(Behavior.CommonTweaks tweaks, BehaviorYamlConfig.PolicyConfig config) {
        if (config.getAbandonCallAfter() != null) {
            tweaks.abandonCallAfter(config.getAbandonCallAfter());
        }
        if (config.getDelayBetweenRetries() != null) {
            tweaks.delayBetweenRetries(config.getDelayBetweenRetries());
        }
        if (config.getMaximumNumberOfCallAttempts() != null) {
            tweaks.maximumNumberOfCallAttempts(config.getMaximumNumberOfCallAttempts());
        }
        if (config.getReplicaOrder() != null) {
            tweaks.replicaOrder(config.getReplicaOrder());
        }
        if (config.getSendKey() != null) {
            tweaks.sendKey(config.getSendKey());
        }
        if (config.getUseCompression() != null) {
            tweaks.useCompression(config.getUseCompression());
        }
        if (config.getWaitForCallToComplete() != null) {
            tweaks.waitForCallToComplete(config.getWaitForCallToComplete());
        }
        if (config.getWaitForConnectionToComplete() != null) {
            tweaks.waitForConnectionToComplete(config.getWaitForConnectionToComplete());
        }
        if (config.getWaitForSocketResponseAfterCallFails() != null) {
            tweaks.waitForSocketResponseAfterCallFails(config.getWaitForSocketResponseAfterCallFails());
        }
    }
    
    /**
     * Apply batch-specific configuration using the new API
     * 
     * @param tweaks The batch tweaks view
     * @param config The batch configuration from YAML
     */
    private static void applyBatchConfig(Behavior.BatchTweaks tweaks, BehaviorYamlConfig.BatchConfig config) {
        if (config.getMaxConcurrentServers() != null) {
            tweaks.maxConcurrentNodes(config.getMaxConcurrentServers());
        }
        if (config.getAllowInlineMemoryAccess() != null) {
            tweaks.allowInlineMemoryAccess(config.getAllowInlineMemoryAccess());
        }
        if (config.getAllowInlineSsdAccess() != null) {
            tweaks.allowInlineSsdAccess(config.getAllowInlineSsdAccess());
        }
    }
} 