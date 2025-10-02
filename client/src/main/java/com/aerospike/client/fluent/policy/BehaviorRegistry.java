package com.aerospike.client.fluent.policy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing Behavior instances in a tree structure.
 * Provides lookup by name and manages parent-child relationships.
 */
class BehaviorRegistry {
    
    private static final BehaviorRegistry INSTANCE = new BehaviorRegistry();
    
    private final Map<String, Behavior> behaviorsByName = new ConcurrentHashMap<>();
    private final Map<String, Behavior> behaviorsByParent = new ConcurrentHashMap<>();
    
    private BehaviorRegistry() {
        // Initialize with DEFAULT behavior
        behaviorsByName.put("default", Behavior.DEFAULT);
    }
    
    /**
     * Get the singleton instance of the registry
     */
    public static BehaviorRegistry getInstance() {
        return INSTANCE;
    }
    
    /**
     * Register a behavior with the registry
     * 
     * @param behavior The behavior to register
     */
    void registerBehavior(Behavior behavior) {
        behaviorsByName.put(behavior.getName(), behavior);
        
        // Update parent-child relationships
        Behavior parent = behavior.getParent();
        if (parent != null) {
            behaviorsByParent.put(parent.getName(), parent);
        }
    }
    

    
    /**
     * Look up a behavior by name
     * 
     * @param name The name of the behavior to find
     * @return Optional containing the behavior if found, or empty if not found
     */
    public Optional<Behavior> getBehavior(String name) {
        return Optional.ofNullable(behaviorsByName.get(name));
    }
    
    /**
     * Look up a behavior by name, returning DEFAULT if not found
     * 
     * @param name The name of the behavior to find
     * @return The behavior, or DEFAULT if not found
     */
    public Behavior getBehaviorOrDefault(String name) {
        Behavior behavior = behaviorsByName.get(name);
        return behavior != null ? behavior : Behavior.DEFAULT;
    }
    
    /**
     * Check if a behavior exists with the given name
     * 
     * @param name The name to check
     * @return true if the behavior exists
     */
    public boolean hasBehavior(String name) {
        return behaviorsByName.containsKey(name);
    }
    
    /**
     * Get all registered behaviors
     * 
     * @return Map of behavior names to behaviors
     */
    Map<String, Behavior> getAllBehaviors() {
        return new HashMap<>(behaviorsByName);
    }
    
    /**
     * Remove a behavior from the registry
     * 
     * @param name The name of the behavior to remove
     * @return The removed behavior, or null if not found
     */
    public Behavior removeBehavior(String name) {
        Behavior removed = behaviorsByName.remove(name);
        if (removed != null) {
            // Remove from parent map if it was there
            behaviorsByParent.remove(name);
        }
        return removed;
    }
    
    /**
     * Clear all behaviors except DEFAULT
     */
    void clear() {
        behaviorsByName.clear();
        behaviorsByParent.clear();
        // Re-add DEFAULT
        behaviorsByName.put("default", Behavior.DEFAULT);
    }
    
    /**
     * Find a behavior by traversing the tree from a starting point
     * 
     * @param startBehavior The behavior to start from
     * @param targetName The name of the behavior to find
     * @return Optional containing the behavior if found, or empty if not found
     */
    public Optional<Behavior> findInTree(Behavior startBehavior, String targetName) {
        if (startBehavior == null) {
            return Optional.empty();
        }
        
        // Check if this is the target
        if (targetName.equals(startBehavior.getName())) {
            return Optional.of(startBehavior);
        }
        
        // Search in children
        for (Behavior child : startBehavior.getChildren()) {
            Optional<Behavior> found = findInTree(child, targetName);
            if (found.isPresent()) {
                return found;
            }
        }
        
        return Optional.empty();
    }
    
    /**
     * Find a behavior by name using tree traversal from DEFAULT
     * 
     * @param name The name of the behavior to find
     * @return Optional containing the behavior if found, or empty if not found
     */
    public Optional<Behavior> findInTree(String name) {
        return findInTree(Behavior.DEFAULT, name);
    }
} 