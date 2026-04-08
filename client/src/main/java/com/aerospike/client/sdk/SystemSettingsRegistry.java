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
package com.aerospike.client.sdk;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global registry for system settings that manages default and cluster-specific configurations.
 *
 * <p>This singleton tracks all active {@link Cluster} instances and applies system settings
 * based on a 4-level priority hierarchy. It supports dynamic reloading from YAML configuration
 * files and automatically propagates changes to all active clusters.</p>
 *
 * <p><b>Priority Hierarchy</b> (highest to lowest):</p>
 * <ol>
 *   <li>YAML cluster-specific settings (matching cluster name)</li>
 *   <li>YAML default settings</li>
 *   <li>Code-provided settings (via {@link ClusterDefinition})</li>
 *   <li>Hard-coded defaults ({@link SystemSettings#DEFAULT})</li>
 * </ol>
 *
 * <p><b>Cluster Tracking</b>:</p>
 * <ul>
 *   <li>Uses {@link WeakHashMap} to track active clusters without preventing GC</li>
 *   <li>Clusters must be strongly referenced by application code to remain active</li>
 *   <li>Closed/abandoned clusters are automatically removed from tracking</li>
 * </ul>
 *
 * <p><b>Dynamic Name Discovery</b>:</p>
 * <p>When a Cluster is created without a cluster name (via {@code validateClusterName()}),
 * it initially uses default settings. When the actual cluster name is discovered from the
 * server, the registry automatically upgrades to cluster-specific settings if available.</p>
 *
 * <p><b>Thread Safety</b>:</p>
 * <p>All operations are thread-safe. Uses concurrent collections and volatile fields
 * for safe publication across threads.</p>
 *
 * <p><b>Example Usage:</b></p>
 * <pre>{@code
 * // Load settings from YAML
 * SystemSettingsRegistry registry = SystemSettingsRegistry.getInstance();
 * registry.updateDefaultSettings(defaultSettings);
 * registry.updateClusterSettings("prod", prodSettings);
 *
 * // Clusters automatically pick up the right settings
 * Cluster cluster = new ClusterDefinition("localhost", 3000)
 *     .validateClusterName("prod")
 *     .connect();  // Gets prod-specific settings
 * }</pre>
 *
 * @see SystemSettings
 * @see Cluster
 */
public class SystemSettingsRegistry {

    private static final SystemSettingsRegistry INSTANCE = new SystemSettingsRegistry();

    /**
     * Default settings loaded from YAML or code.
     * Volatile for safe publication across threads.
     */
    private volatile SystemSettings defaultSettings = SystemSettings.DEFAULT;

    /**
     * Cluster-specific overrides: clusterName -> SystemSettings.
     * Thread-safe concurrent map.
     */
    private final ConcurrentHashMap<String, SystemSettings> clusterSettings = new ConcurrentHashMap<>();

    /**
     * Active clusters tracked with WeakHashMap for automatic cleanup.
     * Synchronized access required.
     * Key: Cluster instance (weak reference)
     * Value: ClusterInfo containing cluster name and code-provided settings
     */
    private final Map<Cluster, ClusterInfo> activeClusters =
        new WeakHashMap<>();

    private SystemSettingsRegistry() {
        // Private constructor for singleton
    }

    /**
     * Gets the singleton instance of the registry.
     */
    public static SystemSettingsRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a new cluster with the registry.
     * Called when a cluster is created via {@link ClusterDefinition#connect()}.
     *
     * @param cluster the cluster instance to register
     * @param clusterName the cluster name (may be null if not specified)
     * @param codeProvidedSettings settings explicitly provided in code (may be null)
     */
    public void registerCluster(Cluster cluster, String clusterName, SystemSettings codeProvidedSettings) {
        synchronized (activeClusters) {
            ClusterInfo info = new ClusterInfo(clusterName, codeProvidedSettings);
            activeClusters.put(cluster, info);

            if (Log.infoEnabled()) {
                if (clusterName != null) {
                    boolean hasClusterSpecific = clusterSettings.containsKey(clusterName);
                    Log.info("Registered cluster '" + clusterName + "' with " +
                            (hasClusterSpecific ? "cluster-specific" : "default") + " system settings");
                } else {
                    Log.info("Registered unnamed cluster with default system settings");
                }
            }
        }
    }

    /**
     * Updates the cluster name for a registered cluster.
     * Called when the cluster discovers its actual name from the server.
     * May trigger an upgrade to cluster-specific settings.
     *
     * @param cluster the cluster instance
     * @param discoveredName the cluster name discovered from the server
     */
    public void updateClusterName(Cluster cluster, String discoveredName) {
        synchronized (activeClusters) {
            ClusterInfo info = activeClusters.get(cluster);
            if (info == null) {
                Log.warn("Attempted to update name for unregistered cluster: " + discoveredName);
                return;
            }

            String oldName = info.clusterName;
            if (discoveredName.equals(oldName)) {
                return;  // No change
            }

            // Update the name
            info.clusterName = discoveredName;

            // Check if we need to upgrade to cluster-specific settings
            boolean hasClusterSpecific = clusterSettings.containsKey(discoveredName);
            if (hasClusterSpecific) {
                SystemSettings effectiveSettings = getEffectiveSettings(info);
                cluster.applySystemSettings(effectiveSettings);

                if (Log.infoEnabled()) {
                    Log.info("Discovered cluster name '" + discoveredName +
                            "', upgraded to cluster-specific system settings");
                }
            } else if (Log.infoEnabled()) {
                Log.info("Discovered cluster name '" + discoveredName +
                        "', continuing with default system settings");
            }
        }
    }

    /**
     * Gets the effective settings for a cluster, applying the 4-level priority hierarchy.
     *
     * @param clusterName the cluster name (may be null)
     * @param codeProvidedSettings settings provided in code (may be null)
     * @return the effective settings after merging all levels
     */
    public SystemSettings getEffectiveSettings(String clusterName, SystemSettings codeProvidedSettings) {
        // Start with hard-coded defaults (Level 1 - lowest priority)
        SystemSettings result = SystemSettings.DEFAULT;

        // Layer 2: Code-provided settings
        if (codeProvidedSettings != null) {
            result = codeProvidedSettings.mergeWith(result);
        }

        // Layer 3: YAML default settings (only when explicitly set; otherwise code-provided wins)
        if (defaultSettings != SystemSettings.DEFAULT) {
             result = defaultSettings.mergeWith(result);
        }
        
        // Layer 4: YAML cluster-specific settings (highest priority)
        if (clusterName != null) {
            SystemSettings clusterSpecific = clusterSettings.get(clusterName);
            if (clusterSpecific != null) {
                result = clusterSpecific.mergeWith(result);
            }
        }

        return result;
    }

    /**
     * Gets the effective settings for a registered cluster.
     */
    private SystemSettings getEffectiveSettings(ClusterInfo info) {
        return getEffectiveSettings(info.clusterName, info.codeProvidedSettings);
    }

    /**
     * Updates the default system settings and applies changes to affected clusters.
     *
     * @param newSettings the new default settings
     */
    public void updateDefaultSettings(SystemSettings newSettings) {
        if (newSettings == null) {
            throw new IllegalArgumentException("Default settings cannot be null");
        }

        SystemSettings oldDefaults = this.defaultSettings;
        this.defaultSettings = newSettings;

        if (Log.infoEnabled()) {
            Log.info("Updated default system settings");
        }

        // Apply to all affected clusters
        synchronized (activeClusters) {
            for (Map.Entry<Cluster, ClusterInfo> entry : activeClusters.entrySet()) {
                Cluster cluster = entry.getKey();
                ClusterInfo info = entry.getValue();

                // Calculate old and new effective settings
                SystemSettings oldEffective = getEffectiveSettingsWithDefaults(info, oldDefaults);
                SystemSettings newEffective = getEffectiveSettings(info);

                // Only apply if something actually changed
                if (!oldEffective.equals(newEffective)) {
                    cluster.applySystemSettings(newEffective);
                    if (Log.infoEnabled()) {
                        String name = info.clusterName != null ? info.clusterName : "(unnamed)";
                        Log.info("Applied updated default settings to cluster: " + name);
                    }
                }
            }
        }
    }

    /**
     * Updates cluster-specific system settings and applies changes to affected clusters.
     *
     * @param clusterName the cluster name
     * @param newSettings the new cluster-specific settings
     */
    public void updateClusterSettings(String clusterName, SystemSettings newSettings) {
        if (clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("Cluster name cannot be null or empty");
        }
        if (newSettings == null) {
            throw new IllegalArgumentException("Cluster settings cannot be null");
        }

        clusterSettings.put(clusterName, newSettings);

        if (Log.infoEnabled()) {
            Log.info("Updated system settings for cluster: " + clusterName);
        }

        // Apply to all clusters with this name
        synchronized (activeClusters) {
            for (Map.Entry<Cluster, ClusterInfo> entry : activeClusters.entrySet()) {
                ClusterInfo info = entry.getValue();
                if (clusterName.equals(info.clusterName)) {
                    Cluster cluster = entry.getKey();
                    SystemSettings effectiveSettings = getEffectiveSettings(info);
                    cluster.applySystemSettings(effectiveSettings);

                    if (Log.infoEnabled()) {
                        Log.info("Applied cluster-specific settings to: " + clusterName);
                    }
                }
            }
        }
    }

    /**
     * Removes cluster-specific settings, causing affected clusters to fall back to defaults.
     *
     * @param clusterName the cluster name
     */
    public void removeClusterSettings(String clusterName) {
        if (clusterName == null || clusterName.isEmpty()) {
            return;
        }

        SystemSettings removed = clusterSettings.remove(clusterName);
        if (removed == null) {
            return;  // Nothing to do
        }

        if (Log.infoEnabled()) {
            Log.info("Removed system settings for cluster: " + clusterName);
        }

        // Apply default settings to affected clusters
        synchronized (activeClusters) {
            for (Map.Entry<Cluster, ClusterInfo> entry : activeClusters.entrySet()) {
                ClusterInfo info = entry.getValue();
                if (clusterName.equals(info.clusterName)) {
                    Cluster cluster = entry.getKey();
                    SystemSettings effectiveSettings = getEffectiveSettings(info);
                    cluster.applySystemSettings(effectiveSettings);

                    if (Log.infoEnabled()) {
                        Log.info("Reverted to default settings for cluster: " + clusterName);
                    }
                }
            }
        }
    }

    /**
     * Helper method to calculate effective settings with specific default settings.
     * Used during dynamic updates to compare old vs new effective settings.
     */
    private SystemSettings getEffectiveSettingsWithDefaults(ClusterInfo info, SystemSettings defaults) {
        SystemSettings result = SystemSettings.DEFAULT;

        if (info.codeProvidedSettings != null) {
            result = info.codeProvidedSettings.mergeWith(result);
        }

        result = defaults.mergeWith(result);

        if (info.clusterName != null) {
            SystemSettings clusterSpecific = clusterSettings.get(info.clusterName);
            if (clusterSpecific != null) {
                result = clusterSpecific.mergeWith(result);
            }
        }

        return result;
    }

    /**
     * Gets the current default settings.
     */
    public SystemSettings getDefaultSettings() {
        return defaultSettings;
    }

    /**
     * Gets cluster-specific settings if they exist.
     *
     * @param clusterName the cluster name
     * @return cluster-specific settings or null if not defined
     */
    public SystemSettings getClusterSettings(String clusterName) {
        return clusterSettings.get(clusterName);
    }

    /**
     * Gets the number of currently tracked clusters.
     * For testing and monitoring purposes.
     */
    public int getActiveClusterCount() {
        synchronized (activeClusters) {
            return activeClusters.size();
        }
    }

    /**
     * Internal class to track cluster information.
     */
    private static class ClusterInfo {
        String clusterName;  // Mutable - can be updated when discovered
        final SystemSettings codeProvidedSettings;  // Immutable

        ClusterInfo(String clusterName, SystemSettings codeProvidedSettings) {
            this.clusterName = clusterName;
            this.codeProvidedSettings = codeProvidedSettings;
        }
    }
}

