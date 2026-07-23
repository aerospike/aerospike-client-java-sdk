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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global registry for metrics settings that manages default and cluster-specific configurations.
 *
 * <p>This singleton tracks all active {@link Cluster} instances and applies metrics settings
 * based on a 4-level priority hierarchy. It supports dynamic reloading from YAML configuration
 * files and automatically propagates changes to all active clusters.</p>
 *
 * <p><b>Priority Hierarchy</b> (highest to lowest):</p>
 * <ol>
 *   <li>YAML cluster-specific settings (matching cluster name)</li>
 *   <li>YAML default settings</li>
 *   <li>Code-provided settings (via {@link ClusterDefinition})</li>
 *   <li>Hard-coded defaults ({@link MetricsSettings#DEFAULT})</li>
 * </ol>
 *
 * <p><b>Cluster Tracking</b>:</p>
 * <ul>
 *   <li>Uses {@link WeakHashMap} to track active clusters without preventing GC</li>
 *   <li>Clusters must be strongly referenced by application code to remain active</li>
 *   <li>Closed/abandoned clusters are automatically removed from tracking</li>
 * </ul>
 *
 * @see MetricsSettings
 * @see Cluster
 */
public class MetricsSettingsRegistry {
    private static final MetricsSettingsRegistry INSTANCE = new MetricsSettingsRegistry();
    private static final Logger log = LoggerFactory.getLogger(Loggers.BEHAVIOR);

    /**
     * Default settings loaded from YAML or code.
     * Volatile for safe publication across threads.
     */
    private volatile MetricsSettings defaultSettings = MetricsSettings.DEFAULT;

    /**
     * Cluster-specific overrides: clusterName -> MetricsSettings.
     * Thread-safe concurrent map.
     */
    private final ConcurrentHashMap<String, MetricsSettings> clusterSettings = new ConcurrentHashMap<>();

    /**
     * Active clusters tracked with WeakHashMap for automatic cleanup.
     * Synchronized access required.
     * Key: Cluster instance (weak reference)
     * Value: ClusterInfo containing cluster name and code-provided settings
     */
    private final Map<Cluster, ClusterInfo> activeClusters =
        new WeakHashMap<>();

    private MetricsSettingsRegistry() {
        // Private constructor for singleton
    }

    /**
     * Gets the singleton instance of the registry.
     */
    public static MetricsSettingsRegistry getInstance() {
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
    public void registerCluster(Cluster cluster, String clusterName, MetricsSettings codeProvidedSettings) {
        synchronized (activeClusters) {
            ClusterInfo info = new ClusterInfo(clusterName, codeProvidedSettings);
            activeClusters.put(cluster, info);

            if (log.isInfoEnabled()) {
                if (clusterName != null) {
                    boolean hasClusterSpecific = clusterSettings.containsKey(clusterName);

                    log.info("Registered cluster '" + clusterName + "' with " +
                        (hasClusterSpecific ? "cluster-specific" : "default") + " metrics settings");
                } else {
                    log.info("Registered unnamed cluster with default metrics settings");
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
                if (log.isWarnEnabled()) {
                    log.warn("Attempted to update name for unregistered cluster: " + discoveredName);
                }
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
                MetricsSettings effectiveSettings = getEffectiveSettings(info);
                cluster.applyMetricsSettings(effectiveSettings);

                if (log.isInfoEnabled()) {
                    log.info("Discovered cluster name '" + discoveredName +
                            "', upgraded to cluster-specific metrics settings");
                }
            }
            else if (log.isInfoEnabled()) {
                log.info("Discovered cluster name '" + discoveredName +
                        "', continuing with default metrics settings");
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
    public MetricsSettings getEffectiveSettings(String clusterName, MetricsSettings codeProvidedSettings) {
        // Start with hard-coded defaults (Level 1 - lowest priority)
        MetricsSettings result = MetricsSettings.DEFAULT;

        // Layer 2: Code-provided settings
        if (codeProvidedSettings != null) {
            result = codeProvidedSettings.mergeWith(result);
        }

        // Layer 3: YAML default settings (only when explicitly set; otherwise code-provided wins)
        if (defaultSettings != MetricsSettings.DEFAULT) {
             result = defaultSettings.mergeWith(result);
        }

        // Layer 4: YAML cluster-specific settings (highest priority)
        if (clusterName != null) {
            MetricsSettings clusterSpecific = clusterSettings.get(clusterName);
            if (clusterSpecific != null) {
                result = clusterSpecific.mergeWith(result);
            }
        }

        return result;
    }

    /**
     * Gets the effective settings for a registered cluster.
     */
    private MetricsSettings getEffectiveSettings(ClusterInfo info) {
        return getEffectiveSettings(info.clusterName, info.codeProvidedSettings);
    }

    /**
     * Updates the default metrics settings and applies changes to affected clusters.
     *
     * @param newSettings the new default settings
     */
    public void updateDefaultSettings(MetricsSettings newSettings) {
        if (newSettings == null) {
            throw new IllegalArgumentException("Default settings cannot be null");
        }

        MetricsSettings oldDefaults = this.defaultSettings;
        this.defaultSettings = newSettings;

        if (log.isInfoEnabled()) {
            log.info("Updated default metrics settings");
        }

        // Apply to all affected clusters
        synchronized (activeClusters) {
            for (Map.Entry<Cluster, ClusterInfo> entry : activeClusters.entrySet()) {
                Cluster cluster = entry.getKey();
                ClusterInfo info = entry.getValue();

                // Calculate old and new effective settings
                MetricsSettings oldEffective = getEffectiveSettingsWithDefaults(info, oldDefaults);
                MetricsSettings newEffective = getEffectiveSettings(info);

                // Only apply if something actually changed
                if (!oldEffective.equals(newEffective)) {
                    cluster.applyMetricsSettings(newEffective);
                }
            }
        }
    }

    /**
     * Updates cluster-specific metrics settings and applies changes to affected clusters.
     *
     * @param clusterName the cluster name
     * @param newSettings the new cluster-specific settings
     */
    public void updateClusterSettings(String clusterName, MetricsSettings newSettings) {
        if (clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("Cluster name cannot be null or empty");
        }
        if (newSettings == null) {
            throw new IllegalArgumentException("Cluster settings cannot be null");
        }

        clusterSettings.put(clusterName, newSettings);

        if (log.isInfoEnabled()) {
            log.info("Updated metrics settings for cluster: " + clusterName);
        }

        // Apply to all clusters with this name
        synchronized (activeClusters) {
            for (Map.Entry<Cluster, ClusterInfo> entry : activeClusters.entrySet()) {
                ClusterInfo info = entry.getValue();
                if (clusterName.equals(info.clusterName)) {
                    Cluster cluster = entry.getKey();
                    MetricsSettings effectiveSettings = getEffectiveSettings(info);
                    cluster.applyMetricsSettings(effectiveSettings);
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

        MetricsSettings removed = clusterSettings.remove(clusterName);
        if (removed == null) {
            return;  // Nothing to do
        }

        if (log.isInfoEnabled()) {
            log.info("Removed metrics settings for cluster: " + clusterName);
        }

        // Apply default settings to affected clusters
        synchronized (activeClusters) {
            for (Map.Entry<Cluster, ClusterInfo> entry : activeClusters.entrySet()) {
                ClusterInfo info = entry.getValue();

                if (clusterName.equals(info.clusterName)) {
                    Cluster cluster = entry.getKey();
                    MetricsSettings effectiveSettings = getEffectiveSettings(info);
                    cluster.applyMetricsSettings(effectiveSettings);
                }
            }
        }
    }

    /**
     * Helper method to calculate effective settings with specific default settings.
     * Used during dynamic updates to compare old vs new effective settings.
     */
    private MetricsSettings getEffectiveSettingsWithDefaults(ClusterInfo info, MetricsSettings defaults) {
        MetricsSettings result = MetricsSettings.DEFAULT;

        if (info.codeProvidedSettings != null) {
            result = info.codeProvidedSettings.mergeWith(result);
        }

        result = defaults.mergeWith(result);

        if (info.clusterName != null) {
            MetricsSettings clusterSpecific = clusterSettings.get(info.clusterName);
            if (clusterSpecific != null) {
                result = clusterSpecific.mergeWith(result);
            }
        }

        return result;
    }

    /**
     * Gets the current default settings.
     */
    public MetricsSettings getDefaultSettings() {
        return defaultSettings;
    }

    /**
     * Gets cluster-specific settings if they exist.
     *
     * @param clusterName the cluster name
     * @return cluster-specific settings or null if not defined
     */
    public MetricsSettings getClusterSettings(String clusterName) {
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
        final MetricsSettings codeProvidedSettings;  // Immutable

        ClusterInfo(String clusterName, MetricsSettings codeProvidedSettings) {
            this.clusterName = clusterName;
            this.codeProvidedSettings = codeProvidedSettings;
        }
    }
}

