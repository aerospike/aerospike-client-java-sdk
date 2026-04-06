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
package com.aerospike.client.sdk.policy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.sdk.Log;

/**
 * Monitors a YAML file for changes and dynamically reloads behaviors
 */
class BehaviorFileMonitor implements Closeable {

    private static final BehaviorFileMonitor INSTANCE = new BehaviorFileMonitor();
    private static final long DEFAULT_RELOAD_DELAY_MS = 1000; // 1 second delay to avoid multiple reloads

    private final BehaviorRegistry registry = BehaviorRegistry.getInstance();
    // Use a scheduled executor with 2 threads: one for monitoring, one for reload tasks
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "BehaviorFileMonitor");
        t.setDaemon(true);
        return t;
    });

    private Path yamlFilePath;
    private WatchService watchService;
    private boolean isMonitoring = false;
    private long lastModified = 0;
    private long reloadDelayMs = DEFAULT_RELOAD_DELAY_MS;

    private BehaviorFileMonitor() {
        // Private constructor for singleton
    }

    /**
     * Get the singleton instance
     */
    public static BehaviorFileMonitor getInstance() {
        return INSTANCE;
    }

    /**
     * Start monitoring a YAML file for changes
     *
     * @param yamlFilePath The path to the YAML file to monitor
     * @throws IOException if there's an error setting up the file monitoring
     */
    public void startMonitoring(String yamlFilePath) throws IOException {
        startMonitoring(yamlFilePath, DEFAULT_RELOAD_DELAY_MS);
    }

    /**
     * Start monitoring a YAML file for changes with a custom reload delay
     *
     * @param yamlFilePath The path to the YAML file to monitor
     * @param reloadDelayMs The delay in milliseconds before reloading after a change
     * @throws IOException if there's an error setting up the file monitoring
     */
    public void startMonitoring(String yamlFilePath, long reloadDelayMs) throws IOException {
        if (isMonitoring) {
            stopMonitoring();
        }

        this.yamlFilePath = Paths.get(yamlFilePath);
        this.reloadDelayMs = reloadDelayMs;

        // Verify the file exists
        if (!Files.exists(this.yamlFilePath)) {
            throw new IOException("YAML file does not exist: " + yamlFilePath);
        }

        // Record the initial modification time
        this.lastModified = Files.getLastModifiedTime(this.yamlFilePath).toMillis();

        // Load initial behaviors
        loadBehaviors();

        // Set up file watching
        watchService = this.yamlFilePath.getFileSystem().newWatchService();
        Path parentDir = this.yamlFilePath.getParent();
        parentDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

        // Start monitoring thread
        isMonitoring = true;
        executor.submit(this::monitorFile);

        Log.debug("Started monitoring YAML file: " + yamlFilePath);
    }

    /**
     * Stop monitoring the file
     */
    public void stopMonitoring() {
        isMonitoring = false;

        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                Log.error("Error closing watch service: " + e.getMessage());
            }
            watchService = null;
        }

        Log.info("Stopped monitoring YAML file");
    }

    /**
     * Check if monitoring is active
     */
    public boolean isMonitoring() {
        return isMonitoring;
    }

    /**
     * Get the path of the monitored file
     */
    public String getMonitoredFilePath() {
        return yamlFilePath != null ? yamlFilePath.toString() : null;
    }

    /**
     * Manually reload behaviors from the YAML file
     */
    void reloadBehaviors() {
        try {
            loadBehaviors();
            Log.info("Manually reloaded behaviors from: " + yamlFilePath);
        } catch (Exception e) {
            Log.error("Error reloading behaviors: " + e.getMessage());
        }
    }

    /**
     * Monitor the file for changes
     */
    private void monitorFile() {
        while (isMonitoring && watchService != null) {
            try {
                WatchKey key = watchService.take();

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                        Path changedFile = (Path) event.context();
                        Path fullPath = yamlFilePath.getParent().resolve(changedFile);

                        // Check if the changed file is our YAML file
                        if (fullPath.equals(yamlFilePath)) {
                            handleFileChange();
                        }
                    }
                }

                boolean resetValid = key.reset();
                if (!resetValid) {
                    Log.warn("WatchKey is no longer valid, stopping file monitoring");
                    break;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ClosedWatchServiceException e) {
                // There is not a isClsoed method which would allow us to check if the watch service is closed, so we have to catch this exception to know that the watch service is closed.
                // Within this context this is considered normal
                Log.info("Watch service is closed.");
                break;
            } catch (Exception e) {
                Log.error("Error in file monitoring: " + e.getMessage());
            }
        }
    }

    /**
     * Handle file change events
     */
    private void handleFileChange() {
        try {
            // Check if file was actually modified
            long currentModified = Files.getLastModifiedTime(yamlFilePath).toMillis();

            if (currentModified <= lastModified) {
                return; // File hasn't actually changed
            }

            lastModified = currentModified;

            // Schedule reload with delay to avoid multiple reloads
            executor.schedule(this::loadBehaviors, reloadDelayMs, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            Log.error("Error handling file change: " + e.getMessage());
        }
    }

    /**
     * Load behaviors from the YAML file
     */
    private void loadBehaviors() {
        try {
            File yamlFile = yamlFilePath.toFile();
            Map<String, Behavior> updatedBehaviors = BehaviorYamlLoader.loadBehaviorsFromFile(yamlFile);

            // Clear caches of all behaviors to ensure they pick up any changes
            Map<String, Behavior> allBehaviors = registry.getAllBehaviors();
            for (Behavior behavior : allBehaviors.values()) {
                if (behavior != Behavior.DEFAULT) {
                    behavior.clearCache();
                }
            }
            Behavior.DEFAULT.clearCache();

            Log.info("Updated " + updatedBehaviors.size() + " behaviors from: " + yamlFilePath);

        } catch (IOException e) {
            Log.error("Error reading or parsing YAML file: " + e.getMessage());
        } catch (Exception e) {
            Log.error("Unexpected error loading behaviors: " + e.getMessage());
        }
    }

    /**
     * Shutdown the monitor and cleanup resources
     */
    public void shutdown() {
        stopMonitoring();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Close the monitor and cleanup resources.
     * This method is called automatically when using try-with-resources.
     */
    @Override
    public void close() throws IOException {
        shutdown();
    }
}
