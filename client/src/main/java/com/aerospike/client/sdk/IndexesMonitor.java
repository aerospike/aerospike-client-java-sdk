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

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.aerospike.ael.Index;
import com.aerospike.client.sdk.query.IndexType;

/**
 * Monitors secondary indexes in an Aerospike cluster and maintains an up-to-date
 * cache of available indexes.
 *
 * <p>This class runs a background thread that periodically queries the cluster
 * for secondary index information and updates an internal cache. The cached index
 * information is used by the query system to optimize query execution and provide
 * index-aware query planning.</p>
 *
 * <p>The monitor automatically starts when a Cluster is created and runs as a
 * daemon thread, so it will not prevent the JVM from shutting down. The monitor
 * can be stopped by calling {@link #stopMonitor()}.</p>
 *
 * <p>Example usage (typically handled automatically by Cluster):</p>
 * <pre>{@code
 * IndexesMonitor monitor = new IndexesMonitor();
 * boolean success = monitor.startMonitor(session, Duration.ofSeconds(5));
 * if (!success) {
 *     // Initial index fetch timed out, but monitoring continues in background
 *     System.out.println("Warning: Index fetch did not complete within 1 second");
 * }
 *
 * // Get current indexes
 * Set<Index> indexes = monitor.getIndexes();
 *
 * // Stop monitoring
 * monitor.stopMonitor();
 * }</pre>
 *
 * @see Cluster
 * @see Index
 * @see Session#info()
 */
// Package level visibility
class IndexesMonitor {
    private Set<Index> indexes = new HashSet<>();
    private Thread monitorThread = null;

    /**
     * Updates the internal cache of indexes.
     *
     * <p>This method is called by the monitoring thread to update the cached
     * index information. The method is synchronized to ensure thread safety.</p>
     *
     * @param indexes the new set of indexes to cache
     */
    private synchronized void setIndexes(Set<Index> indexes) {
        this.indexes = indexes;
    }

    /**
     * Starts monitoring the cluster for secondary index changes.
     *
     * <p>This method creates a daemon thread that periodically queries the cluster
     * for secondary index information. The thread will continue running until
     * {@link #stopMonitor()} is called or the thread is interrupted.</p>
     *
     * <p>The monitoring process:</p>
     * <ol>
     *   <li>Queries the cluster for all secondary indexes using {@link Session#info()}</li>
     *   <li>For each index, retrieves detailed information including entries per bin value</li>
     *   <li>Converts the information to {@link Index} objects</li>
     *   <li>Updates the internal cache</li>
     *   <li>Sleeps for the specified frequency before repeating</li>
     * </ol>
     *
     * <p>If the monitor is already running, this method returns true immediately
     * without starting a new monitor thread.</p>
     *
     * <p>The monitoring thread handles exceptions gracefully and logs errors
     * without stopping the monitoring process.</p>
     *
     * <p>This method waits up to 1 second for the initial index fetch to complete.
     * If the indexes are successfully retrieved within this time, the method returns
     * true. If the timeout expires before indexes are retrieved, the method returns
     * false but the monitoring thread continues running in the background.</p>
     *
     * @param session the session to use for querying index information
     * @param frequency how often to refresh the index information
     * @return true if the initial index fetch completed within 1 second, false otherwise
     * @throws IllegalArgumentException if session is null or frequency is null/negative
     */
    synchronized boolean startMonitor(Session session, Duration frequency) {
        if (monitorThread != null) {
            return true;
        }

        CountDownLatch initialFetchLatch = new CountDownLatch(1);

        monitorThread = new Thread(() -> {
            try {
                while (true) {
                    try {
                        Set<Index> indexes = new HashSet<>();
                        session.info().secondaryIndexes(false).stream()
                            .forEach(sindex -> {
                                session.info().secondaryIndexDetails(sindex, false).ifPresent(details -> {
                                   indexes.add(Index.builder()
                                               .namespace(sindex.getNamespace())
                                               .bin(sindex.getBin())
                                               .indexType(IndexType.valueOf(sindex.getType().name()))
                                               .binValuesRatio((int)details.getEntriesPerBval())
                                           .build());
                                });
                            });

                        this.setIndexes(indexes);
                        initialFetchLatch.countDown();
                        Thread.sleep(frequency.toMillis());
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    catch (Throwable th) {
                        Log.error("Error updating index information: " + th.getMessage());
                        initialFetchLatch.countDown();
                        try {
                            Thread.sleep(frequency.toMillis());
                        }
                        catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
            finally {
                monitorThread = null;
            }
        }, "indexMonitorThread");
        monitorThread.setDaemon(true);
        monitorThread.start();

        try {
            return initialFetchLatch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Gets the current set of cached indexes.
     *
     * <p>This method returns a snapshot of the currently cached indexes. The
     * returned set is thread-safe and represents the indexes as they were
     * last updated by the monitoring thread.</p>
     *
     * <p>If the monitor has not been started or no indexes have been discovered
     * yet, an empty set is returned.</p>
     *
     * @return a set of currently available indexes, or an empty set if none are available
     * @see Index
     */
    synchronized Set<Index> getIndexes() {
        return indexes;
    }

    /**
     * Stops the index monitoring thread.
     *
     * <p>This method interrupts the monitoring thread and waits for it to terminate.
     * After calling this method, the monitor will no longer update the cached index
     * information. The cached indexes remain available until the monitor is restarted.</p>
     *
     * <p>If the monitor is not currently running, this method has no effect.</p>
     *
     * <p>This method is typically called automatically when the Cluster is closed.</p>
     */
    synchronized void stopMonitor() {
        if (this.monitorThread != null) {
            this.monitorThread.interrupt();
            this.monitorThread = null;
        }
    }
}
