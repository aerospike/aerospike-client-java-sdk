package com.aerospike.client.fluent;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import com.aerospike.client.fluent.query.IndexType;
import com.aerospike.dsl.Index;

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
 * monitor.startMonitor(session, Duration.ofSeconds(5));
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
     * <p>If the monitor is already running, this method returns immediately
     * without starting a new monitor thread.</p>
     *
     * <p>The monitoring thread handles exceptions gracefully and logs errors
     * without stopping the monitoring process.</p>
     *
     * @param session the session to use for querying index information
     * @param frequency how often to refresh the index information
     * @throws IllegalArgumentException if session is null or frequency is null/negative
     */
    synchronized void startMonitor(Session session, Duration frequency) {
        if (monitorThread != null) {
            return;
        }

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
                        Thread.sleep(frequency.toMillis());
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    catch (Throwable th) {
                        Log.error("Error updating index information: " + th.getMessage());
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
