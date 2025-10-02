package com.aerospike.client.fluent;

import java.io.Closeable;
import java.time.Duration;
import java.util.Set;

import com.aerospike.client.fluent.policy.Behavior;

/**
 * Represents a connection to an Aerospike cluster.
 *
 * <p>This class manages the lifecycle of a connection to an Aerospike cluster,
 * including the underlying client, index monitoring, and record mapping factory.
 * It implements {@link Closeable} to ensure proper resource cleanup.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
 *     Session session = cluster.createSession(Behavior.DEFAULT);
 *     // Use the session for database operations...
 * }
 * }</pre>
 *
 * @see ClusterDefinition
 * @see Session
 * @see Behavior
 */
public class Cluster implements Closeable {
    /**
     * Default interval for refreshing index information from the cluster.
     */
    public static final Duration INDEX_REFRESH = Duration.ofSeconds(5);

    private ClusterDefinition def;
    //private IndexesMonitor indexesMonitor;
    private RecordMappingFactory recordMappingFactory = null;

    Cluster(ClusterDefinition def, Host[] seeds) {
        this.def = def;
        //this.indexesMonitor = new IndexesMonitor();
        //this.indexesMonitor.startMonitor(createSession(Behavior.DEFAULT), INDEX_REFRESH);

    }

    /**
     * Gets the set of available indexes in the cluster.
     *
     * <p>This returns the current set of secondary indexes that are available
     * for querying. The index information is automatically refreshed at regular
     * intervals.</p>
     *
     * @return a set of Index objects representing available secondary indexes
     * @see Index
     */
    /*
    public Set<Index> getIndexes() {
        return indexesMonitor.getIndexes();
    }
    */

    /**
     * Sets the record mapping factory for this cluster.
     *
     * <p>The record mapping factory is responsible for providing mappers that
     * convert between Aerospike records and Java objects. This enables automatic
     * object serialization/deserialization when working with typed datasets.</p>
     *
     * @param factory the record mapping factory to use
     * @return this Cluster for method chaining
     * @see RecordMappingFactory
     * @see DefaultRecordMappingFactory
     */
    public Cluster setRecordMappingFactory(RecordMappingFactory factory) {
        this.recordMappingFactory = factory;
        return this;
    }

    /**
     * Creates a new session with the specified behavior.
     *
     * <p>A session represents a logical connection to the cluster with specific
     * behavior settings that control how operations are performed (timeouts,
     * retry policies, consistency levels, etc.).</p>
     *
     * @param behavior the behavior configuration for the session
     * @return a new Session instance
     * @see Session
     * @see Behavior
     */
    public Session createSession(Behavior behavior) {
        return new Session(this, behavior);
    }

    /**
     * Gets the current record mapping factory.
     *
     * @return the current record mapping factory, or null if none is set
     * @see RecordMappingFactory
     */
    public RecordMappingFactory getRecordMappingFactory() {
        return recordMappingFactory;
    }

    /**
     * Checks if the cluster connection is currently active.
     *
     * @return true if the connection is active, false otherwise
     */
    public boolean isConnected() {
    	return false;
        //return client.isConnected();
    }

    /**
     * Closes the cluster connection and releases all associated resources.
     *
     * <p>This method stops the index monitor and closes the underlying client
     * connection. It should be called when the cluster is no longer needed
     * to ensure proper resource cleanup.</p>
     *
     * <p>This method is automatically called when using try-with-resources:</p>
     * <pre>{@code
     * try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
     *     // Use the cluster...
     * } // cluster.close() is automatically called here
     * }</pre>
     */
    @Override
    public void close() {
        //indexesMonitor.stopMonitor();
        //this.client.close();
    }
}
