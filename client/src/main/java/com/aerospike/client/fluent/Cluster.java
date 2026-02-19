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
package com.aerospike.client.fluent;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.tend.ClusterTend;
import com.aerospike.client.fluent.tend.ConnectionRecover;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.client.fluent.util.Util;
import com.aerospike.dsl.Index;

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

    ClusterDefinition def;
    ClusterTend tend;
	volatile Node[] nodes;
	volatile HashMap<String,Partitions> partitionMap;
	private final ThreadFactory threadFactory;
	private final AtomicLong commandCount;
	private final AtomicLong retryCount;
	private final AtomicInteger nodeIndex;
	private final AtomicInteger replicaIndex;
	private final AtomicBoolean closed;
	final Log.Context context;
	private boolean metricsEnabled;

	private final IndexesMonitor indexesMonitor;
    private RecordMappingFactory recordMappingFactory = null;

    Cluster(ClusterDefinition def, SystemSettings effectiveSettings) {
        this.def = def;
		nodes = new Node[0];
		partitionMap = new HashMap<String,Partitions>();
		threadFactory = Thread.ofVirtual().name("Aerospike-", 0L).factory();
		commandCount = new AtomicLong();
		retryCount = new AtomicLong();
		nodeIndex = new AtomicInteger();
		replicaIndex = new AtomicInteger();
		closed = new AtomicBoolean();
		context = def.context;

		this.applySystemSettings(effectiveSettings);

        tend = new ClusterTend(this);

		if (def.forceSingleNode) {
			forceSingleNode();
		}
		else {
			tend.runThread();
		}

		this.indexesMonitor = new IndexesMonitor();
        this.indexesMonitor.startMonitor(createSession(Behavior.DEFAULT), INDEX_REFRESH);

        // TODO: Create implementation where this sleep is not needed.
        Util.sleep(1000);
    }

    /**
     * Gets an executor service for running tasks using virtual threads.
     *
     * <p>This method returns a new executor service that creates a new virtual thread
     * for each submitted task. The executor uses the cluster's thread factory which
     * creates threads with names prefixed with "Aerospike-".</p>
     *
     * @return an ExecutorService that creates a new virtual thread per task
     */
    public ExecutorService getExecutorService() {
    	return Executors.newThreadPerTaskExecutor(threadFactory);
    }

    /**
     * Starts a new virtual thread to execute the given runnable.
     *
     * <p>This is a convenience method for creating and starting a virtual thread
     * using the cluster's thread factory. The thread will be named with the
     * "Aerospike-" prefix.</p>
     *
     * @param runnable the runnable to execute in the new thread
     */
    public void startVirtualThread(Runnable runnable) {
        threadFactory.newThread(runnable).start();
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
    public Set<Index> getIndexes() {
        return indexesMonitor.getIndexes();
    }

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
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length > 0 && tend.isActive()) {
			// Even though nodes exist, they may not be currently responding.  Check further.
			for (Node node : nodeArray) {
				// Mark connected if any node is active and cluster tend consecutive info request
				// failures are less than 5.
				if (node.active && node.failures < 5) {
					return true;
				}
			}
		}
		return false;
    }

    /**
     * Gets the cluster definition used to create this cluster connection.
     *
     * <p>The cluster definition contains the configuration settings that were
     * used to establish the connection, including seed hosts, connection pool
     * settings, and other cluster-specific parameters.</p>
     *
     * @return the ClusterDefinition instance for this cluster
     * @see ClusterDefinition
     */
    public ClusterDefinition getClusterDefinition() {
        return def;
    }

    /**
     * Gets the cluster name.
     *
     * <p>This may be the name provided via {@code validateClusterName()}, or the
     * name discovered from the server if none was provided.</p>
     *
     * @return the cluster name, or null if not specified and not yet discovered
     */
    public String getClusterName() {
        return def.clusterName;
    }

    /**
     * Applies system settings dynamically to this cluster.
     * Called by {@link SystemSettingsRegistry} when settings are updated.
     *
     * <p><b>Note:</b> This is an internal method and should not be called directly.
     * System settings are automatically managed by the registry.</p>
     *
     * @param settings the system settings to apply
     */
    void applySystemSettings(SystemSettings settings) {
        if (settings == null) {
            return;
        }

        if (settings.getMinimumConnectionsPerNode() != null) {
        	setMinConnsPerNode(settings.getMinimumConnectionsPerNode());
        }

        if (settings.getMaximumConnectionsPerNode() != null) {
        	setMaxConnsPerNode(settings.getMaximumConnectionsPerNode());
        }

        if (settings.getMaximumErrorsInErrorWindow() != null) {
            def.maxErrorRate = settings.getMaximumErrorsInErrorWindow();
        }

        if (settings.getNumTendIntervalsInErrorWindow() != null) {
        	def.errorRateWindow = settings.getNumTendIntervalsInErrorWindow();
        }

        if (settings.getTendInterval() != null) {
            this.def.tendInterval = (int)settings.getTendInterval().toMillis();
        }

        if (settings.getMaximumSocketIdleTime() != null) {
            this.def.maxSocketIdleNanosTrim = settings.getMaximumSocketIdleTime().toNanos();
        }

        // Currently, the Aerospike Java client does not support dynamic updates
        // to system-level settings like connection pool sizes, socket idle times,
        // circuit breaker settings, or tend intervals.
        //
        // These settings are applied at connection time via ClientPolicy.
        // This method is a placeholder for future enhancement.
        //
        // When dynamic updates become available, implement them here:
        // - client.setMinConnsPerNode(settings.getMinimumConnectionsPerNode())
        // - client.setMaxConnsPerNode(settings.getMaximumConnectionsPerNode())
        // - etc.

        Log.info("System settings updated for cluster '" +
            (def.clusterName != null ? def.clusterName : "(unnamed)") +
            "'. Note: Settings will take effect on next connection.");

        if (Log.debugEnabled()) {
            Log.debug("\tMinConnsPerNode=%,d;MaxConnsPerNode=%,d;MaxErrorRate=%,d;ErrorRateWindow=%,d;TendInterval=%,dms;MaxSocketIdleNanos=%,dns"
                    .formatted(this.def.minConnsPerNode,
                            this.def.maxConnsPerNode,
                            this.def.maxErrorRate,
                            this.def.errorRateWindow,
                            this.def.tendInterval,
                            this.def.maxSocketIdleNanosTrim));
        }
    }

    /**
     * Gets a random active node from the cluster.
     *
     * <p>This method selects an active node using a round-robin approach based on
     * an internal counter. It iterates through the available nodes to find one that
     * is currently active.</p>
     *
     * @return an active Node from the cluster
     * @throws AerospikeException.InvalidNode if the cluster is empty or no active
     *         nodes are available
     */
    public final Node getRandomNode() throws AerospikeException.InvalidNode {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length > 0) {
			int index = Math.abs(nodeIndex.getAndIncrement() % nodeArray.length);

			for (int i = 0; i < nodeArray.length; i++) {
				Node node = nodeArray[index];

				if (node.isActive()) {
					return node;
				}
				index++;
				index %= nodeArray.length;
			}
		}
		throw new AerospikeException.InvalidNode("Cluster is empty");
	}

	/**
	 * Gets a copy of the current array of nodes in the cluster.
	 *
	 * <p>The returned array is a snapshot of the nodes at the time of the call.
	 * The cluster may update its node list asynchronously, so the returned array
	 * may not reflect the current state of the cluster.</p>
	 *
	 * @return an array of Node objects representing the current cluster nodes
	 */
	public final Node[] getNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray;
	}

	/**
	 * Validates that the cluster has at least one node available.
	 *
	 * <p>This method checks if the cluster has any nodes and throws an exception
	 * if the cluster is empty. It returns a snapshot of the current node array
	 * if validation succeeds.</p>
	 *
	 * @return an array of Node objects representing the current cluster nodes
	 * @throws AerospikeException with ResultCode.SERVER_NOT_AVAILABLE if the
	 *         cluster is empty
	 */
	public final Node[] validateNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Cluster is empty");
		}
		return nodeArray;
	}

	/**
	 * Gets a node by its name.
	 *
	 * <p>This method searches for a node in the cluster with the specified name
	 * and returns it if found. If no node with the given name exists, an exception
	 * is thrown.</p>
	 *
	 * @param nodeName the name of the node to retrieve
	 * @return the Node with the specified name
	 * @throws AerospikeException.InvalidNode if no node with the given name exists
	 */
	public final Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		Node node = findNode(nodeName);

		if (node == null) {
			throw new AerospikeException.InvalidNode("Invalid node name: " + nodeName);
		}
		return node;
	}

	protected final Node findNode(String nodeName) {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		for (Node node : nodeArray) {
			if (node.getName().equals(nodeName)) {
				return node;
			}
		}
		return null;
	}

	/**
	 * Gets the partition map for all namespaces in the cluster.
	 *
	 * <p>The partition map contains information about how data is partitioned
	 * across nodes in the cluster for each namespace. This information is used
	 * for routing operations to the correct nodes.</p>
	 *
	 * @return a map from namespace names to their Partitions objects
	 * @see Partitions
	 */
	public final HashMap<String,Partitions> getPartitionMap() {
		return partitionMap;
	}

	/**
	 * Recovers a connection that has failed or become invalid.
	 *
	 * <p>This method is used internally to recover connections that have encountered
	 * errors. The connection recovery process attempts to re-establish the connection
	 * and restore it to a usable state.</p>
	 *
	 * @param cs the ConnectionRecover object containing connection recovery information
	 * @see ConnectionRecover
	 */
	public final void recoverConnection(ConnectionRecover cs) {
		tend.recoverConnection(cs);
	}

	private void setMinConnsPerNode(int min) {
		def.minConnsPerNode = min;

 		Node[] nodeArray = nodes;

		for (Node node : nodeArray) {
			node.setMinConnections(min);
		}
	}

	private final void setMaxConnsPerNode(int max) {
		def.maxConnsPerNode = max;

		Node[] nodeArray = nodes;

		for (Node node : nodeArray) {
			node.setMaxConnections(max);
		}
	}

    private void forceSingleNode() {
		// Communicate with the first seed node only.
		// Do not run cluster tend thread.
    	// For testing purposes only.
		try {
			tend.forceSingleNode();
		}
		catch (Throwable e) {
			close();
			throw e;
		}
    }

	/**
	 * Gets the log context for this cluster.
	 *
	 * <p>The log context provides logging functionality specific to this cluster
	 * instance, allowing log messages to be associated with the cluster connection.</p>
	 *
	 * @return the Log.Context for this cluster
	 * @see Log.Context
	 */
	public final Log.Context getLogContext() {
		return context;
	}

    /**
     * Increments and returns the replica index.
     *
     * <p>This method is used internally to select replica nodes for operations.
     * The replica index is incremented atomically and used in a round-robin
     * fashion to distribute operations across available replicas.</p>
     *
     * @return the incremented replica index value
     */
    public final int incrReplicaIndex() {
    	return replicaIndex.getAndIncrement();
    }

	/**
	 * Increment command count when metrics are enabled.
	 */
	public final void addCommandCount() {
		if (metricsEnabled) {
			commandCount.getAndIncrement();
		}
	}

	/**
	 * Return command count. The value is cumulative and not reset per metrics interval.
	 */
	public final long getCommandCount() {
		return commandCount.get();
	}

	/**
	 * Increment command retry count. There can be multiple retries for a single command.
	 */
	public final void addRetry() {
		retryCount.getAndIncrement();
	}

	/**
	 * Add command retry count. There can be multiple retries for a single command.
	 */
	public final void addRetries(int count) {
		retryCount.getAndAdd(count);
	}

	/**
	 * Return command retry count. The value is cumulative and not reset per metrics interval.
	 */
	public final long getRetryCount() {
		return retryCount.get();
	}

	/**
	 * Sets the array of nodes for this cluster.
	 *
	 * <p>This method updates the cluster's node list. It is typically called
	 * internally by the cluster tend mechanism when nodes are discovered or
	 * removed from the cluster.</p>
	 *
	 * @param nodes the new array of nodes for the cluster
	 */
	public final void setNodes(Node[] nodes) {
    	this.nodes = nodes;
    }

    /**
     * Checks if the cluster tend mechanism is currently active.
     *
     * <p>The cluster tend mechanism is responsible for monitoring the cluster
     * topology and keeping the node list up to date. This method indicates
     * whether that monitoring is currently running.</p>
     *
     * @return true if the cluster tend mechanism is active, false otherwise
     * @see ClusterTend
     */
    public final boolean isActive() {
    	return tend.isActive();
    }

	/**
	 * Gets the partition map for a specific namespace.
	 *
	 * <p>The partition map contains information about how data is partitioned
	 * across nodes in the cluster for the specified namespace. This information
	 * is used for routing operations to the correct nodes based on partition
	 * assignments.</p>
	 *
	 * @param namespace the namespace for which to retrieve partition information
	 * @return the Partitions object containing partition mapping for the namespace
	 * @throws AerospikeException.InvalidNamespace if the specified namespace
	 *         does not exist in the cluster's partition map
	 */
	public Partitions getPartitions(String namespace) {
        HashMap<String, Partitions> map = this.partitionMap;
        Partitions partitions = map.get(namespace);

        if (partitions == null) {
            throw new AerospikeException.InvalidNamespace(namespace, map.size());
        }
        return partitions;
    }

	final void logPartitionMap() {
		for (Entry<String,Partitions> entry : partitionMap.entrySet()) {
			String namespace = entry.getKey();
			Partitions partitions = entry.getValue();

			partitions.log(context, namespace);
		}
	}

    /**
     * Close the cluster connection and releases all associated resources.
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
    	if (! closed.compareAndSet(false, true)) {
			// close() has already been called.
			return;
		}

        indexesMonitor.stopMonitor();
    	tend.close();

    	/* TODO Handle metrics close.
		synchronized(metricsLock) {
			try {
				disableMetricsInternal();
			}
			catch (Throwable e) {
				Log.warn("DisableMetrics failed: " + Util.getErrorMessage(e));
			}
		}
		*/

		Node[] nodeArray = nodes;

		for (Node node : nodeArray) {
			node.close();
		}
    }
}
