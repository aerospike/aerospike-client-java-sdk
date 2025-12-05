/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.fluent.dsl.Index;
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

		// TODO: Resolve when new DSL library is available.
		//this.indexesMonitor = new IndexesMonitor();
        //this.indexesMonitor.startMonitor(createSession(Behavior.DEFAULT), INDEX_REFRESH);
		this.indexesMonitor = null;

        tend = new ClusterTend(this);

		if (def.forceSingleNode) {
			forceSingleNode();
		}
		else {
			tend.runThread();
		}
    }

    public ExecutorService getExecutorService() {
    	return Executors.newThreadPerTaskExecutor(threadFactory);
    }

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

	public final Node[] getNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray;
	}

	public final Node[] validateNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Cluster is empty");
		}
		return nodeArray;
	}

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

	public final HashMap<String,Partitions> getPartitionMap() {
		return partitionMap;
	}

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

	public final void printPartitionMap() {
		for (Entry<String,Partitions> entry : partitionMap.entrySet()) {
			String namespace = entry.getKey();
			Partitions partitions = entry.getValue();
			AtomicReferenceArray<Node>[] replicas = partitions.replicas;

			for (int i = 0; i < replicas.length; i++) {
				AtomicReferenceArray<Node> nodeArray = replicas[i];
				int max = nodeArray.length();

				for (int j = 0; j < max; j++) {
					Node node = nodeArray.get(j);

					if (node != null) {
						Log.info(context, namespace + ',' + i + ',' + j + ',' + node);
					}
				}
			}
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

    final int incrReplicaIndex() {
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

	final void setNodes(Node[] nodes) {
    	this.nodes = nodes;
    }

    public final boolean isActive() {
    	return tend.isActive();
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
    	// TODO: Call stopMonitor() when DSL is supported.
        //indexesMonitor.stopMonitor();

    	if (! closed.compareAndSet(false, true)) {
			// close() has already been called.
			return;
		}

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
