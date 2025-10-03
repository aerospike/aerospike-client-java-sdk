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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.fluent.util.Version;

/**
 * Server node representation.  This class manages server node connections and health status.
 */
public class Node implements Closeable {
	/**
	 * Number of partitions for each namespace.
	 */
	public static final int PARTITIONS = 4096;

	public static final int HAS_PARTITION_SCAN = (1 << 0);
	public static final int HAS_QUERY_SHOW = (1 << 1);
	public static final int HAS_BATCH_ANY = (1 << 2);
	public static final int HAS_PARTITION_QUERY = (1 << 3);

	private static final String[] INFO_PERIODIC = new String[] {"node", "peers-generation", "partition-generation"};
	private static final String[] INFO_PERIODIC_REB = new String[] {"node", "peers-generation", "partition-generation", "rebalance-generation"};

	protected final Cluster cluster;
	private final String name;
	private String hostname; // Optional hostname.
	private final Host host; // Host with IP address name.
	protected final InetSocketAddress address;
	private final Pool[] connectionPools;
	private Connection tendConnection;
	private byte[] sessionToken;
	private long sessionExpiration;
	protected volatile Map<String,Integer> racks;
	//private volatile NodeMetrics metrics;
	final AtomicInteger connsOpened;
	final AtomicInteger connsClosed;
	private final AtomicInteger errorRateCount;
	protected int maxErrorRate;
	private final Counter errorCounter;
	private final Counter timeoutCounter;
	private final Counter keyBusyCounter;
	protected int connectionIter;
	private int peersGeneration;
	int partitionGeneration;
	private int rebalanceGeneration;
	protected int peersCount;
	protected int referenceCount;
	protected int failures;
	private final int features;
	protected boolean partitionChanged;
	protected boolean rebalanceChanged;
	protected volatile boolean performLogin;
	protected volatile boolean active;
	private final Version version;

	/**
	 * Initialize server node with connection parameters.
	 *
	 * @param cluster			collection of active server nodes
	 * @param nv				connection parameters
	 */
	public Node(Cluster cluster, NodeValidator nv) {
		ClusterDefinition def = cluster.def;

		this.cluster = cluster;
		this.name = nv.name;
		this.host = nv.primaryHost;
		this.address = nv.primaryAddress;
		this.tendConnection = nv.primaryConn;
		this.sessionToken = nv.sessionToken;
		this.sessionExpiration = nv.sessionExpiration;
		this.features = nv.features;
		this.version = nv.version;
		this.connsOpened = new AtomicInteger(1);
		this.connsClosed = new AtomicInteger(0);
		this.errorRateCount = new AtomicInteger(0);
		this.maxErrorRate = def.maxErrorRate;
		this.errorCounter = new Counter();
		this.timeoutCounter = new Counter();
		this.keyBusyCounter = new Counter();
		this.peersGeneration = -1;
		this.partitionGeneration = -1;
		this.rebalanceGeneration = -1;
		this.partitionChanged = true;
		this.rebalanceChanged = def.preferrredRacks != null;
		this.racks = this.rebalanceChanged ? new HashMap<String,Integer>() : null;
		this.active = true;

		// TODO: Handle metrics.
		/*
		if (cluster.metricsEnabled) {
			this.metrics = new NodeMetrics(cluster.metricsPolicy);
		}
		*/

		// Create sync connection pools.
		connectionPools = new Pool[def.connPoolsPerNode];
		int min = def.minConnsPerNode / def.connPoolsPerNode;
		int remMin = def.minConnsPerNode - (min * def.connPoolsPerNode);
		int max = def.maxConnsPerNode / def.connPoolsPerNode;
		int remMax = def.maxConnsPerNode - (max * def.connPoolsPerNode);

		for (int i = 0; i < connectionPools.length; i++) {
			int minSize = i < remMin ? min + 1 : min;
			int maxSize = i < remMax ? max + 1 : max;

			Pool pool = new Pool(minSize, maxSize);
			connectionPools[i] = pool;
		}
	}

	public final void createMinConnections() {
		// TODO: Implement
	}

	/**
	 * Put connection back into connection pool.
	 *
	 * @param conn					socket connection
	 */
	public final void putConnection(Connection conn) {
		if (! active || ! conn.pool.offer(conn)) {
			closeConnection(conn);
		}
	}

	public final void refresh(Peers peers) {
		// TODO: Implement
	}

	public final void refreshPeers(Peers peers) {
		// TODO: Implement
	}

	protected final void refreshPartitions(Peers peers) {
		// TODO: Implement
	}

	protected final void refreshRacks() {
		// TODO: Implement
	}

	final void balanceConnections() {
		// TODO: Implement
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(int timeoutMillis) {
		// TODO: Implement
		return null;
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(int connectTimeout, int socketTimeout) {
		// TODO: Implement
		return null;
	}

	public final void incrErrorRate() {
		// TODO add def to node instance instead of cluster??
		if (cluster.def.maxErrorRate > 0) {
			errorRateCount.getAndIncrement();
		}
	}

	public final void resetErrorRate() {
		if (isErrorRateValid()) {
			errorRateCount.set(0);
			// Error rate limit was not breached. Next error rate trigger is doubled up to a max of cluster maxErrorRate
			maxErrorRate = Math.min(maxErrorRate * 2, cluster.def.maxErrorRate);
		}
		else {
			errorRateCount.set(0);
			// Error rate limit was breached. Next error rate trigger is half.
			if (maxErrorRate >= 2) {
				maxErrorRate /= 2;
			}
			else {
				maxErrorRate = 1;
			}
		}
	}

	/**
	 * Add to the count of bytes sent to the node.
	 */
	public void addBytesOut(String namespace, long count) {
		// TODO: Implement
		//metrics.bytesOutCounter.increment(namespace, count);
	}

	/**
	 * Add to the count of bytes received from the node.
	 */
	public void addBytesIn(String namespace, long count) {
		// TODO: Implement
		//metrics.bytesInCounter.increment(namespace, count);
	}

	public boolean isMetricsEnabled() {
		// TODO: Implement
		//return cluster.metricsEnabled;
		return false;
	}

	private boolean isErrorRateValid() {
		return this.errorRateCount.get() <= this.maxErrorRate;
	}

	/**
	 * Return server node IP address and port.
	 */
	public final Host getHost() {
		return host;
	}

	/**
	 * Return whether node is currently active.
	 */
	public final boolean isActive() {
		return active;
	}

	/**
	 * Return server node name.
	 */
	public final String getName() {
		return name;
	}

	/**
	 * Return node IP address.
	 */
	public final InetSocketAddress getAddress() {
		return address;
	}

	/**
	 * Return node session token.
	 */
	public final byte[] getSessionToken() {
		return sessionToken;
	}

	/**
	 * Return current generation of cluster peers.
	 */
	public final int getPeersGeneration() {
		return peersGeneration;
	}

	/**
	 * Return current generation of partition maps.
	 */
	public final int getPartitionGeneration() {
		return partitionGeneration;
	}

	/**
	 * Return current generation of racks.
	 */
	public final int getRebalanceGeneration() {
		return rebalanceGeneration;
	}

	/**
	 * Return this node's build version
	 */
	public Version getVersion() {
		return version;
	}

	/**
	 * Close pooled connection on error and decrement connection count.
	 */
	public final void closeConnection(Connection conn) {
		conn.pool.total.getAndDecrement();
		closeConnectionOnError(conn);
	}

	/**
	 * Close any connection on error.
	 */
	public final void closeConnectionOnError(Connection conn) {
		connsClosed.getAndIncrement();
		incrErrorRate();
		conn.close();
	}

	/**
	 * Close connection without incrementing error count.
	 */
	public final void closeIdleConnection(Connection conn) {
		connsClosed.getAndIncrement();
		conn.close();
	}

	/**
	 * Close all socket connections.
	 */
	public final void close() {
		 // Mark node invalid.
		active = false;

		// Close tend connection after making reference copy.
		Connection conn = tendConnection;
		conn.close();

		// Close synchronous connections.
		for (Pool pool : connectionPools) {
			while ((conn = pool.poll()) != null) {
				conn.close();
			}
		}
	}
}
