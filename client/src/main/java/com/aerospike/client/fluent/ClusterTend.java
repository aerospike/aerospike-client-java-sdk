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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.fluent.exception.AeroException;
import com.aerospike.client.fluent.util.Util;

/**
 * Cluster tend thread.
 */
public class ClusterTend implements Runnable {
	private final Cluster cluster;
	private final ClusterDefinition def;
	private final Thread tendThread;
	private final HashMap<String,Node> nodesMap;
	private volatile Host[] seeds;
	private int tendCount;
	private volatile int invalidNodeCount;
	private volatile boolean tendValid;

    ClusterTend(Cluster cluster, Host[] seeds) {
    	this.cluster = cluster;
    	this.seeds = seeds;
    	this.def = cluster.def;
		this.nodesMap = new HashMap<String,Node>();

		// Tend cluster until all nodes identified.
		waitTillStabilized(def.failIfNotConnected);

		if (Log.debugEnabled()) {
			for (Host host : seeds) {
				Log.debug("Add seed " + host);
			}
		}

		// Add other nodes as seeds, if they don't already exist.
		ArrayList<Host> seedsToAdd = new ArrayList<Host>(cluster.nodes.length);
		for (Node node : cluster.nodes) {
			Host host = node.getHost();
			if (! findSeed(host)) {
				seedsToAdd.add(host);
			}
		}

		if (seedsToAdd.size() > 0) {
			addSeeds(seedsToAdd.toArray(new Host[seedsToAdd.size()]));
		}

		// Run cluster tend thread.
		tendValid = true;
		tendThread = new Thread(this);
		tendThread.setName("tend");
		tendThread.setDaemon(true);
		tendThread.start();
    }

	public final void run() {
		while (tendValid) {
			// Tend cluster.
			try {
				tend(false, false);
			}
			catch (Throwable e) {
				if (Log.warnEnabled()) {
					Log.warn("Cluster tend failed: " + Util.getErrorMessage(e));
				}
			}
			// Sleep between polling intervals.
			Util.sleep(def.tendInterval);
		}
	}

	/**
	 * Tend the cluster until it has stabilized and return control.
	 * This helps avoid initial database request timeout issues when
	 * a large number of threads are initiated at client startup.
	 */
	private final void waitTillStabilized(boolean failIfNotConnected) {
		// Tend now requests partition maps in same iteration as the nodes
		// are added, so there is no need to call tend twice anymore.
		tend(failIfNotConnected, true);

		if (cluster.nodes.length == 0) {
			String message = "Cluster seed(s) failed";

			if (failIfNotConnected) {
				throw new AeroException(message);
			}
			else {
				Log.warn(message);
			}
		}
	}

	/**
	 * Check health of all nodes in the cluster.
	 */
	private final void tend(boolean failIfNotConnected, boolean isInit) {
		// All node additions/deletions are performed in tend thread.
		// Initialize tend iteration node statistics.
		Node[] nodes = cluster.nodes;
		Peers peers = new Peers(nodes.length + 16);

		// Clear node reference counts.
		for (Node node : nodes) {
			node.referenceCount = 0;
			node.partitionChanged = false;
			node.rebalanceChanged = false;
		}

		// If active nodes don't exist, seed cluster.
		if (nodes.length == 0) {
			seedNode(peers, failIfNotConnected);

			// Abort cluster init if all peers of the seed are not reachable and failIfNotConnected is true.
			if (isInit && failIfNotConnected && nodes.length == 1 && peers.getInvalidCount() > 0) {
				peers.clusterInitError();
			}
		}
		else {
			// Refresh all known nodes.
			for (Node node : nodes) {
				node.refresh(peers);
			}

			// Refresh peers when necessary.
			if (peers.genChanged) {
				// Refresh peers for all nodes that responded the first time even if only one node's peers changed.
				peers.refreshCount = 0;

				for (Node node : nodes) {
					node.refreshPeers(peers);
				}

				// Handle nodes changes determined from refreshes.
				findNodesToRemove(peers);

				// Remove nodes in a batch.
				if (peers.removeNodes.size() > 0) {
					removeNodes(peers.removeNodes);
				}
			}

			// Add peer nodes to cluster.
			if (peers.nodes.size() > 0) {
				addNodes(peers.nodes);
				refreshPeers(peers);
			}
		}

		invalidNodeCount += peers.getInvalidCount();

		// Refresh partition map when necessary.
		for (Node node : nodes) {
			if (node.partitionChanged) {
				node.refreshPartitions(peers);
			}

			if (node.rebalanceChanged) {
				node.refreshRacks();
			}
		}

		tendCount++;

		// Balance connections every 30 tend iterations.
		if (tendCount % 30 == 0) {
			for (Node node : nodes) {
				node.balanceConnections();
			}
		}

		// Reset connection error window for all nodes every connErrorWindow tend iterations.
		if (tendCount % def.errorRateWindow == 0) {
			for (Node node : nodes) {
				node.resetErrorRate();
			}
		}

		// Perform metrics snapshot.
		// TODO: Handle metrics
		/*
		synchronized(metricsLock) {
			if (metricsEnabled && (tendCount % metricsPolicy.interval) == 0) {
				metricsListener.onSnapshot(this);
			}
		}
		*/

		// Convert config interval from a millisecond duration to the number of cluster tend
		// iterations.
		int interval = def.configInterval / def.tendInterval;

		// Check configuration file for updates.
		if (def.configPath != null && tendCount % interval == 0) {
			try {
				// TODO: Handle dynamic config.
				//loadConfiguration();
			}
			catch (Throwable t) {
				if (Log.warnEnabled()) {
					Log.warn("Dynamic configuration failed: " + t);
				}
			}
		}

		// TODO: Handle connection recovery.
		//processRecoverQueue();
	}

	private boolean seedNode(Peers peers, boolean failIfNotConnected) {
		// Must copy array reference for copy on write semantics to work.
		Host[] seedArray = seeds;
		Throwable[] exceptions = null;
		NodeValidator nv = new NodeValidator();

		for (int i = 0; i < seedArray.length; i++) {
			Host seed = seedArray[i];

			try {
				Node node = nv.seedNode(cluster, seed, peers);

				if (node != null) {
					addSeedAndPeers(node, peers);
					return true;
				}
			}
			catch (Throwable e) {
				peers.fail(seed);

				if (seed.tlsName != null && def.tlsBuilder == null) {
					// Fail immediately for known configuration errors like this.
					throw new AeroException.Connection("Seed host tlsName '" + seed.tlsName +
						"' defined but client tlsPolicy not enabled", e);
				}

				// Store exception and try next seed.
				if (failIfNotConnected) {
					if (exceptions == null) {
						exceptions = new Exception[seedArray.length];
					}
					exceptions[i] = e;
				}
				else {
					if (Log.warnEnabled()) {
						Log.warn("Seed " + seed + " failed: " + Util.getErrorMessage(e));
					}
				}
			}
		}

		// No seeds valid. Use fallback node if it exists.
		if (nv.fallback != null) {
			// When a fallback is used, peers refreshCount is reset to zero.
			// refreshCount should always be one at this point.
			peers.refreshCount = 1;
			addSeedAndPeers(nv.fallback, peers);
			return true;
		}

		if (failIfNotConnected) {
			StringBuilder sb = new StringBuilder(500);
			sb.append("Failed to connect to ["+ seedArray.length +"] host(s): ");
			sb.append(System.lineSeparator());

			for (int i = 0; i < seedArray.length; i++) {
				sb.append(seedArray[i]);
				sb.append(' ');

				Throwable ex = exceptions == null ? null : exceptions[i];

				if (ex != null) {
					sb.append(ex.getMessage());
					sb.append(System.lineSeparator());
				}
			}
			throw new AeroException.Connection(sb.toString());
		}
		return false;
	}

	private void addSeedAndPeers(Node seed, Peers peers) {
		seed.createMinConnections();
		nodesMap.clear();

		addNodes(seed, peers);

		if (peers.nodes.size() > 0) {
			refreshPeers(peers);
		}
	}

	private void refreshPeers(Peers peers) {
		// Iterate until peers have been refreshed and all new peers added.
		while (true) {
			// Copy peer node references to array.
			Node[] nodeArray = new Node[peers.nodes.size()];
			int count = 0;

			for (Node node : peers.nodes.values()) {
				nodeArray[count++] = node;
			}

			// Reset peer nodes.
			peers.nodes.clear();

			// Refresh peers of peers in order retrieve the node's peersCount
			// which is used in RefreshPartitions(). This call might add even
			// more peers.
			for (Node node : nodeArray) {
				node.refreshPeers(peers);
			}

			if (peers.nodes.size() > 0) {
				// Add new peer nodes to cluster.
				addNodes(peers.nodes);
			}
			else {
				break;
			}
		}
	}

	private void addSeeds(Host[] hosts) {
		// Use copy on write semantics.
		Host[] seedArray = new Host[seeds.length + hosts.length];
		int count = 0;

		// Add existing seeds.
		for (Host seed : seeds) {
			seedArray[count++] = seed;
		}

		// Add new seeds
		for (Host host : hosts) {
			if (Log.debugEnabled()) {
				Log.debug("Add seed " + host);
			}
			seedArray[count++] = host;
		}

		// Replace nodes with copy.
		seeds = seedArray;
	}

	private boolean findSeed(Host search) {
		for (Host seed : seeds) {
			if (seed.equals(search)) {
				return true;
			}
		}
		return false;
	}

	private final void findNodesToRemove(Peers peers) {
		int refreshCount = peers.refreshCount;
		HashSet<Node> removeNodes = peers.removeNodes;

		for (Node node : cluster.nodes) {
			if (! node.isActive()) {
				// Inactive nodes must be removed.
				removeNodes.add(node);
				continue;
			}

			if (refreshCount == 0 && node.failures >= 5) {
				// All node info requests failed and this node had 5 consecutive failures.
				// Remove node.  If no nodes are left, seeds will be tried in next cluster
				// tend iteration.
				removeNodes.add(node);
				continue;
			}

			if (cluster.nodes.length > 1 && refreshCount >= 1 && node.referenceCount == 0) {
				// Node is not referenced by other nodes.
				// Check if node responded to info request.
				if (node.failures == 0) {
					// Node is alive, but not referenced by other nodes.  Check if mapped.
					if (! findNodeInPartitionMap(node)) {
						// Node doesn't have any partitions mapped to it.
						// There is no point in keeping it in the cluster.
						removeNodes.add(node);
					}
				}
				else {
					// Node not responding. Remove it.
					removeNodes.add(node);
				}
			}
		}
	}

	private final boolean findNodeInPartitionMap(Node filter) {
		for (Partitions partitions : cluster.partitionMap.values()) {
			for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
				int max = nodeArray.length();

				for (int i = 0; i < max; i++) {
					Node node = nodeArray.get(i);
					// Use reference equality for performance.
					if (node == filter) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private void addNodes(Node seed, Peers peers) {
		// Add all nodes at once to avoid copying entire array multiple times.
		// Create temporary nodes array.
		Node[] nodeArray = new Node[peers.nodes.size() + 1];
		int count = 0;

		// Add seed.
		nodeArray[count++] = seed;
		addNode(seed);

		// Add peers.
		for (Node peer : peers.nodes.values()) {
			nodeArray[count++] = peer;
			addNode(peer);
		}

		// Replace nodes with copy.
		cluster.setNodes(nodeArray);
	}

	/**
	 * Add nodes using copy on write semantics.
	 */
	private void addNodes(HashMap<String,Node> nodesToAdd) {
		// Add all nodes at once to avoid copying entire array multiple times.
		// Create temporary nodes array.
		Node[] nodeArray = new Node[cluster.nodes.length + nodesToAdd.size()];
		int count = 0;

		// Add existing nodes.
		for (Node node : cluster.nodes) {
			nodeArray[count++] = node;
		}

		// Add new nodes.
		for (Node node : nodesToAdd.values()) {
			nodeArray[count++] = node;
			addNode(node);
		}

		// Replace nodes with copy.
		cluster.setNodes(nodeArray);
	}

	private void addNode(Node node) {
		if (Log.infoEnabled()) {
			Log.info("Add node " + node);
		}

		nodesMap.put(node.getName(), node);
	}

	private final void removeNodes(HashSet<Node> nodesToRemove) {
		// There is no need to delete nodes from partitionWriteMap because the nodes
		// have already been set to inactive. Further connection requests will result
		// in an exception and a different node will be tried.

		// Cleanup node resources.
		for (Node node : nodesToRemove) {
			// Remove node from map.
			nodesMap.remove(node.getName());

			// TODO Handle metrics
			/*
			synchronized(metricsLock) {
				if (metricsEnabled) {
					// Flush node metrics before removal.
					try {
						metricsListener.onNodeClose(node);
					}
					catch (Throwable e) {
						Log.warn("Write metrics failed on " + node + ": " + Util.getErrorMessage(e));
					}
				}
			}
			*/
			node.close();
		}

		// Remove all nodes at once to avoid copying entire array multiple times.
		removeNodesCopy(nodesToRemove);
	}

	/**
	 * Remove nodes using copy on write semantics.
	 */
	private final void removeNodesCopy(HashSet<Node> nodesToRemove) {
		// Create temporary nodes array.
		// Since nodes are only marked for deletion using node references in the nodes array,
		// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
		// in nodesToRemove exist.  Therefore, we know the final array size.
		Node[] nodeArray = new Node[cluster.nodes.length - nodesToRemove.size()];
		int count = 0;

		// Add nodes that are not in remove list.
		for (Node node : cluster.nodes) {
			if (nodesToRemove.contains(node)) {
				if (Log.infoEnabled()) {
					Log.info("Remove node " + node);
				}
			}
			else {
				nodeArray[count++] = node;
			}
		}

		// Do sanity check to make sure assumptions are correct.
		if (count < nodeArray.length) {
			if (Log.warnEnabled()) {
				Log.warn("Node remove mismatch. Expected " + nodeArray.length + " Received " + count);
			}
			// Resize array.
			Node[] nodeArray2 = new Node[count];
			System.arraycopy(nodeArray, 0, nodeArray2, 0, count);
			nodeArray = nodeArray2;
		}

		// Replace nodes with copy.
		cluster.setNodes(nodeArray);
	}
}
