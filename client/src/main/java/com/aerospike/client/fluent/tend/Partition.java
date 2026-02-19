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
package com.aerospike.client.fluent.tend;

import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.command.Buffer;
import com.aerospike.client.fluent.command.PartitionStatus;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Replica;

public final class Partition {
	public static Replica getReplicaSC(Replica replica, ReadModeSC mode) {
		switch (mode) {
		case SESSION:
			return Replica.MASTER;

		case LINEARIZE:
			return replica == Replica.PREFER_RACK ? Replica.SEQUENCE : replica;

		default:
			return replica;
		}
	}

	private Partitions partitions;
	private final String namespace;
	private final Replica replica;
	public Node prevNode;
	private int partitionId;
	public int sequence;
	private final boolean linearize;

	public Partition(Partitions partitions, Key key, Replica replica, Node prevNode, boolean linearize) {
		this.partitions = partitions;
		this.namespace = key.namespace;
		this.replica = replica;
		this.prevNode = prevNode;
		this.linearize = linearize;
		this.partitionId = getPartitionId(key.digest);
	}

	public Partition(String namespace, Replica replica) {
		this.namespace = namespace;
		this.replica = replica;
		this.linearize = false;
	}

	public static int getPartitionId(byte[] digest) {
		// CAN'T USE MOD directly - mod will give negative numbers.
		// First AND makes positive and negative correctly, then mod.
		return (Buffer.littleBytesToInt(digest, 0) & 0xFFFF) % Node.PARTITIONS;
	}

	public Node getNodeQuery(Cluster cluster, Partitions partitions, PartitionStatus ps) {
		this.partitions = partitions;
		this.partitionId = ps.id;
		this.sequence = ps.sequence;
		this.prevNode = ps.node;

		Node node = getNodeRead(cluster);
		ps.node = node;
		ps.sequence = this.sequence;
		ps.retry = false;
		return node;
	}

	public Node getNodeRead(Cluster cluster) {
		switch (replica) {
		default:
		case SEQUENCE:
			return getSequenceNode(cluster);

		case PREFER_RACK:
			return getRackNode(cluster);

		case MASTER:
			return getMasterNode(cluster);

		case MASTER_PROLES:
			return getMasterProlesNode(cluster);

		case RANDOM:
			return cluster.getRandomNode();
		}
	}

	public Node getNodeWrite(Cluster cluster) {
		switch (replica) {
		default:
		case SEQUENCE:
		case PREFER_RACK:
			return getSequenceNode(cluster);

		case MASTER:
		case MASTER_PROLES:
			return getMasterNode(cluster);
		case RANDOM:
			return cluster.getRandomNode();
		}
	}

	public void prepareRetryRead(boolean timeout) {
		if (! timeout || !linearize) {
			sequence++;
		}
	}

	public void prepareRetryWrite(boolean timeout) {
		if (! timeout) {
			sequence++;
		}
	}

	private Node getSequenceNode(Cluster cluster) {
		AtomicReferenceArray<Node>[] replicas = partitions.replicas;
		int max = replicas.length;

		for (int i = 0; i < max; i++) {
			int index = sequence % max;
			Node node = replicas[index].get(partitionId);

			if (node != null && node.isActive()) {
				return node;
			}
			sequence++;
		}
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	private Node getRackNode(Cluster cluster) {
		AtomicReferenceArray<Node>[] replicas = partitions.replicas;
		int max = replicas.length;
		int seq1 = 0;
		int seq2 = 0;
		Node fallback1 = null;
		Node fallback2 = null;
		int[] rackIds = cluster.getClusterDefinition().getRackIds();

		for (int rackId : rackIds) {
			int seq = sequence;

			for (int i = 0; i < max; i++) {
				int index = seq % max;
				Node node = replicas[index].get(partitionId);
				// Log.info("Try " + rackId + ',' + index + ',' + prevNode + ',' + node + ',' + node.hasRack(namespace, rackId));

				if (node != null) {
					// Avoid retrying on node where command failed
					// even if node is the only one on the same rack.
					if (node != prevNode) {
						if (node.hasRack(namespace, rackId)) {
							if (node.isActive()) {
								// Log.info("Found node on same rack: " + node);
								prevNode = node;
								sequence = seq;
								return node;
							}
						}
						else if (fallback1 == null && node.isActive()) {
							// Meets all criteria except not on same rack.
							fallback1 = node;
							seq1 = seq;
						}
					}
					else if (fallback2 == null && node.isActive()){
						// Previous node is the least desirable fallback.
						fallback2 = node;
						seq2 = seq;
					}
				}
				seq++;
			}
		}

		// Return node on a different rack if it exists.
		if (fallback1 != null) {
			// Log.info("Found fallback node: " + fallback1);
			prevNode = fallback1;
			sequence = seq1;
			return fallback1;
		}

		// Return previous node if it still exists.
		if (fallback2 != null) {
			// Log.info("Found previous node: " + fallback2);
			prevNode = fallback2;
			sequence = seq2;
			return fallback2;
		}

		// Failed to find suitable node.
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	private Node getMasterNode(Cluster cluster) {
		Node node = partitions.replicas[0].get(partitionId);

		if (node != null && node.isActive()) {
			return node;
		}
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	private Node getMasterProlesNode(Cluster cluster) {
		AtomicReferenceArray<Node>[] replicas = partitions.replicas;

		for (int i = 0; i < replicas.length; i++) {
			int index = Math.abs(cluster.incrReplicaIndex() % replicas.length);
			Node node = replicas[index].get(partitionId);

			if (node != null && node.isActive()) {
				return node;
			}
		}
		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, this);
	}

	@Override
	public String toString() {
		return namespace + ':' + partitionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + namespace.hashCode();
		result = prime * result + partitionId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		Partition other = (Partition) obj;
		return this.namespace.equals(other.namespace) && this.partitionId == other.partitionId;
	}
}
