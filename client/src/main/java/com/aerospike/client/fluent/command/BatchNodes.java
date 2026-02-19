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
package com.aerospike.client.fluent.command;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.tend.Partition;
import com.aerospike.client.fluent.tend.Partitions;

public final class BatchNodes {

	public interface IBatchStatus {
		public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite);
		public void batchKeyError(AerospikeException ae);
	}

	/**
	 * Assign keys to nodes in initial batch attempt.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchCommand cmd,
		List<? extends BatchRecord> records,
		IBatchStatus status
	) {
		Node[] nodes = cluster.validateNodes();

		// Create initial key capacity for each node as average + 25%.
		int max = records.size();
		int keysPerNode = max / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);
		AerospikeException except = null;

		for (int i = 0; i < max; i++) {
			BatchRecord b = records.get(i);

			if (b.resultCode != ResultCode.NO_RESPONSE) {
				// Record will already have an error if namespace is invalid.
				// Do not send this record to the server.
				continue;
			}

			try {
				b.prepare();

				Node node = b.hasWrite ?
					getNodeBatchWrite(cluster, cmd, b.key, null, 0) :
					getNodeBatchRead(cluster, cmd, b.key, null, 0, 0);

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, i));
				}
				else {
					batchNode.addKey(i);
				}
			}
			catch (AerospikeException ae) {
				// This method only called on initialization, so inDoubt must be false.
				b.setError(ae.getResultCode(), false);

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			// Fatal if no key requests were generated on initialization.
			if (batchNodes.size() == 0) {
				throw except;
			}
			else {
				status.batchKeyError(except);
			}
		}
		return batchNodes;
	}

	/**
	 * Assign keys to nodes in batch node retry.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchCommand cmd,
		List<? extends BatchRecord> records,
		int sequence,
		int sequenceSC,
		BatchNode batchSeed,
		IBatchStatus status
	) {
		Node[] nodes = cluster.validateNodes();

		// Create initial key capacity for each node as average + 25%.
		int keysPerNode = batchSeed.offsetsSize / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);
		AerospikeException except = null;

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];
			BatchRecord b = records.get(offset);

			if (b.resultCode != ResultCode.NO_RESPONSE) {
				// Do not retry keys that already have a response.
				continue;
			}

			try {
				Node node = b.hasWrite ?
					getNodeBatchWrite(cluster, cmd, b.key, batchSeed.node, sequence) :
					getNodeBatchRead(cluster, cmd, b.key, batchSeed.node, sequence, sequenceSC);

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, offset));
				}
				else {
					batchNode.addKey(offset);
				}
			}
			catch (AerospikeException ae) {
				// This method only called on retry, so commandSentCounter(2) will be greater than 1.
				b.setError(ae.getResultCode(), BatchCommand.inDoubt(b.hasWrite, 2));

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			status.batchKeyError(except);
		}
		return batchNodes;
	}

	private static Node getNodeBatchWrite(
		Cluster cluster,
		BatchCommand cmd,
		Key key,
		Node prevNode,
		int sequence
	) {
		Partitions partitions = (cmd.partitions != null)? cmd.partitions :
			cluster.getPartitions(key.namespace);

		Partition p = new Partition(partitions, key, cmd.replica, prevNode, false);
		p.sequence = sequence;
		return p.getNodeWrite(cluster);
	}

	private static Node getNodeBatchRead(
		Cluster cluster,
		BatchCommand cmd,
		Key key,
		Node prevNode,
		int sequence,
		int sequenceSC
	) {
		Partitions partitions = (cmd.partitions != null)? cmd.partitions :
			cluster.getPartitions(key.namespace);

		Replica replica;

		if (partitions.scMode) {
			replica = cmd.replicaSC;
			sequence = sequenceSC;
		}
		else {
			replica = cmd.replica;
		}

		Partition p = new Partition(partitions, key, replica, prevNode, cmd.linearize);
		p.sequence = sequence;
		return p.getNodeRead(cluster);
	}

	private static BatchNode findBatchNode(List<BatchNode> nodes, Node node) {
		for (BatchNode batchNode : nodes) {
			// Note: using pointer equality for performance.
			if (batchNode.node == node) {
				return batchNode;
			}
		}
		return null;
	}
}
