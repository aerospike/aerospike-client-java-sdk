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
package com.aerospike.client.sdk.command;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.policy.Replica;
import com.aerospike.client.sdk.tend.Partition;
import com.aerospike.client.sdk.tend.Partitions;

public final class BatchNodes {
    private static final Logger log = LoggerFactory.getLogger(Loggers.COMMAND);

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

        if (cmd.singleNodeReads) {
            if (cmd.replica == Replica.MASTER_PROLES || cmd.replica == Replica.PREFER_RACK) {
                List<BatchNode> single = generateSingleNode(cluster, cmd, records, null,
                    records.size(), null);

                if (single != null) {
                    return single;
                }
            }
            else {
                warnReplicaOrder(cluster, cmd.replica);
            }
        }

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

                b.node = b.hasWrite ?
                    getNodeBatchWrite(cluster, cmd, b.key, null, 0) :
                    getNodeBatchRead(cluster, cmd, b.key, null, 0, 0);

                BatchNode batchNode = findBatchNode(batchNodes, b.node);

                if (batchNode == null) {
                    batchNodes.add(new BatchNode(b.node, keysPerNode, i));
                }
                else {
                    batchNode.addKey(i);
                }
            }
            catch (AerospikeException ae) {
                // This method only called on initialization, so inDoubt must be false.
                b.setError(ae, false);

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

        if (batchSeed.singleNodeTried != null) {
            // Retry of a batch that was sent to a single node. Move to another node that still
            // owns every partition the batch touches. If none is left, split by key below.
            List<BatchNode> single = generateSingleNode(cluster, cmd, records, batchSeed.offsets,
                batchSeed.offsetsSize, batchSeed.singleNodeTried);

            if (single != null) {
                return single;
            }
        }

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
                b.node = b.hasWrite ?
                    getNodeBatchWrite(cluster, cmd, b.key, batchSeed.node, sequence) :
                    getNodeBatchRead(cluster, cmd, b.key, batchSeed.node, sequence, sequenceSC);

                BatchNode batchNode = findBatchNode(batchNodes, b.node);

                if (batchNode == null) {
                    batchNodes.add(new BatchNode(b.node, keysPerNode, offset));
                }
                else {
                    batchNode.addKey(offset);
                }
            }
            catch (AerospikeException ae) {
                // This method only called on retry, so commandSentCounter(2) will be greater than 1.
                b.setError(ae, BatchCommand.inDoubt(b.hasWrite, 2));

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

    /**
     * Send a read-only batch to a single node when one node owns every partition the batch
     * touches. Return null if the batch must be split by key as usual.
     * <p>
     * All of these must hold:
     * <ul>
     * <li>the batch contains no write operations</li>
     * <li>the command is not linearizing, and for every namespace in the batch the effective
     * replica order is MASTER_PROLES or PREFER_RACK</li>
     * <li>a single node owns every partition the batch touches, in every namespace</li>
     * </ul>
     * Ownership is checked against the namespace's full-owner set first. A namespace with no
     * usable full owner falls back to intersecting the owners of each key's partition, which
     * stops as soon as the intersection is empty.
     *
     * @param offsets   record offsets to assign, or null for every record (initial attempt)
     * @param size      number of offsets, or the number of records when offsets is null
     * @param tried     nodes a collapsed batch has already been sent to, excluded from the
     *                  target choice; null on the initial attempt
     */
    private static List<BatchNode> generateSingleNode(
        Cluster cluster,
        BatchCommand cmd,
        List<? extends BatchRecord> records,
        int[] offsets,
        int size,
        List<Node> tried
    ) {
        if (cmd.linearize) {
            return null;
        }

        int count = 0;

        // Namespaces in the batch, mapped to their partition maps. A null value marks a
        // namespace whose ownership is already settled by its full-owner set.
        LinkedHashMap<String,Partitions> namespaces = new LinkedHashMap<String,Partitions>();

        // Namespaces read with PREFER_RACK. The target must be in the client's rack for each.
        List<String> rackNamespaces = new ArrayList<String>(1);

        for (int i = 0; i < size; i++) {
            BatchRecord b = records.get(offset(offsets, i));

            if (b.resultCode != ResultCode.NO_RESPONSE) {
                // Skipped by the split loop as well.
                continue;
            }

            if (b.hasWrite) {
                return null;
            }

            String namespace = b.key.namespace;

            if (! namespaces.containsKey(namespace)) {
                Partitions partitions;

                try {
                    // Same precedence as getNodeBatchRead(): prefer the command's partition map.
                    partitions = (cmd.partitions != null)? cmd.partitions :
                        cluster.getPartitions(namespace);
                }
                catch (AerospikeException ae) {
                    // Let the split loop report the error against the record.
                    return null;
                }

                Replica replica = getReplicaRead(cmd, partitions);

                if (replica == Replica.PREFER_RACK) {
                    rackNamespaces.add(namespace);
                }
                else if (replica != Replica.MASTER_PROLES) {
                    return null;
                }
                namespaces.put(namespace, partitions);
            }
            count++;
        }

        if (count == 0) {
            return null;
        }

        // Nodes that own every partition seen so far. Null means no constraint yet.
        List<Node> candidates = null;
        boolean scanKeys = false;

        // Tier 1: full-owner sets.
        for (Map.Entry<String,Partitions> entry : namespaces.entrySet()) {
            List<Node> owners = activeNodes(entry.getValue().getFullOwners());
            List<Node> next = (candidates == null)? owners : intersect(candidates, owners);

            if (next.isEmpty()) {
                // No full owner that also suits the other namespaces. Check this namespace's
                // keys individually.
                scanKeys = true;
            }
            else {
                candidates = next;
                entry.setValue(null);
            }
        }

        // Tier 2: intersect the owners of each key's partition.
        if (scanKeys) {
            for (int i = 0; i < size; i++) {
                BatchRecord b = records.get(offset(offsets, i));

                if (b.resultCode != ResultCode.NO_RESPONSE) {
                    continue;
                }

                Partitions partitions = namespaces.get(b.key.namespace);

                if (partitions == null) {
                    // Settled by tier 1.
                    continue;
                }

                int partitionId = Partition.getPartitionId(b.key.digest);

                if (candidates == null) {
                    candidates = partitionOwners(partitions, partitionId);
                }
                else {
                    candidates.removeIf(node -> ! ownsPartition(partitions, partitionId, node));
                }

                if (candidates.isEmpty()) {
                    return null;
                }
            }
        }

        Node target = selectTarget(cluster, candidates, rackNamespaces, tried);

        if (target == null) {
            return null;
        }

        BatchNode batchNode = null;

        for (int i = 0; i < size; i++) {
            BatchRecord b = records.get(offset(offsets, i));

            if (b.resultCode != ResultCode.NO_RESPONSE) {
                continue;
            }

            if (offsets == null) {
                // Initial attempt: same as the split loop.
                b.prepare();
            }
            b.node = target;

            int offset = offset(offsets, i);

            if (batchNode == null) {
                batchNode = new BatchNode(target, count, offset);
            }
            else {
                batchNode.addKey(offset);
            }
        }

        // Remember every node this batch has been sent to, so a retry moves to another one.
        List<Node> sentTo = new ArrayList<Node>((tried == null)? 1 : tried.size() + 1);

        if (tried != null) {
            sentTo.addAll(tried);
        }
        sentTo.add(target);
        batchNode.singleNodeTried = sentTo;

        List<BatchNode> batchNodes = new ArrayList<BatchNode>(1);
        batchNodes.add(batchNode);
        return batchNodes;
    }

    private static int offset(int[] offsets, int i) {
        return (offsets == null)? i : offsets[i];
    }

    /**
     * Warn once per cluster that allowSingleNodeBatchReads is enabled with a replica order that
     * can never pass the gate. For SC namespaces the replica order can only narrow (to MASTER or
     * SEQUENCE), so these replica orders fail for every namespace.
     */
    private static void warnReplicaOrder(Cluster cluster, Replica replica) {
        if (cluster.firstSingleNodeBatchReadsWarning() && log.isWarnEnabled()) {
            log.atWarn()
                .addKeyValue(Cluster.CONTEXT, cluster.getClusterDefinition().getClusterName())
                .log("allowSingleNodeBatchReads is enabled but replicaOrder is " + replica +
                    ", so read-only batches are still split by key. The setting only takes" +
                    " effect when replicaOrder is " + Replica.MASTER_PROLES + " or " +
                    Replica.PREFER_RACK + ".");
        }
    }

    /**
     * Return the replica order getNodeBatchRead() uses for this partition map.
     */
    private static Replica getReplicaRead(BatchCommand cmd, Partitions partitions) {
        return partitions.scMode ? cmd.replicaSC : cmd.replica;
    }

    /**
     * Choose the node that receives a single-node batch, or return null if the batch must be
     * split. Successive batches rotate across the eligible candidates using the cluster's
     * replica rotation counter.
     * <p>
     * For each namespace read with PREFER_RACK, only candidates in the client's rack for that
     * namespace are eligible. If none is, the batch is split rather than sent to another rack.
     * Nodes in {@code tried} are never chosen, so a retry always moves to a different node.
     */
    private static Node selectTarget(
        Cluster cluster,
        List<Node> candidates,
        List<String> rackNamespaces,
        List<Node> tried
    ) {
        List<Node> eligible = candidates;

        if (! rackNamespaces.isEmpty()) {
            int[] rackIds = cluster.getClusterDefinition().getRackIds();

            if (rackIds == null || rackIds.length == 0) {
                // Nothing to anchor the rack constraint to. Let the split path handle it.
                return null;
            }

            Node[] nodes = cluster.getNodes();

            for (String namespace : rackNamespaces) {
                Integer rackId = getClientRack(nodes, namespace, rackIds);

                if (rackId == null) {
                    // No node is in any of the client's racks for this namespace, so a split
                    // batch would also read from other racks. No constraint to apply.
                    continue;
                }

                List<Node> inRack = new ArrayList<Node>(eligible.size());

                for (Node node : eligible) {
                    if (node.hasRack(namespace, rackId)) {
                        inRack.add(node);
                    }
                }

                if (inRack.isEmpty()) {
                    return null;
                }
                eligible = inRack;
            }
        }

        if (tried != null) {
            List<Node> untried = new ArrayList<Node>(eligible.size());

            for (Node node : eligible) {
                if (! tried.contains(node)) {
                    untried.add(node);
                }
            }
            eligible = untried;
        }

        if (eligible.isEmpty()) {
            return null;
        }

        // Rotate over a stable order so successive batches spread across the candidates.
        List<Node> sorted = new ArrayList<Node>(eligible);
        sorted.sort(Comparator.comparing(Node::getName));
        int index = Math.abs(cluster.incrReplicaIndex() % sorted.size());
        return sorted.get(index);
    }

    /**
     * Return the client's rack for the namespace: the first of the client's rack ids, in
     * preference order, that holds an active node for the namespace. Return null if none does.
     * <p>
     * This matches the rack a split PREFER_RACK batch reads from first, so a collapsed batch
     * never moves to a less-preferred rack while a more-preferred one serves the namespace.
     */
    private static Integer getClientRack(Node[] nodes, String namespace, int[] rackIds) {
        for (int rackId : rackIds) {
            for (Node node : nodes) {
                if (node.isActive() && node.hasRack(namespace, rackId)) {
                    return rackId;
                }
            }
        }
        return null;
    }

    private static List<Node> activeNodes(Iterable<Node> nodes) {
        List<Node> list = new ArrayList<Node>();

        for (Node node : nodes) {
            if (node.isActive()) {
                list.add(node);
            }
        }
        return list;
    }

    private static List<Node> intersect(List<Node> a, List<Node> b) {
        List<Node> list = new ArrayList<Node>(a);
        list.retainAll(b);
        return list;
    }

    /**
     * Return the active nodes that own the partition at any replica level.
     */
    private static List<Node> partitionOwners(Partitions partitions, int partitionId) {
        List<Node> list = new ArrayList<Node>(partitions.replicas.length);

        for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
            Node node = nodeArray.get(partitionId);

            if (node != null && node.isActive() && ! list.contains(node)) {
                list.add(node);
            }
        }
        return list;
    }

    private static boolean ownsPartition(Partitions partitions, int partitionId, Node node) {
        for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
            // Note: using pointer equality for performance.
            if (nodeArray.get(partitionId) == node) {
                return true;
            }
        }
        return false;
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
