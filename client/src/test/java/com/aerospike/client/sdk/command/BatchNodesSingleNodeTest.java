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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.SingleNodeBatchFixture;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ReadModeSC;
import com.aerospike.client.sdk.policy.Replica;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partition;
import com.aerospike.client.sdk.tend.Partitions;
import com.aerospike.client.sdk.tend.PartitionsFixture;

/**
 * allowSingleNodeBatchReads: how {@link BatchNodes} assigns a batch to nodes across cluster
 * topologies built by hand, so each case is deterministic and needs no server.
 * {@code SingleNodeBatchReadIntegrationTest} covers a live cluster.
 */
public class BatchNodesSingleNodeTest {
    private static final String NS = "test";
    private static final int RACK_A = 1;
    private static final int RACK_B = 2;
    private static final String WARNING = "allowSingleNodeBatchReads is enabled";

    private static int behaviorCount;

    private static final BatchNodes.IBatchStatus STATUS = new BatchNodes.IBatchStatus() {
        @Override
        public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite) {
            throw new AssertionError("Unexpected key error", ae);
        }

        @Override
        public void batchKeyError(AerospikeException ae) {
            throw new AssertionError("Unexpected batch error", ae);
        }
    };

    private record Topology(String name, Cluster cluster, Node[] nodes, Partitions partitions,
        boolean rackAware) {
    }

    @AfterEach
    void restoreRegistry() {
        Behavior.restoreBehaviorRegistry();
    }

    //-------------------------------------------------------
    // Default off
    //-------------------------------------------------------

    /**
     * Regression guard: with the setting left unset, every topology splits exactly as the
     * per-key node lookup does, in the same order.
     */
    @Test
    void defaultOffSplitIsUnchanged() {
        ResolvedSettings sequence = settings(null, null);
        ResolvedSettings preferRack = settings(Replica.PREFER_RACK, null);
        ResolvedSettings masterProles = settings(Replica.MASTER_PROLES, null);

        assertFalse(sequence.getAllowSingleNodeBatchReads());
        assertFalse(masterProles.getAllowSingleNodeBatchReads());

        for (Topology t : allTopologies()) {
            assertMatchesPerKey(t, spread(NS, 40), sequence, Replica.SEQUENCE);

            if (t.rackAware()) {
                assertMatchesPerKey(t, spread(NS, 40), preferRack, Replica.PREFER_RACK);
            }

            // MASTER_PROLES rotates per key, so check the split path ran rather than exact nodes.
            List<BatchRecord> records = spread(NS, 40);
            List<BatchNode> batchNodes = generate(t.cluster(), null, records, masterProles);
            assertNotCollapsed(batchNodes, t.name());
            assertOffsetsCovered(batchNodes, records, t.name());

            assertTrue(t.cluster().firstSingleNodeBatchReadsWarning(),
                t.name() + ": no warning while the setting is unset");
        }
    }

    //-------------------------------------------------------
    // Full owners: 2-node RF 2, 3-node RF 3, 4-node RF 4
    //-------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4})
    void fullOwnerTopologyCollapsesAndRotates(int nodeCount) {
        Topology t = roundRobin(nodeCount, nodeCount);
        assertEquals(Set.of(t.nodes()), t.partitions().getFullOwners());

        ResolvedSettings settings = settings(Replica.MASTER_PROLES, true);
        Set<Node> targets = new HashSet<Node>();

        for (int i = 0; i < nodeCount; i++) {
            List<BatchRecord> records = spread(NS, 40);
            targets.add(assertCollapsed(generate(t.cluster(), null, records, settings), records).node);
        }
        assertEquals(Set.of(t.nodes()), targets, "successive batches rotate across every full owner");
    }

    //-------------------------------------------------------
    // Default replica order
    //-------------------------------------------------------

    @Test
    void defaultReplicaOrderSplitsAndWarnsOnce() {
        Topology t = roundRobin(3, 3);
        ResolvedSettings settings = settings(null, true);
        assertEquals(Replica.SEQUENCE, settings.getReplicaOrder());
        assertTrue(settings.getAllowSingleNodeBatchReads());

        String stderr = captureStderr(() -> {
            assertMatchesPerKey(t, spread(NS, 40), settings, Replica.SEQUENCE);
            assertMatchesPerKey(t, spread(NS, 40), settings, Replica.SEQUENCE);
            assertMatchesPerKey(t, spread(NS, 40), settings, Replica.SEQUENCE);
        });

        assertEquals(1, occurrences(stderr, WARNING), "warning logged exactly once:\n" + stderr);
        assertTrue(stderr.contains("MASTER_PROLES") && stderr.contains("PREFER_RACK"),
            "warning names the replica orders that work:\n" + stderr);
        assertFalse(t.cluster().firstSingleNodeBatchReadsWarning(), "warning already used");

        // A replica order that can pass the gate never warns.
        Topology other = roundRobin(3, 3);
        List<BatchRecord> records = spread(NS, 40);
        assertCollapsed(generate(other.cluster(), null, records, settings(Replica.MASTER_PROLES, true)), records);
        assertTrue(other.cluster().firstSingleNodeBatchReadsWarning());
    }

    //-------------------------------------------------------
    // Racks
    //-------------------------------------------------------

    @Test
    void preferRackCollapsesOntoFullOwnerInClientRack() {
        Topology t = rackForced(RACK_A);
        assertEquals(Set.of(t.nodes()[0]), t.partitions().getFullOwners());

        ResolvedSettings settings = settings(Replica.PREFER_RACK, true);

        for (int i = 0; i < 3; i++) {
            List<BatchRecord> records = spread(NS, 40);
            assertSame(t.nodes()[0], assertCollapsed(generate(t.cluster(), null, records, settings), records).node);
        }
    }

    @Test
    void preferRackSplitsRatherThanLeaveRack() {
        Topology t = rackForced(RACK_B);
        ResolvedSettings settings = settings(Replica.PREFER_RACK, true);

        List<BatchNode> batchNodes = generate(t.cluster(), null, spread(NS, 40), settings);
        assertNotCollapsed(batchNodes, t.name());

        for (BatchNode batchNode : batchNodes) {
            assertNotSame(t.nodes()[0], batchNode.node, "never crosses to rack A");
            assertTrue(batchNode.node.hasRack(NS, RACK_B));
        }

        // And it's the ordinary per-key split.
        assertMatchesPerKey(t, spread(NS, 40), settings, Replica.PREFER_RACK);
    }

    /**
     * Each rack holds a full copy, but no single node does. The batch must not collapse onto
     * "the rack" and stays split across the client rack's two nodes.
     */
    @Test
    void noFullOwnerStaysSplitWithinRack() {
        Topology t = threeRacks(1);
        assertTrue(t.partitions().getFullOwners().isEmpty());

        List<BatchRecord> records = spread(NS, 40);
        List<BatchNode> batchNodes = generate(t.cluster(), null, records, settings(Replica.PREFER_RACK, true));

        assertNotCollapsed(batchNodes, t.name());
        assertEquals(Set.of(t.nodes()[0], t.nodes()[1]), nodesOf(batchNodes), "confined to rack 1");
        assertOffsetsCovered(batchNodes, records, t.name());
    }

    //-------------------------------------------------------
    // Opportunistic tier: 5-node RF 2
    //-------------------------------------------------------

    @Test
    void opportunisticTierCollapsesOnlyWhenPartitionsShareOwner() {
        Topology t = roundRobin(5, 2);
        assertTrue(t.partitions().getFullOwners().isEmpty());

        ResolvedSettings settings = settings(Replica.MASTER_PROLES, true);

        // Partitions 0, 1 and 5 are owned by {n0,n1}, {n1,n2} and {n0,n1}. n1 owns all three.
        List<BatchRecord> shared = records(NS, 0, 1, 5);
        assertSame(t.nodes()[1], assertCollapsed(generate(t.cluster(), null, shared, settings), shared).node);

        // Partitions 0, 2 and 4 are owned by {n0,n1}, {n2,n3} and {n4,n0}. No common owner.
        List<BatchRecord> disjoint = records(NS, 0, 2, 4);
        List<BatchNode> batchNodes = generate(t.cluster(), null, disjoint, settings);
        assertTrue(batchNodes.size() > 1);
        assertNotCollapsed(batchNodes, t.name());
    }

    //-------------------------------------------------------
    // Strong consistency
    //-------------------------------------------------------

    @ParameterizedTest
    @EnumSource(ReadModeSC.class)
    void scNamespaceReadModes(ReadModeSC mode) {
        Topology t = roundRobinSc(3, 3);
        ResolvedSettings settings = settings(Replica.MASTER_PROLES, true, mode);

        // As ReadAttr sets it for an SC namespace.
        boolean linearize = mode == ReadModeSC.LINEARIZE;

        List<BatchRecord> records = spread(NS, 40);
        List<BatchNode> batchNodes = generate(t.cluster(), null, records, settings, linearize);

        if (mode == ReadModeSC.ALLOW_REPLICA || mode == ReadModeSC.ALLOW_UNAVAILABLE) {
            assertCollapsed(batchNodes, records);
        }
        else {
            // SESSION reads the master only. LINEARIZE linearizes.
            assertNotCollapsed(batchNodes, mode.toString());
        }
    }

    @ParameterizedTest
    @EnumSource(ReadModeSC.class)
    void linearizingCommandNeverCollapses(ReadModeSC mode) {
        Topology sc = roundRobinSc(3, 3);
        assertNotCollapsed(generate(sc.cluster(), null, spread(NS, 40),
            settings(Replica.MASTER_PROLES, true, mode), true), "SC " + mode);

        Topology ap = roundRobin(3, 3);
        assertNotCollapsed(generate(ap.cluster(), null, spread(NS, 40),
            settings(Replica.MASTER_PROLES, true), true), "AP");
    }

    //-------------------------------------------------------
    // Pre-resolved partitions
    //-------------------------------------------------------

    @Test
    void preResolvedPartitionsAreUsedForTheGate() {
        // The cluster's own map has no full owner. The command's map is owned by n2 and n3.
        Topology t = roundRobin(4, 2);
        Node[] nodes = t.nodes();
        Partitions pinned = PartitionsFixture.build(2, false, p -> new Node[] {nodes[2], nodes[3]});
        ResolvedSettings settings = settings(Replica.MASTER_PROLES, true);

        List<BatchRecord> records = spread(NS, 40);
        Node target = assertCollapsed(generate(t.cluster(), pinned, records, settings), records).node;
        assertTrue(target == nodes[2] || target == nodes[3], "target comes from the command's map");

        assertNotCollapsed(generate(t.cluster(), null, spread(NS, 40), settings), "cluster map");

        // The reverse: the cluster's map has full owners, the command's map does not.
        Topology full = roundRobin(4, 4);
        Partitions split = PartitionsFixture.roundRobin(full.nodes(), 2, false);
        assertNotCollapsed(generate(full.cluster(), split, spread(NS, 40), settings), "command map");
    }

    //-------------------------------------------------------
    // Multiple namespaces
    //-------------------------------------------------------

    @Test
    void multiNamespaceBatchSplitsWhenOneNamespaceFails() {
        Cluster cluster = SingleNodeBatchFixture.cluster();
        Node[] nodes = SingleNodeBatchFixture.nodes(cluster, 4);
        Partitions a = PartitionsFixture.roundRobin(nodes, 4, false);
        Partitions b = PartitionsFixture.roundRobin(nodes, 2, false);
        Partitions c = PartitionsFixture.roundRobin(nodes, 4, true);
        SingleNodeBatchFixture.install(cluster, nodes, Map.of("a", a, "b", b, "c", c));

        ResolvedSettings settings = settings(Replica.MASTER_PROLES, true);

        // Ownership: b's partitions 0 and 2 are owned by {n0,n1} and {n2,n3}.
        assertNotCollapsed(generate(cluster, null, concat(spread("a", 20), records("b", 0, 2)), settings),
            "b fails ownership");

        // Replica order: c is SC and reads with SESSION, so only its master may serve it.
        assertNotCollapsed(generate(cluster, null, concat(spread("a", 20), spread("c", 5)), settings),
            "c fails replica order");

        // Control: b's partitions 0 and 4 are both owned by {n0,n1}, so every key fits there.
        List<BatchRecord> passing = concat(spread("a", 20), records("b", 0, 4));
        Node target = assertCollapsed(generate(cluster, null, passing, settings), passing).node;
        assertTrue(target == nodes[0] || target == nodes[1]);
    }

    //-------------------------------------------------------
    // Writes
    //-------------------------------------------------------

    @Test
    void batchWithWriteSplits() {
        Topology t = roundRobin(3, 3);
        List<BatchRecord> records = spread(NS, 40);
        records.set(7, new BatchRecord(key(NS, 5, 1000), true));

        List<BatchNode> batchNodes = generate(t.cluster(), null, records, settings(Replica.MASTER_PROLES, true));
        assertTrue(batchNodes.size() > 1);
        assertNotCollapsed(batchNodes, t.name());
    }

    //-------------------------------------------------------
    // Retry
    //-------------------------------------------------------

    @Test
    void retryMovesToDifferentNodeThenSplits() {
        Topology t = roundRobin(3, 3);
        ResolvedSettings settings = settings(Replica.MASTER_PROLES, true);
        List<BatchRecord> records = spread(NS, 40);
        BatchCommand cmd = command(t.cluster(), null, records, settings, false);

        BatchNode seed = assertCollapsed(BatchNodes.generate(t.cluster(), cmd, records, STATUS), records);
        List<Node> sentTo = new ArrayList<Node>(List.of(seed.node));

        for (int attempt = 1; attempt < t.nodes().length; attempt++) {
            List<BatchNode> retry = BatchNodes.generate(t.cluster(), cmd, records, attempt, attempt, seed, STATUS);
            BatchNode next = assertCollapsed(retry, records);

            assertFalse(sentTo.contains(next.node), "retry " + attempt + " moves to a node not yet tried");
            sentTo.add(next.node);
            assertEquals(sentTo, next.singleNodeTried);
            seed = next;
        }
        assertEquals(Set.of(t.nodes()), new HashSet<Node>(sentTo));

        // Every full owner has been tried: the next retry splits by key.
        List<BatchNode> split = BatchNodes.generate(t.cluster(), cmd, records, 3, 3, seed, STATUS);
        assertNotCollapsed(split, "after exhausting candidates");
        assertOffsetsCovered(split, records, "after exhausting candidates");
    }

    //-------------------------------------------------------
    // Topologies
    //-------------------------------------------------------

    private static List<Topology> allTopologies() {
        return List.of(
            roundRobin(2, 2),
            roundRobin(3, 3),
            roundRobin(4, 4),
            roundRobin(5, 2),
            rackForced(RACK_A),
            rackForced(RACK_B),
            threeRacks(1));
    }

    private static Topology roundRobin(int nodeCount, int replicaCount) {
        return roundRobin(nodeCount, replicaCount, false);
    }

    private static Topology roundRobinSc(int nodeCount, int replicaCount) {
        return roundRobin(nodeCount, replicaCount, true);
    }

    private static Topology roundRobin(int nodeCount, int replicaCount, boolean scMode) {
        Cluster cluster = SingleNodeBatchFixture.cluster();
        Node[] nodes = SingleNodeBatchFixture.nodes(cluster, nodeCount);
        Partitions partitions = PartitionsFixture.roundRobin(nodes, replicaCount, scMode);
        SingleNodeBatchFixture.install(cluster, nodes, Map.of(NS, partitions));
        return new Topology(nodeCount + "-node RF " + replicaCount + (scMode ? " SC" : ""),
            cluster, nodes, partitions, false);
    }

    /**
     * 4 nodes, RF 2, rack A = {n0}, rack B = {n1, n2, n3}. Each partition has one copy in each
     * rack, so n0 owns every partition and each rack B node owns a third.
     */
    private static Topology rackForced(int clientRack) {
        Cluster cluster = SingleNodeBatchFixture.cluster(clientRack);
        Node[] nodes = SingleNodeBatchFixture.nodes(cluster, 4);
        SingleNodeBatchFixture.setRack(nodes[0], NS, RACK_A);

        for (int i = 1; i < 4; i++) {
            SingleNodeBatchFixture.setRack(nodes[i], NS, RACK_B);
        }

        Partitions partitions = PartitionsFixture.build(2, false, p -> {
            Node inB = nodes[1 + p % 3];
            return (p % 2 == 0) ? new Node[] {nodes[0], inB} : new Node[] {inB, nodes[0]};
        });
        SingleNodeBatchFixture.install(cluster, nodes, Map.of(NS, partitions));
        return new Topology("4-node RF 2, client in rack " + clientRack, cluster, nodes, partitions, true);
    }

    /**
     * 6 nodes in 3 racks of 2, RF 3. Each partition has one copy per rack, and each rack's two
     * nodes split the partitions in half, so no node owns every partition. Rack r holds
     * nodes n(2r-2) and n(2r-1).
     */
    private static Topology threeRacks(int clientRack) {
        Cluster cluster = SingleNodeBatchFixture.cluster(clientRack);
        Node[] nodes = SingleNodeBatchFixture.nodes(cluster, 6);

        for (int i = 0; i < 6; i++) {
            SingleNodeBatchFixture.setRack(nodes[i], NS, 1 + i / 2);
        }

        Partitions partitions = PartitionsFixture.build(3, false, p -> {
            Node[] owners = new Node[3];

            for (int i = 0; i < 3; i++) {
                // Rotate the master across racks; pick the rack's node by partition parity.
                int rack = (p + i) % 3;
                owners[i] = nodes[2 * rack + (p % 2)];
            }
            return owners;
        });
        SingleNodeBatchFixture.install(cluster, nodes, Map.of(NS, partitions));
        return new Topology("6-node RF 3 in 3 racks, client in rack " + clientRack, cluster, nodes,
            partitions, true);
    }

    //-------------------------------------------------------
    // Batches
    //-------------------------------------------------------

    private static List<BatchNode> generate(Cluster cluster, Partitions pinned, List<BatchRecord> records,
        ResolvedSettings settings) {
        return generate(cluster, pinned, records, settings, false);
    }

    private static List<BatchNode> generate(Cluster cluster, Partitions pinned, List<BatchRecord> records,
        ResolvedSettings settings, boolean linearize) {
        BatchCommand cmd = command(cluster, pinned, records, settings, linearize);
        return BatchNodes.generate(cluster, cmd, records, STATUS);
    }

    private static BatchCommand command(Cluster cluster, Partitions pinned, List<BatchRecord> records,
        ResolvedSettings settings, boolean linearize) {
        return new BatchCommand(cluster, pinned, null, NS, records, null, false, false, linearize, false,
            settings);
    }

    /**
     * Read records whose keys hash to partitions spread across the map, alternating parity.
     */
    private static List<BatchRecord> spread(String namespace, int count) {
        List<BatchRecord> records = new ArrayList<BatchRecord>(count);

        for (int i = 0; i < count; i++) {
            records.add(new BatchRecord(key(namespace, (i * 97) % PartitionsFixture.PARTITIONS, i), false));
        }
        return records;
    }

    /**
     * Read records, one per given partition id.
     */
    private static List<BatchRecord> records(String namespace, int... partitionIds) {
        List<BatchRecord> records = new ArrayList<BatchRecord>(partitionIds.length);

        for (int i = 0; i < partitionIds.length; i++) {
            records.add(new BatchRecord(key(namespace, partitionIds[i], i), false));
        }
        return records;
    }

    private static List<BatchRecord> concat(List<BatchRecord> first, List<BatchRecord> second) {
        List<BatchRecord> records = new ArrayList<BatchRecord>(first);
        records.addAll(second);
        return records;
    }

    /**
     * Key whose digest maps to the given partition. Partition.getPartitionId() reads the
     * first two digest bytes, little-endian.
     */
    private static Key key(String namespace, int partitionId, int id) {
        byte[] digest = new byte[20];
        digest[0] = (byte)partitionId;
        digest[1] = (byte)(partitionId >>> 8);
        digest[4] = (byte)id;
        digest[5] = (byte)(id >>> 8);
        Key key = new Key(namespace, digest, "set", null);
        assertEquals(partitionId, Partition.getPartitionId(key.digest));
        return key;
    }

    //-------------------------------------------------------
    // Settings
    //-------------------------------------------------------

    private static ResolvedSettings settings(Replica replica, Boolean singleNode) {
        return resolve(replica, singleNode, null, Mode.AP);
    }

    private static ResolvedSettings settings(Replica replica, Boolean singleNode, ReadModeSC consistency) {
        return resolve(replica, singleNode, consistency, Mode.CP);
    }

    private static ResolvedSettings resolve(Replica replica, Boolean singleNode, ReadModeSC consistency,
        Mode mode) {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("single-node-batch-" + (++behaviorCount),
            builder -> {
                builder.on(Selectors.reads().batch(), ops -> {
                    if (replica != null) {
                        ops.replicaOrder(replica);
                    }
                    if (singleNode != null) {
                        ops.allowSingleNodeBatchReads(singleNode);
                    }
                });

                if (consistency != null) {
                    builder.on(Selectors.reads().batch().cp(), ops -> ops.consistency(consistency));
                }
            });
        return behavior.getSettings(OpKind.READ, OpShape.BATCH, mode);
    }

    //-------------------------------------------------------
    // Assertions
    //-------------------------------------------------------

    private static BatchNode assertCollapsed(List<BatchNode> batchNodes, List<BatchRecord> records) {
        assertEquals(1, batchNodes.size(), "one batch node");
        BatchNode batchNode = batchNodes.get(0);

        assertEquals(records.size(), batchNode.offsetsSize, "every key on the one node");
        assertNotNull(batchNode.singleNodeTried, "marked as a single-node batch");

        for (BatchRecord record : records) {
            assertSame(batchNode.node, record.node);
        }
        return batchNode;
    }

    private static void assertNotCollapsed(List<BatchNode> batchNodes, String context) {
        for (BatchNode batchNode : batchNodes) {
            assertNull(batchNode.singleNodeTried, context + ": split by key");
        }
    }

    private static void assertOffsetsCovered(List<BatchNode> batchNodes, List<BatchRecord> records, String context) {
        Set<Integer> offsets = new HashSet<Integer>();

        for (BatchNode batchNode : batchNodes) {
            for (int i = 0; i < batchNode.offsetsSize; i++) {
                assertTrue(offsets.add(batchNode.offsets[i]), context + ": offset assigned once");
            }
        }
        assertEquals(records.size(), offsets.size(), context + ": every key assigned");
    }

    /**
     * The batch nodes must be exactly what the per-key lookup produces, grouped in the same
     * order with the same offsets.
     */
    private static void assertMatchesPerKey(Topology t, List<BatchRecord> records, ResolvedSettings settings,
        Replica replica) {
        LinkedHashMap<Node,List<Integer>> expected = new LinkedHashMap<Node,List<Integer>>();

        for (int i = 0; i < records.size(); i++) {
            Partition partition = new Partition(t.partitions(), records.get(i).key, replica, null, false);
            Node node = partition.getNodeRead(t.cluster());
            expected.computeIfAbsent(node, n -> new ArrayList<Integer>()).add(i);
        }

        List<BatchNode> batchNodes = generate(t.cluster(), null, records, settings);
        LinkedHashMap<Node,List<Integer>> actual = new LinkedHashMap<Node,List<Integer>>();

        for (BatchNode batchNode : batchNodes) {
            List<Integer> offsets = new ArrayList<Integer>();

            for (int i = 0; i < batchNode.offsetsSize; i++) {
                offsets.add(batchNode.offsets[i]);
            }
            actual.put(batchNode.node, offsets);
        }

        String context = t.name() + " " + replica;
        assertEquals(new ArrayList<Node>(expected.keySet()), new ArrayList<Node>(actual.keySet()), context);
        assertEquals(expected, actual, context);
        assertNotCollapsed(batchNodes, context);
    }

    private static Set<Node> nodesOf(List<BatchNode> batchNodes) {
        Set<Node> nodes = new HashSet<Node>();

        for (BatchNode batchNode : batchNodes) {
            nodes.add(batchNode.node);
        }
        return nodes;
    }

    private static String captureStderr(Runnable action) {
        PrintStream original = System.err;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setErr(new PrintStream(bytes, true, StandardCharsets.UTF_8));

        try {
            action.run();
        }
        finally {
            System.setErr(original);
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private static int occurrences(String text, String fragment) {
        int count = 0;
        int index = text.indexOf(fragment);

        while (index >= 0) {
            count++;
            index = text.indexOf(fragment, index + fragment.length());
        }
        return count;
    }
}
