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
package com.aerospike.client.sdk;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.aerospike.client.sdk.tend.NodeValidator;
import com.aerospike.client.sdk.tend.Partitions;
import com.aerospike.client.sdk.util.Version;

/**
 * Builds clusters with hand-made nodes and partition maps, for unit tests of batch node
 * selection that need a specific topology without a live server.
 */
public final class SingleNodeBatchFixture {

    private SingleNodeBatchFixture() {
    }

    /**
     * Return a cluster with no nodes and no running tend thread. Install nodes and partition
     * maps with {@link #install}. The tend thread is stopped so it can't refresh the hand-made
     * nodes, fail to reach them and drop them from the cluster.
     *
     * @param rackIds   the client's preferred racks, in preference order; none for no rack awareness
     */
    public static Cluster cluster(int... rackIds) {
        // Unroutable seed: startup fails fast and the cluster stays empty.
        ClusterDefinition def = new ClusterDefinition(new Host("127.0.0.1", 9))
            .clusterName("single-node-batch-" + UUID.randomUUID())
            .connPoolsPerNode(1)
            .failIfNotConnected(false);
        def.minConnsPerNode = 0;

        if (rackIds.length > 0) {
            def.preferringRacks(rackIds);
        }

        Cluster cluster = new Cluster(def, SystemSettings.DEFAULT);
        cluster.close();
        return cluster;
    }

    /**
     * Return a node that is never connected. It reports itself active.
     */
    public static Node node(Cluster cluster, String name) {
        NodeValidator nv = new NodeValidator();
        nv.name = name;
        nv.primaryHost = new Host("127.0.0.1", 9);
        nv.primaryAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9);
        nv.version = Version.SERVER_VERSION_8_1_3;
        return new Node(cluster, nv);
    }

    /**
     * Return {@code count} nodes named n0, n1, ...
     */
    public static Node[] nodes(Cluster cluster, int count) {
        Node[] nodes = new Node[count];

        for (int i = 0; i < count; i++) {
            nodes[i] = node(cluster, "n" + i);
        }
        return nodes;
    }

    /**
     * Put the node in a rack for the namespace, as the rack info command would.
     */
    public static void setRack(Node node, String namespace, int rackId) {
        Map<String,Integer> racks = new HashMap<String,Integer>();
        Map<String,Integer> current = node.racks;

        if (current != null) {
            racks.putAll(current);
        }
        racks.put(namespace, rackId);
        node.racks = racks;
    }

    /**
     * Install the nodes and per-namespace partition maps on the cluster.
     */
    public static void install(Cluster cluster, Node[] nodes, Map<String,Partitions> partitionMap) {
        cluster.setNodes(nodes);
        cluster.partitionMap = new HashMap<String,Partitions>(partitionMap);
    }
}
