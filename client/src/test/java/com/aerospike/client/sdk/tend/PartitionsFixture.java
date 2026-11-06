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
package com.aerospike.client.sdk.tend;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntFunction;

import com.aerospike.client.sdk.Node;

/**
 * Builds partition maps for unit tests. The full-owner set is filled from the replica arrays
 * by counting, the same rule {@link PartitionParser} applies to each node's response.
 * {@code PartitionParserFullOwnerTest} covers the parser itself.
 */
public final class PartitionsFixture {
    public static final int PARTITIONS = Node.PARTITIONS;

    private PartitionsFixture() {
    }

    /**
     * Replica {@code i} of partition {@code p} is {@code nodes[(p + i) % nodes.length]}. With a
     * replica count equal to the node count, every node owns every partition. With fewer, no
     * node does.
     */
    public static Partitions roundRobin(Node[] nodes, int replicaCount, boolean scMode) {
        return build(replicaCount, scMode, p -> {
            Node[] owners = new Node[replicaCount];

            for (int i = 0; i < replicaCount; i++) {
                owners[i] = nodes[(p + i) % nodes.length];
            }
            return owners;
        });
    }

    /**
     * Build a partition map from the owners of each partition, master first.
     */
    public static Partitions build(int replicaCount, boolean scMode, IntFunction<Node[]> owners) {
        Partitions partitions = new Partitions(PARTITIONS, replicaCount, scMode);

        for (int p = 0; p < PARTITIONS; p++) {
            Node[] o = owners.apply(p);

            for (int i = 0; i < replicaCount; i++) {
                partitions.replicas[i].set(p, o[i]);
            }
        }
        refreshFullOwners(partitions);
        return partitions;
    }

    /**
     * Recompute the full-owner set from the replica arrays.
     */
    public static void refreshFullOwners(Partitions partitions) {
        Map<Node,Integer> counts = new HashMap<Node,Integer>();

        for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
            for (int p = 0; p < nodeArray.length(); p++) {
                Node node = nodeArray.get(p);

                if (node != null) {
                    counts.merge(node, 1, Integer::sum);
                }
            }
        }

        for (Map.Entry<Node,Integer> entry : counts.entrySet()) {
            partitions.setFullOwner(entry.getKey(), entry.getValue() == PARTITIONS);
        }
    }
}
