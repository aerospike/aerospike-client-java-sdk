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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.Node;

public final class Partitions {
    private static final Logger log = LoggerFactory.getLogger(Loggers.TEND);

    public final AtomicReferenceArray<Node>[] replicas;
    final int[] regimes;
    public final boolean scMode;

    // Nodes that own every partition in this namespace, at some replica level. Only the tend
    // thread writes this field, and it always replaces the set rather than modifying it, so
    // command threads can read it without locking.
    private volatile Set<Node> fullOwners = Set.of();

    @SuppressWarnings("unchecked")
    public Partitions(int partitionCount, int replicaCount, boolean scMode) {
        this.replicas = new AtomicReferenceArray[replicaCount];

        for (int i = 0; i < replicaCount; i++) {
            this.replicas[i] = new AtomicReferenceArray<Node>(partitionCount);
        }
        this.regimes = new int[partitionCount];
        this.scMode = scMode;
    }

    /**
     * Copy partition map while reserving space for a new replica count.
     */
    @SuppressWarnings("unchecked")
    public Partitions(Partitions other, int replicaCount) {
        this.replicas = new AtomicReferenceArray[replicaCount];

        if (other.replicas.length < replicaCount) {
            int i = 0;

            // Copy existing entries.
            for (; i < other.replicas.length; i++) {
                this.replicas[i] = other.replicas[i];
            }

            // Create new entries.
            for (; i < replicaCount; i++) {
                this.replicas[i] = new AtomicReferenceArray<Node>(other.regimes.length);
            }
        }
        else {
            // Copy existing entries.
            for (int i = 0; i < replicaCount; i++) {
                this.replicas[i] = other.replicas[i];
            }
        }
        this.regimes = other.regimes;
        this.scMode = other.scMode;
        this.fullOwners = other.fullOwners;
    }

    /**
     * Return true if the node owns every partition in this namespace, at some replica level.
     */
    public boolean isFullOwner(Node node) {
        return node != null && fullOwners.contains(node);
    }

    /**
     * Return the nodes that own every partition in this namespace. The returned set is an
     * immutable snapshot.
     */
    public Set<Node> getFullOwners() {
        return fullOwners;
    }

    /**
     * Add the node to the full-owner set, or remove it. Called from the tend thread only.
     */
    void setFullOwner(Node node, boolean fullOwner) {
        Set<Node> current = fullOwners;

        if (current.contains(node) == fullOwner) {
            return;
        }

        HashSet<Node> next = new HashSet<Node>(current);

        if (fullOwner) {
            next.add(node);
        }
        else {
            next.remove(node);
        }
        fullOwners = Set.copyOf(next);
    }

    /**
     * Remove nodes that are leaving the cluster from the full-owner set. Called from the tend
     * thread only.
     */
    void removeFullOwners(Collection<Node> nodes) {
        Set<Node> current = fullOwners;

        if (current.isEmpty()) {
            return;
        }

        HashSet<Node> next = new HashSet<Node>(current);

        if (next.removeAll(nodes)) {
            fullOwners = Set.copyOf(next);
        }
    }

    public void log(ClusterDefinition def, String namespace) {
        for (int i = 0; i < replicas.length; i++) {
            AtomicReferenceArray<Node> nodeArray = replicas[i];
            int max = nodeArray.length();

            for (int j = 0; j < max; j++) {
                Node node = nodeArray.get(j);

                if (node != null) {
                    log.atInfo()
                        .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                        .log(namespace + ',' + i + ',' + j + ',' + node);
                }
            }
        }
    }
}
