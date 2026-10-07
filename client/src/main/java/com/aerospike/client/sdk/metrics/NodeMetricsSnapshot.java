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
package com.aerospike.client.sdk.metrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.tend.Partitions;

/**
 * Node metrics snapshot.
 */
public final class NodeMetricsSnapshot {
    public final String nodeName;
    public final String nodeAddress;
    public final int nodePort;

    // TODO Support failure counts.
    //public final long connectionOpenFailure;
    //public final long connectionTlsHandshakeFailures;
    //public final long connectionAuthFailure;

    public final ConnectionMetricsSnapshot connections;
    public final NamespaceMetricsSnapshot[] namespaces;

	public NodeMetricsSnapshot(Node node) {
		this.nodeName = node.getName();
		this.nodeAddress = node.getAddress().getAddress().toString();
		this.nodePort = node.getAddress().getPort();
        //this.connectionOpenFailure = connectionOpenFailure;
        //this.connectionTlsHandshakeFailures = connectionTlsHandshakeFailures;
        //this.connectionAuthFailure = connectionAuthFailure;
        this.connections = new ConnectionMetricsSnapshot(node);

        HashMap<String,Partitions> partitionMap = node.cluster.getPartitionMap();
        Set<String> namespaces = partitionMap.keySet();
        int count = 0;

        this.namespaces = new NamespaceMetricsSnapshot[namespaces.size()];

        for (String namespace : namespaces) {
            this.namespaces[count++] = new NamespaceMetricsSnapshot(node, namespace);
        }
	}

    @Override
    public String toString() {
        return "NodeMetricsSnapshot{"
            + "nodeName=" + nodeName
            + ", nodeAddress=" + nodeAddress
            + ", nodePort=" + nodePort
            //+ ", connectionOpenFailure=" + connectionOpenFailure
            //+ ", connectionTlsHandshakeFailures=" + connectionTlsHandshakeFailures
            //+ ", connectionAuthFailure=" + connectionAuthFailure
            + ", connections=" + connections
            + ", namespaces=" + Arrays.toString(namespaces)
            + '}';
    }
}
