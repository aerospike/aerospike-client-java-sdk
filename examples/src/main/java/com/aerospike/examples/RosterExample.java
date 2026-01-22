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
package com.aerospike.examples;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Info;
import com.aerospike.client.fluent.Node;

import static java.lang.String.format;

/**
 * RosterExample demonstrates how to read and initialize the roster for a namespace
 * using Aerospike's roster management Info commands.
 *
 * <p><strong>Important:</strong> This example ONLY works with Aerospike clusters
 * running in <em>strong-consistency mode</em>. Roster-based cluster management
 * is not supported in AP (availability) mode.</p>
 *
 * <p>For more details on Aerospike consistency modes, see:
 * <a href="https://aerospike.com/docs/database/learn/architecture/clustering/consistency-modes">
 * https://aerospike.com/docs/database/learn/architecture/clustering/consistency-modes
 * </a>
 * </p>
 *
 * <p>The example:
 * <ul>
 *   <li>Reads the current roster and observed nodes from the cluster</li>
 *   <li>Sets the roster on all nodes using the observed node list</li>
 *   <li>Triggers a recluster to apply the changes</li>
 * </ul>
 *
 */
public class RosterExample extends Example{

    public RosterExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        Node[] nodes = cluster.getNodes();

        String response = Info.request(nodes[0], format("roster:namespace=%s", args.namespace));
        System.out.printf("Current roster: %s\n", response);

        // Parse observed nodes from response
        String observedNodes = parseObservedNodes(response);
        System.out.printf("Observed nodes: %s\n", observedNodes);

        for (Node node : nodes) {
            String setResponse = Info.request(node,
                    format("roster-set:namespace=%s;nodes=%s", args.namespace, observedNodes));
            System.out.printf("roster-set on %s: %s\n", node.getHost(), setResponse);
        }

        // Trigger recluster
        for (Node node : nodes) {
            String reclusterResponse = Info.request(node, "recluster:");
            System.out.printf("recluster on %s: %s\n", node.getHost(), reclusterResponse);
        }

        // Wait for changes to take effect
        Thread.sleep(3000);

        System.out.println("Roster initialization complete");
    }

    private static String parseObservedNodes(String response) {
        // Parse "roster=null:pending_roster=null:observed_nodes=BB9A469513007B2,BB92D77C4434192,BB928E67FF3F0C6"
        String[] parts = response.split(":");
        for (String part : parts) {
            if (part.startsWith("observed_nodes=")) {
                return part.substring("observed_nodes=".length());
            }
        }
        throw new IllegalArgumentException("Could not find observed_nodes in response: " + response);
    }
}
