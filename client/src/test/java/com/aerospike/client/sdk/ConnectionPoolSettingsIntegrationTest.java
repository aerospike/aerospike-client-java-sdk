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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.policy.Behavior;

class ConnectionPoolSettingsIntegrationTest {

    private static final Duration SOCKET_IDLE_TRIM = Duration.ofMillis(50);
    private static final int TIGHT_MAX_CONNS_PER_NODE = 8;

    @Test
    void maximumSocketIdleTime_trimsIdlePooledConnectionsOnBalance() throws Exception {
        Args args = Args.Instance;

        SystemSettings poolTuned = SystemSettings.builder()
            .connections(ops -> ops
                .minimumConnectionsPerNode(0)
                .maximumConnectionsPerNode(200)
                .maximumSocketIdleTime(SOCKET_IDLE_TRIM))
            .build()
            .mergeWith(SystemSettings.DEFAULT);

        Cluster cluster = buildClusterDefinition(args, poolTuned).connect();
        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            Key key = args.set.id("poolIdleTrim");
            String bin = "b";

            session.upsert(key).bins(bin).values(1).execute();

            for (int i = 0; i < 32; i++) {
                RecordStream rs = session.query(key).execute();
                while (rs.hasNext()) {
                    rs.next();
                }
            }

            Node[] nodes = cluster.getNodes();
            long closedBefore = 0;
            for (Node n : nodes) {
                closedBefore += n.connsClosed.get();
            }

            Thread.sleep(200);

            for (Node n : nodes) {
                n.balanceConnections();
            }

            long closedAfter = 0;
            for (Node n : nodes) {
                closedAfter += n.connsClosed.get();
            }

            assertTrue(
                closedAfter > closedBefore,
                "Idle pooled connections should be closed when older than maximumSocketIdleTime "
                    + "(trim=" + SOCKET_IDLE_TRIM + ")");
        }
        finally {
            cluster.close();
        }
    }

    /**
     * Asserts {@link ResultCode#NO_MORE_CONNECTIONS} by leasing sync pool connections from
     * {@link Node#getConnection(int, int)} until the per-node cap is hit.
     * {@code connPoolsPerNode(1)} keeps the cap equal to {@code maximumConnectionsPerNode}.
     */
    @Test
    public void maximumConnectionsPerNode_exhaustionThrowsNoMoreConnections() throws Exception {
        Args args = Args.Instance;

        SystemSettings tightPool = SystemSettings.builder()
            .connections(ops -> ops
                .minimumConnectionsPerNode(0)
                .maximumConnectionsPerNode(TIGHT_MAX_CONNS_PER_NODE))
            .build()
            .mergeWith(SystemSettings.DEFAULT);

        try (Cluster cluster = buildClusterDefinition(args, tightPool).connPoolsPerNode(1).connect()) {
            Node node = cluster.getNodes()[0];
            int expectedMax = cluster.def.maxConnsPerNode;

            List<Connection> held = new ArrayList<>(expectedMax + 1);
            AerospikeException.Connection exhaust = null;

            try {
                while (true) {
                    try {
                        held.add(node.getConnection(5_000, 5_000));
                    } catch (AerospikeException.Connection e) {
                        exhaust = e;
                        break;
                    }
                }

                assertNotNull(exhaust, "expected AerospikeException.Connection when pool is full");
                assertEquals(ResultCode.NO_MORE_CONNECTIONS, exhaust.getResultCode());
                assertEquals(
                        expectedMax,
                        held.size(),
                        "should hold exactly maxConnsPerNode connections before exhaustion");
            } finally {
                for (Connection c : held) {
                    node.putConnection(c);
                }
            }
        }
    }

    private static ClusterDefinition buildClusterDefinition(Args args, SystemSettings poolTuned) {
        Host[] hosts = Host.parseHosts(args.host, args.port);

        ClusterDefinition def = new ClusterDefinition(hosts)
            .withLogLevel(Log.Level.DEBUG)
            .clusterName(args.clusterName)
            .withSystemSettings(poolTuned);

        if (args.useServicesAlternate) {
            def.usingServicesAlternate();
        }

        return def;
    }
}
