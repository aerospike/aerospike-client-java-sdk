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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.IntPredicate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.SingleNodeBatchFixture;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Connection;

/**
 * Full-owner tracking in {@link PartitionParser} and {@link Partitions}, driven by real
 * {@code replicas} responses served from a loopback socket.
 */
public class PartitionParserFullOwnerTest {
    private static final int PARTITIONS = Node.PARTITIONS;

    private FakeInfoServer server;
    private Cluster cluster;
    private Connection conn;
    private Node nodeA;
    private Node nodeB;
    private HashMap<String,Partitions> map;
    private int generation;

    @BeforeEach
    void setUp() throws IOException {
        server = new FakeInfoServer();
        cluster = SingleNodeBatchFixture.cluster();
        conn = new Connection(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()),
            5_000, cluster);
        nodeA = SingleNodeBatchFixture.node(cluster, "A");
        nodeB = SingleNodeBatchFixture.node(cluster, "B");
        map = new HashMap<String,Partitions>();
    }

    @AfterEach
    void tearDown() throws IOException {
        conn.close();
        server.close();
    }

    @Test
    void fullOwnerDetectedAtEveryReplicationFactor() {
        for (int rf = 1; rf <= 4; rf++) {
            final int replicaCount = rf;
            map = new HashMap<String,Partitions>();

            // A owns every partition, spread across all replica levels.
            IntPredicate[] spread = new IntPredicate[replicaCount];

            for (int i = 0; i < replicaCount; i++) {
                final int level = i;
                spread[i] = p -> p % replicaCount == level;
            }
            refresh(nodeA, ns("test", 0, spread));

            Partitions partitions = map.get("test");
            assertEquals(replicaCount, partitions.replicas.length, "map sized from replica count " + rf);
            assertTrue(partitions.isFullOwner(nodeA), "A owns every partition at rf " + rf);

            // B reports the namespace but owns nothing, so A keeps every slot.
            IntPredicate[] none = new IntPredicate[replicaCount];

            for (int i = 0; i < replicaCount; i++) {
                none[i] = p -> false;
            }
            refresh(nodeB, ns("test", 0, none));
            assertFalse(partitions.isFullOwner(nodeB), "B owns nothing at rf " + rf);
            assertTrue(partitions.isFullOwner(nodeA), "A unaffected at rf " + rf);
        }
    }

    @Test
    void partialOwnerIsNotFullOwner() {
        // Two nodes, RF 2: each is master for half and prole for the other half.
        refresh(nodeA, ns("test", 0, p -> p % 2 == 0, p -> false));
        refresh(nodeB, ns("test", 0, p -> p % 2 == 1, p -> false));

        Partitions partitions = map.get("test");
        assertFalse(partitions.isFullOwner(nodeA));
        assertFalse(partitions.isFullOwner(nodeB));

        // The proles arrive: each node now owns every partition.
        refresh(nodeA, ns("test", 0, p -> p % 2 == 0, p -> p % 2 == 1));
        refresh(nodeB, ns("test", 0, p -> p % 2 == 1, p -> p % 2 == 0));

        assertEquals(Set.of(nodeA, nodeB), partitions.getFullOwners());
    }

    @Test
    void ownershipLossOnRefreshDropsFullOwner() {
        refresh(nodeA, ns("test", 0, p -> true));
        Partitions partitions = map.get("test");
        assertTrue(partitions.isFullOwner(nodeA));

        refresh(nodeA, ns("test", 0, p -> p < PARTITIONS - 1));
        assertFalse(partitions.isFullOwner(nodeA), "one partition short is not a full owner");
    }

    @Test
    void namespaceNoLongerReportedDropsFullOwner() {
        refresh(nodeA, ns("a", 0, p -> true), ns("b", 0, p -> true));
        assertTrue(map.get("a").isFullOwner(nodeA));
        assertTrue(map.get("b").isFullOwner(nodeA));

        refresh(nodeA, ns("b", 0, p -> true));
        assertFalse(map.get("a").isFullOwner(nodeA), "namespace a no longer reported");
        assertTrue(map.get("b").isFullOwner(nodeA));
    }

    @Test
    void displacedOwnerIsDropped() {
        // A is master for even partitions and prole for odd ones.
        refresh(nodeA, ns("test", 0, p -> p % 2 == 0, p -> p % 2 == 1));
        Partitions partitions = map.get("test");
        assertTrue(partitions.isFullOwner(nodeA));

        // B takes over a few of A's master slots before A's next refresh.
        refresh(nodeB, ns("test", 0, p -> p < 10, p -> false));
        assertFalse(partitions.isFullOwner(nodeA), "A lost slots to B");
        assertFalse(partitions.isFullOwner(nodeB));
    }

    @Test
    void partitionsSkippedForOlderRegimeAreNotCounted() {
        refresh(nodeA, ns("test", 5, p -> true));
        Partitions partitions = map.get("test");
        assertTrue(partitions.scMode);
        assertTrue(partitions.isFullOwner(nodeA));

        // B claims every partition, but with an older regime, so none is mapped to it.
        refresh(nodeB, ns("test", 3, p -> true));
        assertFalse(partitions.isFullOwner(nodeB), "older-regime claims don't count");
        assertTrue(partitions.isFullOwner(nodeA), "A keeps its partitions");
    }

    @Test
    void replicationFactorChangeCarriesFullOwners() {
        refresh(nodeA, ns("test", 0, p -> p % 2 == 0, p -> p % 2 == 1));
        Partitions before = map.get("test");
        assertTrue(before.isFullOwner(nodeA));

        // The copy constructor alone keeps the set.
        Partitions copy = new Partitions(before, 3);
        assertEquals(before.getFullOwners(), copy.getFullOwners());

        // A parsed RF change rebuilds the map and A is still a full owner.
        refresh(nodeA, ns("test", 0, p -> p % 3 == 0, p -> p % 3 == 1, p -> p % 3 == 2));
        Partitions after = map.get("test");
        assertNotSame(before, after);
        assertEquals(3, after.replicas.length);
        assertTrue(after.isFullOwner(nodeA));
    }

    @Test
    void departedNodesAreRemoved() {
        refresh(nodeA, ns("test", 0, p -> p % 2 == 0, p -> p % 2 == 1));
        refresh(nodeB, ns("test", 0, p -> p % 2 == 1, p -> p % 2 == 0));
        Partitions partitions = map.get("test");
        assertEquals(Set.of(nodeA, nodeB), partitions.getFullOwners());

        partitions.removeFullOwners(List.of(nodeA));
        assertEquals(Set.of(nodeB), partitions.getFullOwners());
    }

    //-------------------------------------------------------
    // Helpers
    //-------------------------------------------------------

    private void refresh(Node node, String... namespaces) {
        server.respond("partition-generation\t" + (++generation) + "\nreplicas\t" +
            String.join(";", namespaces) + "\n");
        PartitionParser parser = new PartitionParser(conn, node, map, PARTITIONS);
        map = parser.getPartitionMap();
    }

    /**
     * One namespace entry of a replicas response: name:regime,count,bitmap,...
     */
    private static String ns(String name, int regime, IntPredicate... replicas) {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(':').append(regime).append(',').append(replicas.length);

        for (IntPredicate owns : replicas) {
            byte[] bits = new byte[PARTITIONS / 8];

            for (int p = 0; p < PARTITIONS; p++) {
                if (owns.test(p)) {
                    bits[p >> 3] |= (byte)(0x80 >> (p & 7));
                }
            }
            sb.append(',').append(Base64.getEncoder().encodeToString(bits));
        }
        return sb.toString();
    }

    /**
     * Answers each info request on one connection with the next queued response.
     */
    private static final class FakeInfoServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final BlockingQueue<String> responses = new LinkedBlockingQueue<String>();
        private final Thread thread;

        FakeInfoServer() throws IOException {
            serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            thread = new Thread(this::serve, "fake-info-server");
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void respond(String body) {
            responses.add(body);
        }

        private void serve() {
            try (Socket socket = serverSocket.accept()) {
                DataInputStream in = new DataInputStream(socket.getInputStream());
                OutputStream out = socket.getOutputStream();

                while (true) {
                    byte[] header = new byte[8];
                    in.readFully(header);
                    int size = (int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL);
                    in.skipNBytes(size);

                    byte[] body = responses.take().getBytes(StandardCharsets.UTF_8);
                    byte[] message = new byte[8 + body.length];
                    Buffer.longToBytes(body.length | (2L << 56) | (1L << 48), message, 0);
                    System.arraycopy(body, 0, message, 8, body.length);
                    out.write(message);
                    out.flush();
                }
            }
            catch (IOException | InterruptedException e) {
                // Closed by the test.
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            thread.interrupt();
        }
    }
}
