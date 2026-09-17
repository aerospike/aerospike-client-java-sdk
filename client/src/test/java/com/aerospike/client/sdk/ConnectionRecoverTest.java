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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.command.Pool;
import com.aerospike.client.sdk.tend.ConnectionRecover;
import com.aerospike.client.sdk.tend.NodeValidator;
import com.aerospike.client.sdk.util.Version;

/**
 * Unit tests for {@link ConnectionRecover}, focused on auth-header draining during
 * connection recovery.
 */
class ConnectionRecoverTest {

    private Cluster cluster;
    private LoopbackSocketServer server;
    private Connection tendConn;

    @AfterEach
    void tearDown() throws IOException {
        if (tendConn != null) {
            tendConn.close();
            tendConn = null;
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        if (server != null) {
            server.close();
            server = null;
        }
    }

    /**
     * When the final auth-header byte reports a rejected login, drainHeader() closes the
     * connection. drain() must not continue to recover() and return that socket to the pool.
     */
    @Test
    void rejectedAuthDuringDrainClosesConnectionWithoutPooling() throws Exception {
        Harness harness = newHarness(
            socket -> writeBytes(socket, authResponseRemainder((byte) 1)));

        ConnectionRecover recover = newRecover(harness, partialAuthHeaderReadTimeout());
        Pool pool = harness.conn.getPool();
        int closedBefore = harness.node.connsClosed.get();

        assertTrue(recover.drain(new byte[8192]));
        assertTrue(recover.isComplete());
        assertTrue(harness.conn.isClosed(),
            "rejected auth must close the connection");
        assertEquals(0, pool.size(),
            "a closed connection must not be returned to the pool");
        assertEquals(closedBefore + 1, harness.node.connsClosed.get(),
            "connection must be closed via abort(), not pooled via recover()");
    }

    /**
     * Control case: a successful auth header is drained to completion and the connection
     * is returned to the pool.
     */
    @Test
    void successfulAuthDuringDrainReturnsConnectionToPool() throws Exception {
        Harness harness = newHarness(
            socket -> writeBytes(socket, authResponseRemainder((byte) 0)));

        ConnectionRecover recover = newRecover(harness, partialAuthHeaderReadTimeout());
        Pool pool = harness.conn.getPool();
        int closedBefore = harness.node.connsClosed.get();

        assertTrue(recover.drain(new byte[8192]));
        assertTrue(recover.isComplete());
        assertFalse(harness.conn.isClosed(),
            "successful drain must keep the connection open for reuse");
        assertEquals(1, pool.size(),
            "a healthy connection must be returned to the pool");
        assertEquals(closedBefore, harness.node.connsClosed.get(),
            "successful recovery must not count as a closed connection");

        pool.poll().close();
    }

    private Harness newHarness(Consumer<Socket> onPooledConnection) throws Exception {
        server = new LoopbackSocketServer();
        InetSocketAddress address =
            new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port());

        // Cluster tend probes seed hosts during construction. Use an unroutable port and
        // do not fail startup when it is unreachable.
        ClusterDefinition def = new ClusterDefinition(new Host("127.0.0.1", 9))
            .clusterName("connection-recover-" + UUID.randomUUID())
            .connPoolsPerNode(1)
            .failIfNotConnected(false);
        def.minConnsPerNode = 0;
        def.maxConnsPerNode = 5;

        cluster = new Cluster(def, SystemSettings.DEFAULT);

        server.expectConnection(ConnectionRecoverTest::holdOpen);
        tendConn = new Connection(address, 5_000, null, null);

        NodeValidator nv = new NodeValidator();
        nv.name = "test-node";
        nv.primaryHost = new Host(address.getHostString(), address.getPort());
        nv.primaryAddress = address;
        nv.primaryConn = tendConn;
        nv.version = Version.SERVER_VERSION_8_0;

        Node node = new Node(cluster, nv);

        LoopbackSocketServer.ExpectedConnection pooledConnection =
            server.expectConnection(onPooledConnection);
        Connection conn = node.getConnection(5_000, 5_000);
        pooledConnection.awaitHandled();

        return new Harness(node, conn);
    }

    private static ConnectionRecover newRecover(Harness harness, Connection.ReadTimeout crt) {
        return new ConnectionRecover(harness.conn, harness.node, 10_000, crt, true);
    }

    private static Connection.ReadTimeout partialAuthHeaderReadTimeout() {
        // Nine bytes were read before the auth result code at byte offset 9 timed out.
        byte[] buffer = new byte[ADMIN_HEADER_SIZE];
        Buffer.longToBytes(ADMIN_PROTO, buffer, 0);
        return new Connection.ReadTimeout(buffer, PARTIAL_AUTH_HEADER_BYTES, ADMIN_HEADER_SIZE,
            Command.STATE_READ_AUTH_HEADER);
    }

    private static byte[] authResponseRemainder(byte resultCode) {
        byte[] bytes = new byte[ADMIN_HEADER_SIZE - PARTIAL_AUTH_HEADER_BYTES];
        bytes[0] = resultCode;
        return bytes;
    }

    private static void writeBytes(Socket socket, byte... bytes) {
        try (OutputStream out = socket.getOutputStream()) {
            out.write(bytes);
            out.flush();
        }
        catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private static void holdOpen(Socket socket) {
        // Keep the node's tend connection alive for the duration of the test.
    }

    private record Harness(Node node, Connection conn) {}

    private static final int ADMIN_HEADER_SIZE = 24;
    private static final int ADMIN_HEADER_REMAINING = 16;
    private static final int PARTIAL_AUTH_HEADER_BYTES = 9;
    private static final long ADMIN_PROTO = (2L << 56) | (2L << 48) | ADMIN_HEADER_REMAINING;

    private static final class LoopbackSocketServer implements AutoCloseable {
        private final ServerSocket server;
        private final Queue<ExpectedConnection> handlers = new ConcurrentLinkedQueue<>();

        LoopbackSocketServer() throws IOException {
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().name("connection-recover-server").start(this::acceptLoop);
        }

        int port() {
            return server.getLocalPort();
        }

        ExpectedConnection expectConnection(Consumer<Socket> handler) {
            ExpectedConnection expected = new ExpectedConnection(handler);
            handlers.add(expected);
            return expected;
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    ExpectedConnection expected = handlers.poll();

                    if (expected != null) {
                        expected.handle(socket);
                    }
                    else {
                        // Unexpected connection: keep it open so client-side connect() succeeds.
                        holdOpen(socket);
                    }
                }
                catch (IOException ioe) {
                    if (!server.isClosed()) {
                        throw new UncheckedIOException(ioe);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
        }

        private static final class ExpectedConnection {
            private final Consumer<Socket> handler;
            private final CountDownLatch handled = new CountDownLatch(1);
            private volatile Throwable failure;

            private ExpectedConnection(Consumer<Socket> handler) {
                this.handler = handler;
            }

            private void handle(Socket socket) {
                try {
                    handler.accept(socket);
                }
                catch (Throwable t) {
                    failure = t;
                }
                finally {
                    handled.countDown();
                }
            }

            private void awaitHandled() throws InterruptedException {
                assertTrue(handled.await(5, TimeUnit.SECONDS),
                    "loopback server did not handle the expected connection");

                if (failure != null) {
                    throw new AssertionError("loopback server handler failed", failure);
                }
            }
        }
    }
}
