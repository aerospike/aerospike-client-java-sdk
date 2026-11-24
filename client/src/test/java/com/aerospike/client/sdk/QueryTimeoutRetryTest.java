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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.command.PartitionFilter;
import com.aerospike.client.sdk.command.PartitionTracker;
import com.aerospike.client.sdk.command.PartitionTracker.NodePartitions;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.command.QueryNodeExecutor;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.BinDataType;
import com.aerospike.client.sdk.query.Order;
import com.aerospike.client.sdk.query.QueryBuilder;
import com.aerospike.client.sdk.tend.NodeValidator;
import com.aerospike.client.sdk.util.Version;

/**
 * Which query failures the client retries, against a loopback node that either never answers
 * (a client socket timeout) or answers every request with one error code (a server error).
 *
 * <p>The node counts the requests it receives, so a retry is observed as a request that arrives
 * after the first. See {@link QueryBuilder#topK(int)} for the behavior under test.</p>
 */
class QueryTimeoutRetryTest {
    private static final int ATTEMPTS = 3;
    private static final int SOCKET_TIMEOUT_MS = 100;

    /** The response of a node that reads every request and never answers. */
    private static final byte[] NEVER_ANSWER = null;

    private enum QueryKind {
        /** Top-K, which every node ranks, so a node has no partial result to resume from. */
        TOP_K,
        /** A long query without Top-K. */
        PLAIN
    }

    private Cluster cluster;
    private LoopbackNode loopbackNode;
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
        if (loopbackNode != null) {
            loopbackNode.close();
            loopbackNode = null;
        }
        Behavior.restoreBehaviorRegistry();
    }

    @Test
    void topKQueryIsNotRetriedAfterClientTimeout() throws Exception {
        QueryNodeExecutor executor = newExecutor(QueryKind.TOP_K, NEVER_ANSWER);

        AerospikeException.Timeout timeout =
            assertThrows(AerospikeException.Timeout.class, executor::execute);

        assertTrue(timeout.client, "the socket timeout is a client timeout");
        assertEquals(1, loopbackNode.awaitRequests(1));
    }

    @Test
    void topKQueryIsNotRetriedAfterServerTimeout() throws Exception {
        QueryNodeExecutor executor =
            newExecutor(QueryKind.TOP_K, errorResponse(ResultCode.TIMEOUT));

        AerospikeException.Timeout timeout =
            assertThrows(AerospikeException.Timeout.class, executor::execute);

        assertFalse(timeout.client, "the server sent the timeout");
        assertEquals(1, loopbackNode.awaitRequests(1));
    }

    @Test
    void plainQueryIsRetriedAfterClientTimeout() throws Exception {
        QueryNodeExecutor executor = newExecutor(QueryKind.PLAIN, NEVER_ANSWER);

        assertThrows(AerospikeException.Timeout.class, executor::execute);

        assertEquals(ATTEMPTS, loopbackNode.awaitRequests(ATTEMPTS));
    }

    @Test
    void plainQueryIsRetriedAfterServerTimeout() throws Exception {
        QueryNodeExecutor executor =
            newExecutor(QueryKind.PLAIN, errorResponse(ResultCode.TIMEOUT));

        assertThrows(AerospikeException.Timeout.class, executor::execute);

        assertEquals(ATTEMPTS, loopbackNode.awaitRequests(ATTEMPTS));
    }

    @ParameterizedTest
    @EnumSource(QueryKind.class)
    void queryTimeoutFromServerLimitIsNotRetried(QueryKind kind) throws Exception {
        QueryNodeExecutor executor = newExecutor(kind, errorResponse(ResultCode.QUERY_TIMEOUT));

        AerospikeException ae = assertThrows(AerospikeException.class, executor::execute);

        assertInstanceOf(AerospikeException.QueryException.class, ae);
        assertEquals(ResultCode.QUERY_TIMEOUT, ae.getResultCode());
        assertEquals(1, loopbackNode.awaitRequests(1));
    }

    @ParameterizedTest
    @EnumSource(QueryKind.class)
    void forbiddenFromTopKConcurrencyCapIsNotRetried(QueryKind kind) throws Exception {
        QueryNodeExecutor executor = newExecutor(kind, errorResponse(ResultCode.FAIL_FORBIDDEN));

        AerospikeException ae = assertThrows(AerospikeException.class, executor::execute);

        assertEquals(ResultCode.FAIL_FORBIDDEN, ae.getResultCode());
        assertEquals(1, loopbackNode.awaitRequests(1));
    }

    /**
     * Build an executor for one query against a loopback node.
     *
     * @param response what the node writes for every request it receives, or {@code null} for a
     *                 node that never answers
     */
    private QueryNodeExecutor newExecutor(QueryKind kind, byte[] response) throws Exception {
        loopbackNode = new LoopbackNode(response);
        Node node = newNode(loopbackNode);

        DataSet dataSet = DataSet.of("test", "timeout_retry");
        ResolvedSettings settings = querySettings();
        Session session = new Session(null, Behavior.DEFAULT);
        QueryBuilder builder = new QueryBuilder(session, dataSet);

        if (kind != QueryKind.PLAIN) {
            builder.orderBy("rank", BinDataType.INTEGER, Order.ASC).topK(3);
        }

        QueryCommand cmd = new QueryCommand(cluster, dataSet, null, null, settings, builder, false);

        PartitionTracker tracker = new PartitionTracker(cmd, new Node[1], PartitionFilter.all());
        NodePartitions nodePartitions = new NodePartitions(node, 1);
        return new QueryNodeExecutor(cluster, cmd, 1L, tracker, nodePartitions, null);
    }

    private Node newNode(LoopbackNode loopback) throws Exception {
        InetSocketAddress address =
            new InetSocketAddress(InetAddress.getLoopbackAddress(), loopback.port());

        // Cluster tend probes seed hosts during construction. Use an unroutable port and
        // do not fail startup when it is unreachable.
        ClusterDefinition def = new ClusterDefinition(new Host("127.0.0.1", 9))
            .clusterName("query-timeout-retry-" + UUID.randomUUID())
            .connPoolsPerNode(1)
            .failIfNotConnected(false);
        def.minConnsPerNode = 0;
        def.maxConnsPerNode = 5;

        cluster = new Cluster(def, SystemSettings.DEFAULT);

        tendConn = new Connection(address, 5_000, null, null);

        NodeValidator nv = new NodeValidator();
        nv.name = "test-node";
        nv.primaryHost = new Host(address.getHostString(), address.getPort());
        nv.primaryAddress = address;
        nv.primaryConn = tendConn;
        nv.version = Version.SERVER_VERSION_8_0;
        return new Node(cluster, nv);
    }

    /**
     * Settings with a short socket timeout and no sleep between attempts, so that a retry is
     * quick and the timeout comes from the socket rather than from a connection being recovered.
     */
    private static ResolvedSettings querySettings() {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges(
            "query-timeout-retry-" + UUID.randomUUID(), builder ->
            builder.on(Selectors.reads().query(), ops -> ops
                .waitForCallToComplete(Duration.ofMillis(SOCKET_TIMEOUT_MS))
                .waitForSocketResponseAfterCallFails(Duration.ZERO)
                .delayBetweenRetries(Duration.ZERO)
                .maximumNumberOfCallAttempts(ATTEMPTS)));
        return behavior.getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
    }

    /** A last message of a query response that carries only a result code. */
    private static byte[] errorResponse(int resultCode) {
        int size = Command.MSG_REMAINING_HEADER_SIZE;
        byte[] bytes = new byte[8 + size];
        long proto = (2L << 56) | (Command.AS_MSG_TYPE << 48) | size;
        Buffer.longToBytes(proto, bytes, 0);
        bytes[8] = (byte)size;
        bytes[8 + 3] = (byte)Command.INFO3_LAST;
        bytes[8 + 5] = (byte)resultCode;
        return bytes;
    }

    /**
     * Accepts connections and counts the requests that arrive on them. It answers each request
     * with a fixed response, or never answers when there is none.
     */
    private static final class LoopbackNode implements AutoCloseable {
        private static final int PROTO_HEADER_SIZE = 8;

        private final ServerSocket server;
        private final byte[] response;
        private final AtomicInteger requests = new AtomicInteger();

        LoopbackNode(byte[] response) throws IOException {
            this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            this.response = response;
            Thread.ofVirtual().name("query-timeout-retry-server").start(this::acceptLoop);
        }

        int port() {
            return server.getLocalPort();
        }

        /**
         * The number of requests received once at least {@code expected} have arrived, giving
         * the node a moment to read a request that the client has already written.
         */
        int awaitRequests(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();

            while (requests.get() < expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return requests.get();
        }

        private void acceptLoop() {
            while (! server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread.ofVirtual().name("query-timeout-retry-connection").start(() -> serve(socket));
                }
                catch (IOException ioe) {
                    // The server was closed.
                    return;
                }
            }
        }

        private void serve(Socket socket) {
            try (socket) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                byte[] header = new byte[PROTO_HEADER_SIZE];

                while (in.readNBytes(header, 0, PROTO_HEADER_SIZE) == PROTO_HEADER_SIZE) {
                    long proto = Buffer.bytesToLong(header, 0);
                    int size = (int)(proto & 0xFFFFFFFFFFFFL);
                    byte[] body = in.readNBytes(size);

                    if (body.length < size) {
                        return;
                    }
                    requests.incrementAndGet();

                    if (response != null) {
                        out.write(response);
                        out.flush();
                    }
                }
            }
            catch (IOException ioe) {
                // The client closed the connection.
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }
}
