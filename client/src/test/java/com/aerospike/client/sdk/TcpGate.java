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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in-process TCP forwarder whose listening socket can be closed and reopened on a
 * fixed port.
 * <p>
 * This exists because a Toxiproxy container cannot produce a refused connection. Docker
 * publishes the container's port by binding the host port itself, so the host keeps
 * accepting TCP connections even while the proxy inside is disabled -- the connection is
 * established and then immediately closed. That is a read failure, not a connect failure,
 * and it never reaches the code that releases a pool slot reserved for a connection open
 * that failed. Tests relying on it pass whether or not that release is present.
 * <p>
 * Closing this gate's {@link ServerSocket} leaves nothing bound to the port, so a connect
 * attempt gets a genuine {@code ECONNREFUSED} and the client fails where the real defect
 * lives. Data-level faults are still supplied by Toxiproxy, which sits upstream of this
 * gate and is unaffected by it.
 */
final class TcpGate implements Closeable {
    private static final int UPSTREAM_CONNECT_TIMEOUT_MS = 5_000;
    private static final int BUFFER_SIZE = 16 * 1024;

    /** Every proto message starts with a version byte, a type byte, then a 48-bit body length. */
    private static final int PROTO_HEADER_SIZE = 8;
    private static final int AS_MSG_TYPE = 3;
    private static final int MSG_TYPE_COMPRESSED = 4;

    /** Data messages the client may still send before the gate trips. Negative disables it. */
    private final AtomicInteger allowance = new AtomicInteger(-1);

    private final String upstreamHost;
    private final int upstreamPort;
    private final int port;

    /** Sockets of established connections, so they can be dropped along with the listener. */
    private final Set<Socket> live = ConcurrentHashMap.newKeySet();

    /** The client-facing half of each pair, so tests can push bytes toward the client. */
    private final Set<Socket> clientSide = ConcurrentHashMap.newKeySet();

    private volatile ServerSocket listener;

    private TcpGate(String upstreamHost, int upstreamPort, int port) {
        this.upstreamHost = upstreamHost;
        this.upstreamPort = upstreamPort;
        this.port = port;
    }

    /** Bind a loopback port and start forwarding to {@code upstreamHost:upstreamPort}. */
    static TcpGate open(String upstreamHost, int upstreamPort) throws IOException {
        ServerSocket socket = bind(0);
        TcpGate gate = new TcpGate(upstreamHost, upstreamPort, socket.getLocalPort());
        gate.startAccepting(socket);
        return gate;
    }

    int getPort() {
        return port;
    }

    /**
     * Stop listening and drop established connections. Subsequent connects are refused
     * because nothing is bound to the port.
     */
    void refuseConnections() {
        ServerSocket socket = listener;
        listener = null;
        closeQuietly(socket);

        for (Socket live : this.live) {
            closeQuietly(live);
        }
        this.live.clear();
        this.clientSide.clear();
    }

    /**
     * Forward {@code count} more data messages from the client, then refuse connections when the
     * one after that arrives, before it reaches the server.
     * <p>
     * This exists to fail a specific command in a multi-command sequence. {@link
     * #refuseConnections()} can only cut the whole exchange, which always lands on the first
     * command, so a later one cannot be reached. Only data messages are counted: cluster tending
     * uses info messages and shares these connections, so counting everything would make the
     * allowance depend on tend timing.
     */
    void refuseAfterClientMessages(int count) {
        allowance.set(count);
    }

    /** Rebind the same port and resume forwarding. */
    void allowConnections() throws IOException {
        if (listener != null) {
            return;
        }
        startAccepting(bind(port));
    }

    private static ServerSocket bind(int port) throws IOException {
        ServerSocket socket = new ServerSocket();
        // Without this, rebinding the port fails while connections accepted on the
        // previous listener are still in TIME_WAIT.
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
        return socket;
    }

    private void startAccepting(ServerSocket socket) {
        listener = socket;
        Thread.ofVirtual().name("tcp-gate-accept").start(() -> acceptLoop(socket));
    }

    private void acceptLoop(ServerSocket socket) {
        while (! socket.isClosed()) {
            Socket downstream;

            try {
                downstream = socket.accept();
            }
            catch (IOException ioe) {
                return; // Listener closed by refuseConnections() or close().
            }

            try {
                Socket upstream = new Socket();
                upstream.connect(
                    new InetSocketAddress(upstreamHost, upstreamPort), UPSTREAM_CONNECT_TIMEOUT_MS);

                live.add(downstream);
                live.add(upstream);
                clientSide.add(downstream);

                pump(downstream, upstream, true);
                pump(upstream, downstream, false);
            }
            catch (IOException ioe) {
                closeQuietly(downstream);
            }
        }
    }

    private void pump(Socket from, Socket to, boolean fromClient) {
        Thread.ofVirtual().name("tcp-gate-pump").start(() -> {
            byte[] buffer = new byte[BUFFER_SIZE];
            MessageBoundaries boundaries = fromClient ? new MessageBoundaries() : null;

            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                int count;

                while ((count = in.read(buffer)) > 0) {
                    int forward = boundaries == null ? count : boundaries.forwardable(buffer, count);

                    if (forward > 0) {
                        out.write(buffer, 0, forward);
                        out.flush();
                    }

                    if (forward < count) {
                        // The withheld bytes begin a message the allowance does not cover.
                        refuseConnections();
                        return;
                    }
                }
            }
            catch (IOException ioe) {
                // Either side went away; fall through and tear the pair down.
            }
            finally {
                closeQuietly(from);
                closeQuietly(to);
                live.remove(from);
                live.remove(to);
                clientSide.remove(from);
                clientSide.remove(to);
            }
        });
    }

    /**
     * Write raw bytes to every established client connection, as though the server had
     * sent them. Only safe while those connections are idle, since the forwarding threads
     * write to the same sockets.
     */
    /** Number of established client connections currently being forwarded. */
    int clientConnectionCount() {
        return clientSide.size();
    }

    void sendToClients(byte[] bytes) throws IOException {
        for (Socket socket : clientSide) {
            OutputStream out = socket.getOutputStream();
            out.write(bytes);
            out.flush();
        }
    }

    /**
     * Tracks where one client message ends and the next begins, so the gate trips between two
     * commands rather than part way through one. Only the boundaries matter here; the bodies are
     * forwarded unread.
     */
    private final class MessageBoundaries {
        private final byte[] header = new byte[PROTO_HEADER_SIZE];
        private int headerFilled;
        private long bodyRemaining;

        /** How much of {@code buffer} may be forwarded before the gate has to trip. */
        int forwardable(byte[] buffer, int length) {
            int offset = 0;

            while (offset < length) {
                if (bodyRemaining == 0) {
                    int messageStart = offset;
                    int take = Math.min(PROTO_HEADER_SIZE - headerFilled, length - offset);

                    System.arraycopy(buffer, offset, header, headerFilled, take);
                    headerFilled += take;
                    offset += take;

                    if (headerFilled < PROTO_HEADER_SIZE) {
                        // Header split across reads. Decide once the rest of it arrives.
                        return length;
                    }

                    headerFilled = 0;
                    bodyRemaining = bodyLength(header);

                    if (isDataMessage(header) && ! spendAllowance()) {
                        return messageStart;
                    }
                }

                long skipped = Math.min(bodyRemaining, length - offset);
                bodyRemaining -= skipped;
                offset += (int)skipped;
            }
            return length;
        }
    }

    /** @return false once the allowance is used up, meaning the gate should trip. */
    private boolean spendAllowance() {
        int remaining = allowance.get();

        if (remaining < 0) {
            return true;
        }
        if (remaining == 0) {
            return false;
        }
        allowance.decrementAndGet();
        return true;
    }

    private static long bodyLength(byte[] header) {
        long length = 0;

        for (int i = 2; i < PROTO_HEADER_SIZE; i++) {
            length = (length << 8) | (header[i] & 0xFF);
        }
        return length;
    }

    private static boolean isDataMessage(byte[] header) {
        int type = header[1] & 0xFF;
        return type == AS_MSG_TYPE || type == MSG_TYPE_COMPRESSED;
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        }
        catch (IOException ioe) {
            // Nothing useful to do while tearing down.
        }
    }

    @Override
    public void close() {
        refuseConnections();
    }
}
