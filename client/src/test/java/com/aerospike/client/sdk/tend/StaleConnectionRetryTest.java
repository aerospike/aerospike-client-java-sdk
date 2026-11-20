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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.command.Command;
import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.command.SyncExecutor;
import com.aerospike.client.sdk.metrics.LatencyType;
import com.aerospike.client.sdk.metrics.MetricsExporterType;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.util.Version;

/**
 * A sync command whose pooled socket was closed or reset by the peer while idle runs the same
 * attempt once more on a new connection, without consuming a retry or counting an error. A write
 * whose request was sent is never sent again.
 */
public class StaleConnectionRetryTest {
	private static final String NAMESPACE = "test";
	private static final int TIMEOUT = 2000;
	private static final int RST_WAIT_MS = 200;

	/**
	 * The legacy Policy: 2 s socket and total timeout, no retries, no sleep between retries.
	 */
	private static final ResolvedSettings SETTINGS = Behavior.DEFAULT
		.deriveWithChanges("stale-connection-retry", builder -> builder
			.on(Selectors.all(), ops -> ops
				.abandonCallAfter(Duration.ofMillis(TIMEOUT))
				.waitForCallToComplete(Duration.ofMillis(TIMEOUT))
				.maximumNumberOfCallAttempts(1)
				.delayBetweenRetries(Duration.ZERO)))
		.getSettings(OpKind.READ, OpShape.POINT, Mode.AP);

	private Peer peer;
	private Cluster cluster;
	private Node node;

	/**
	 * Operational metrics are enabled because the SDK counts node errors only there; the legacy
	 * client always counted them.
	 */
	@BeforeEach
	void setUp() throws Exception {
		peer = new Peer();

		cluster = new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder.metrics(ops -> ops
				.enabled(true)
				.exporter(MetricsExporterType.NONE)
				.extended(ext -> ext.operational(op -> op.enabled(true)))))
			.connect();

		InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), peer.port());
		NodeValidator nv = new NodeValidator();
		nv.name = "BB9000000000001";
		nv.primaryHost = new Host("127.0.0.1", peer.port());
		nv.primaryAddress = address;
		nv.primaryConn = new Connection(address, 1000, cluster);
		nv.version = new Version("8.1.0.0");
		node = new Node(cluster, nv);
		peer.waitAccepted(1);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (node != null) {
			node.close();
		}
		if (cluster != null) {
			cluster.close();
		}
		if (peer != null) {
			peer.close();
		}
	}

	@Test
	void healthyPooledSocketIsReused() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();

		new TestCommand(false).execute();

		assertEquals(accepted, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(0);
	}

	@Test
	void pooledSocketClosedByPeerIsRetriedOnNewConnection() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();
		peer.last().close();
		Thread.sleep(RST_WAIT_MS);

		new TestCommand(false).execute();

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(1);
	}

	@Test
	void pooledSocketResetByPeerIsRetriedOnNewConnection() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();
		reset(peer.last());

		new TestCommand(false).execute();

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(1);
	}

	@Test
	void writeOnPooledSocketResetByPeerIsRetriedOnNewConnection() throws Exception {
		new TestCommand(true).execute();
		int accepted = peer.accepted.get();
		reset(peer.last());

		new TestCommand(true).execute();

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(1);
	}

	@Test
	void writeFailedAfterSendIsNotRetried() throws Exception {
		new TestCommand(true).execute();
		int accepted = peer.accepted.get();
		peer.resetAfterRequest = true;

		try {
			new TestCommand(true).execute();
			fail("Write succeeded after the peer reset the connection");
		}
		catch (AerospikeException.Connection ae) {
			assertTrue(ae.getInDoubt());
		}

		assertEquals(accepted, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertEquals(1, node.getErrorCount(NAMESPACE));
	}

	@Test
	void staleRetryHappensOncePerAttempt() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();
		peer.resetAfterRequest = true;

		try {
			new TestCommand(false).execute();
			fail("Read succeeded after the peer reset every connection");
		}
		catch (AerospikeException.Connection ae) {
			assertEquals(1, ae.getIteration());
		}

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(3, peer.requests.get());
		assertEquals(1, node.getErrorCount(NAMESPACE));
	}

	private void assertNoErrorOrRetry(int closed) throws Exception {
		Field field = Node.class.getDeclaredField("errorRateCount");
		field.setAccessible(true);

		assertEquals(0, ((AtomicInteger)field.get(node)).get());
		assertEquals(0, node.getErrorCount(NAMESPACE));
		assertEquals(0, cluster.getRetryCount());
		assertEquals(closed, node.getConnectionsClosed());
	}

	private static void reset(Socket socket) throws Exception {
		socket.setSoLinger(true, 0);
		socket.close();
		Thread.sleep(RST_WAIT_MS);
	}

	/**
	 * Command buffer holding one 8-byte request. CommandBuffer has no public way to write raw bytes,
	 * so its fields are set directly.
	 */
	private static CommandBuffer request() {
		try {
			CommandBuffer cb = new CommandBuffer();
			Field buffer = CommandBuffer.class.getDeclaredField("dataBuffer");
			buffer.setAccessible(true);
			buffer.set(cb, new byte[8]);
			Field offset = CommandBuffer.class.getDeclaredField("dataOffset");
			offset.setAccessible(true);
			offset.setInt(cb, 8);
			return cb;
		}
		catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * latencyEnabled is set so the cluster counts retries, as the legacy client always did;
	 * getLatencyType returns NONE so no latency is recorded.
	 */
	private final class TestCommand extends SyncExecutor {
		private final boolean write;

		TestCommand(boolean write) {
			super(cluster, new Command(cluster, NAMESPACE, null, null, null, true, SETTINGS));
			this.write = write;
		}

		@Override
		protected boolean isWrite() {
			return write;
		}

		@Override
		protected Node getNode() {
			return node;
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.NONE;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			return request();
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			conn.readFully(new byte[8], 8);
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			return true;
		}
	}

	/**
	 * Answers every 8-byte request with an 8-byte response, or resets the connection after reading
	 * the request when resetAfterRequest is set.
	 */
	private static final class Peer implements AutoCloseable {
		final AtomicInteger accepted = new AtomicInteger();
		final AtomicInteger requests = new AtomicInteger();
		volatile boolean resetAfterRequest;
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();

		Peer() throws IOException {
			server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		Socket last() {
			synchronized (sockets) {
				return sockets.get(sockets.size() - 1);
			}
		}

		void waitAccepted(int count) throws InterruptedException {
			long end = System.currentTimeMillis() + TIMEOUT;

			while (accepted.get() < count && System.currentTimeMillis() < end) {
				Thread.sleep(10);
			}
			assertEquals(count, accepted.get());
		}

		private void accept() {
			while (true) {
				Socket socket;

				try {
					socket = server.accept();
				}
				catch (IOException ioe) {
					return;
				}

				synchronized (sockets) {
					sockets.add(socket);
				}
				accepted.incrementAndGet();

				Thread thread = new Thread(() -> serve(socket));
				thread.setDaemon(true);
				thread.start();
			}
		}

		private void serve(Socket socket) {
			try {
				DataInputStream in = new DataInputStream(socket.getInputStream());
				OutputStream out = socket.getOutputStream();
				byte[] request = new byte[8];

				while (true) {
					in.readFully(request);
					requests.incrementAndGet();

					if (resetAfterRequest) {
						socket.setSoLinger(true, 0);
						socket.close();
						return;
					}
					out.write(new byte[8]);
					out.flush();
				}
			}
			catch (IOException ioe) {
			}
		}

		@Override
		public void close() throws IOException {
			server.close();

			synchronized (sockets) {
				for (Socket socket : sockets) {
					socket.close();
				}
			}
		}
	}
}
