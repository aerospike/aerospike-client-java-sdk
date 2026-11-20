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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.Endpoint;
import com.aerospike.client.sdk.discovery.PassThroughAddressTranslator;
import com.aerospike.client.sdk.util.Version;

public class TranslationCollisionTest {
	private final List<FakeNode> servers = new ArrayList<>();
	private final List<Node> nodes = new ArrayList<>();
	private Cluster cluster;

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}. WARN lines logged on the test thread are the legacy test's warnings; the
	 * tend thread's are ignored.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;
	private String warnPrefix;
	private int warningsCleared;

	static final class OneAddressTranslator implements AddressTranslator {
		private final int port;

		OneAddressTranslator(int port) {
			this.port = port;
		}

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.of(new Host("127.0.0.1", port));
		}
	}

	@BeforeEach
	void setUp() {
		warnPrefix = "[" + Thread.currentThread().getName() + "] WARN ";
		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() throws Exception {
		System.setErr(originalErr);

		for (Node node : nodes) {
			node.close();
		}

		if (cluster != null) {
			cluster.close();
		}

		for (FakeNode server : servers) {
			server.close();
		}
	}

	@Test
	void staticMapCollisionInOneReplyCreatesNeither() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A", "[[BB9B,,[10.0.0.2:" + b.port() + "]],[BB9C,,[10.0.0.3:" + b.port() + "]]]");
		Node seed = node(definition().ipMap(Map.of("10.0.0.2", "127.0.0.1", "10.0.0.3", "127.0.0.1")), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);

		assertTrue(peers.nodes.isEmpty());
		assertEquals(0, b.accepted.get());
		assertEquals(-1, seed.getPeersGeneration());
		assertEquals(2, collisions().size());

		for (String message : collisions()) {
			assertNames(message, "BB9B", "BB9C", "127.0.0.1:" + b.port(), "static-map", "Give each node its own reachable address or mapping");
		}
	}

	@Test
	void customTranslatorCollisionNamesClass() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A", "[[BB9B,,[10.0.0.2:3000]],[BB9C,,[10.0.0.3:3000]]]");
		Node seed = node(definition().addressTranslator(new OneAddressTranslator(b.port())), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);

		assertTrue(peers.nodes.isEmpty());
		assertEquals(0, b.accepted.get());
		assertEquals(2, collisions().size());

		for (String message : collisions()) {
			assertNames(message, "BB9B", "BB9C", "127.0.0.1:" + b.port(), OneAddressTranslator.class.getName());
		}
	}

	@Test
	void collisionWithLiveNodeKeepsLiveNode() throws Exception {
		FakeNode live = server("BB9L");
		FakeNode a = server("BB9A", "[[BB9P,,[10.0.0.2:" + live.port() + "]]]");
		Node seed = node(definition().ipMap(Map.of("10.0.0.2", "127.0.0.1")), a);
		Node liveNode = node(live);
		HashMap<String,Node> nodesMap = nodesMap();
		nodesMap.put(liveNode.getName(), liveNode);
		await(() -> live.accepted.get() == 1);

		try {
			Peers peers = new Peers(16);
			seed.refreshPeers(peers);

			assertTrue(peers.nodes.isEmpty());
			assertEquals(1, live.accepted.get());
			assertSame(liveNode, nodesMap.get("BB9L"));
			assertTrue(liveNode.isActive());
			assertEquals(1, collisions().size());
			assertNames(collisions().get(0), "BB9P", "BB9L", "127.0.0.1:" + live.port(), "static-map");
		}
		finally {
			nodesMap.remove(liveNode.getName());
		}
	}

	@Test
	void collisionWithSeedBeforeItJoinsCreatesNoNode() throws Exception {
		FakeNode a = server("BB9A", "[[BB9B,,[10.0.0.2:3000]]]");
		Node seed = node(definition().addressTranslator(new OneAddressTranslator(a.port())), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);

		assertTrue(peers.nodes.isEmpty());
		assertTrue(nodesMap().isEmpty());
		assertEquals(1, a.accepted.get());
		assertEquals(-1, seed.getPeersGeneration());
		assertEquals(1, collisions().size());
		assertNames(collisions().get(0), "BB9B", "BB9A", "127.0.0.1:" + a.port(), OneAddressTranslator.class.getName());
	}

	@Test
	void distinctPortsOnSharedHostCreateEveryPeer() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode c = server("BB9C");
		FakeNode a = server("BB9A", "[[BB9B,,[10.0.0.2:" + b.port() + "]],[BB9C,,[10.0.0.3:" + c.port() + "]]]");
		Node seed = node(definition().ipMap(Map.of("10.0.0.2", "127.0.0.1", "10.0.0.3", "127.0.0.1")), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertEquals(2, peers.nodes.size());
		assertEquals(new Host("127.0.0.1", b.port()), peers.nodes.get("BB9B").getHost());
		assertEquals(new Host("127.0.0.1", c.port()), peers.nodes.get("BB9C").getHost());
		assertEquals(5, seed.getPeersGeneration());
		assertEquals(List.of(), warnings());
	}

	@Test
	void collisionDoesNotAdvancePeersGenerationAndRepeats() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A", "[[BB9B,,[10.0.0.2:" + b.port() + "]],[BB9C,,[10.0.0.3:" + b.port() + "]]]");
		Node seed = node(definition().ipMap(Map.of("10.0.0.2", "127.0.0.1", "10.0.0.3", "127.0.0.1")), a);

		for (int tend = 1; tend <= 3; tend++) {
			Peers peers = new Peers(16);
			seed.refreshPeers(peers);

			assertTrue(peers.nodes.isEmpty());
			assertEquals(-1, seed.getPeersGeneration());
			assertEquals(1, peers.refreshCount);
			assertEquals(2 * tend, collisions().size());
		}
		assertEquals(0, b.accepted.get());
	}

	@Test
	void passThroughNewcomerOnLiveNodeEndpointCreatesNoNode() throws Exception {
		FakeNode live = server("BB9L");
		FakeNode a = server("BB9A", "[[BB9N,,[localhost:" + live.port() + "]]]");
		Node seed = node(definition().addressTranslator(PassThroughAddressTranslator.INSTANCE), a);
		Node liveNode = node(live);
		HashMap<String,Node> nodesMap = nodesMap();
		nodesMap.put(liveNode.getName(), liveNode);

		try {
			for (int tend = 1; tend <= 3; tend++) {
				Peers peers = new Peers(16);
				seed.refreshPeers(peers);
				nodes.addAll(peers.nodes.values());

				assertTrue(peers.nodes.isEmpty(), peers.nodes.keySet().toString());
				assertNoSharedName(nodesMap, peers);
				assertSame(liveNode, nodesMap.get("BB9L"));
				assertTrue(liveNode.isActive());
				assertEquals(-1, seed.getPeersGeneration());
				assertEquals(tend, collisions().size());
			}

			for (String message : collisions()) {
				assertNames(message, "BB9N", "BB9L", "localhost:" + live.port(), "pass-through");
			}
		}
		finally {
			nodesMap.remove(liveNode.getName());
		}
	}

	@Test
	void passThroughNewcomerOnRefreshingNodeEndpointCreatesNoNode() throws Exception {
		FakeNode a = server("BB9A", "[[BB9N,,[localhost:0]]]");
		a.setPeers("[[BB9N,,[localhost:" + a.port() + "]]]");
		Node seed = node(definition().addressTranslator(PassThroughAddressTranslator.INSTANCE), a);
		HashMap<String,Node> nodesMap = nodesMap();
		nodesMap.put(seed.getName(), seed);

		try {
			Peers peers = new Peers(16);
			seed.refreshPeers(peers);
			nodes.addAll(peers.nodes.values());

			assertTrue(peers.nodes.isEmpty(), peers.nodes.keySet().toString());
			assertNoSharedName(nodesMap, peers);
			assertSame(seed, nodesMap.get("BB9A"));
			assertTrue(seed.isActive());
			assertEquals(-1, seed.getPeersGeneration());
			assertEquals(1, collisions().size());
			assertNames(collisions().get(0), "BB9N", "BB9A", "localhost:" + a.port(), "pass-through");
		}
		finally {
			nodesMap.remove(seed.getName());
		}
	}

	@Test
	void passThroughPeersOnOneEndpointInOneReplyCreateNeither() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A", "[[BB9B,,[127.0.0.1:" + b.port() + "]],[BB9N,,[localhost:" + b.port() + "]]]");
		Node seed = node(definition().addressTranslator(PassThroughAddressTranslator.INSTANCE), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertTrue(peers.nodes.isEmpty(), peers.nodes.keySet().toString());
		assertEquals(-1, seed.getPeersGeneration());
		assertEquals(1, collisions().size());
		assertNames(collisions().get(0), "BB9N", "BB9B", "localhost:" + b.port(), "pass-through");
	}

	@Test
	void passThroughPeersOnOneEndpointInReverseOrderCreateNeither() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A", "[[BB9N,,[localhost:" + b.port() + "]],[BB9B,,[127.0.0.1:" + b.port() + "]]]");
		Node seed = node(definition().addressTranslator(PassThroughAddressTranslator.INSTANCE), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertTrue(peers.nodes.isEmpty(), peers.nodes.keySet().toString());
		assertEquals(-1, seed.getPeersGeneration());
		assertEquals(2, collisions().size());
		assertNames(collisions().get(0), "BB9N", "BB9B", "localhost:" + b.port(), "pass-through");
		assertNames(collisions().get(1), "BB9B", "BB9N", "127.0.0.1:" + b.port(), "pass-through");
	}

	@Test
	void passThroughPeerOnUnadvertisedNodeIsCreatedUnderItsName() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A", "[[BB9N,,[localhost:" + b.port() + "]]]");
		Node seed = node(definition().addressTranslator(PassThroughAddressTranslator.INSTANCE), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertEquals(List.of("BB9B"), new ArrayList<>(peers.nodes.keySet()));
		assertEquals(5, seed.getPeersGeneration());
		assertEquals(1, warnings().size());
		assertNames(warnings().get(0), "Peer node BB9N is different than actual node BB9B");
	}

	@Test
	void exceptionIsTranslationCollision() {
		AerospikeException.TranslationCollision tc = new AerospikeException.TranslationCollision(
			"BB9B", "BB9C", new Host("127.0.0.1", 3000), "static-map");

		assertEquals(ResultCode.TRANSLATION_COLLISION, tc.getResultCode());
		assertFalse(tc.getInDoubt());
		assertNames(tc.getMessage(), "BB9B", "BB9C", "127.0.0.1:3000", "static-map");
	}

	private static void assertNoSharedName(HashMap<String,Node> nodesMap, Peers peers) {
		for (String name : peers.nodes.keySet()) {
			assertFalse(nodesMap.containsKey(name), name);
		}
	}

	private static void assertNames(String message, String... parts) {
		for (String part : parts) {
			assertTrue(message.contains(part), message);
		}
	}

	/**
	 * Definition with an unreachable seed that does not fail when the cluster cannot connect.
	 */
	private static ClusterDefinition definition() {
		return new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(1000)
			.withSystemSettings(builder -> builder.refresh(ops -> ops.tendInterval(Duration.ofSeconds(5))));
	}

	/**
	 * The cluster's live nodes by name. The legacy test used {@code Cluster.nodesMap}.
	 */
	private HashMap<String,Node> nodesMap() {
		return cluster.getClusterTend().getNodesMap();
	}

	private FakeNode server(String name) throws IOException {
		return server(name, "[]");
	}

	private FakeNode server(String name, String peers) throws IOException {
		FakeNode server = new FakeNode(name, "5,3000," + peers);
		servers.add(server);
		return server;
	}

	private Node node(ClusterDefinition def, FakeNode server) throws Exception {
		cluster = def.connect();
		clearWarnings();
		return node(server);
	}

	private Node node(FakeNode server) throws Exception {
		InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port());
		NodeValidator nv = new NodeValidator();
		nv.name = server.name;
		nv.primaryHost = new Host("127.0.0.1", server.port());
		nv.primaryAddress = address;
		nv.primaryConn = new Connection(address, 1000, cluster);
		nv.version = new Version("8.1.0.0");
		Node node = new Node(cluster, nv);
		nodes.add(node);
		return node;
	}

	/**
	 * WARN messages logged on the test thread since the last {@link #clearWarnings()}.
	 */
	private List<String> warnings() {
		List<String> warnings = stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.startsWith(warnPrefix))
			.map(line -> line.substring(line.indexOf(" - ") + 3))
			.toList();
		return warnings.subList(Math.min(warningsCleared, warnings.size()), warnings.size());
	}

	private void clearWarnings() {
		warningsCleared = 0;
		warningsCleared = warnings().size();
	}

	private List<String> collisions() {
		List<String> list = new ArrayList<>();

		for (String message : warnings()) {
			if (message.contains("to the same address")) {
				list.add(message);
			}
		}
		assertEquals(warnings(), list);
		return list;
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}

	private static final class FakeNode implements AutoCloseable {
		final String name;
		final AtomicInteger accepted = new AtomicInteger();
		private volatile String peers;
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();

		FakeNode(String name, String peers) throws IOException {
			this.name = name;
			this.peers = peers;
			this.server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		void setPeers(String peers) {
			this.peers = "5,3000," + peers;
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
				accepted.incrementAndGet();

				synchronized (sockets) {
					sockets.add(socket);
				}
				Thread thread = new Thread(() -> serve(socket));
				thread.setDaemon(true);
				thread.start();
			}
		}

		private void serve(Socket socket) {
			try {
				DataInputStream in = new DataInputStream(socket.getInputStream());
				OutputStream out = socket.getOutputStream();
				byte[] header = new byte[8];

				while (true) {
					in.readFully(header);
					byte[] request = new byte[(int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL)];
					in.readFully(request);

					StringBuilder sb = new StringBuilder();

					for (String command : new String(request, StandardCharsets.UTF_8).split("\n")) {
						if (! command.isEmpty()) {
							sb.append(command).append('\t').append(value(command)).append('\n');
						}
					}

					byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
					byte[] response = new byte[8 + body.length];
					Buffer.longToBytes(body.length | (2L << 56) | (1L << 48), response, 0);
					System.arraycopy(body, 0, response, 8, body.length);
					out.write(response);
					out.flush();
				}
			}
			catch (EOFException eof) {
			}
			catch (IOException ioe) {
			}
		}

		private String value(String command) {
			switch (command) {
			case "node":
				return name;
			case "partition-generation":
			case "peers-generation":
				return "1";
			case "build":
				return "8.1.0.0";
			case "peers-clear-std":
				return peers;
			default:
				return "ok";
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
