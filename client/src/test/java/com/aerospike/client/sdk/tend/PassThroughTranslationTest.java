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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.discovery.Endpoint;
import com.aerospike.client.sdk.discovery.Endpoint.SourceList;
import com.aerospike.client.sdk.discovery.PassThroughAddressTranslator;
import com.aerospike.client.sdk.discovery.TranslationMissMode;
import com.aerospike.client.sdk.util.Version;

public class PassThroughTranslationTest {
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
	void strictNeverMisses() {
		cluster = definition(false, false).connect();
		ClusterTend tend = cluster.getClusterTend();

		for (SourceList sourceList : SourceList.values()) {
			for (String host : new String[] {"10.0.0.2", "2001:db8::10", "node2.priv.example.com"}) {
				Host result = tend.translate(PassThroughAddressTranslator.INSTANCE, TranslationMissMode.STRICT,
					new Endpoint("BB9", "db2.example.com", host, 3000, sourceList));

				assertEquals(host, result.name);
				assertEquals(3000, result.port);
				assertEquals("db2.example.com", result.tlsName);
			}
		}

		Host result = tend.translate(PassThroughAddressTranslator.INSTANCE, TranslationMissMode.STRICT,
			new Endpoint("BB9", null, "10.0.0.2", 3000, SourceList.ALTERNATE));
		assertNull(result.tlsName);
	}

	@Test
	void clusterTranslatesUnchangedUnderStrict() {
		ClusterDefinition def = definition(true, true).ipMap(Map.of("10.0.0.1", "192.168.1.1"));

		try (Cluster cluster = def.connect()) {
			ClusterTend tend = cluster.getClusterTend();
			Host host = tend.translateAddress("BB9", "tls1", "10.0.0.1", 31002);

			assertEquals("10.0.0.1", host.name);
			assertEquals(31002, host.port);
			assertEquals("tls1", host.tlsName);
			assertNull(tend.translateAddress("BB9", null, "10.0.0.1", 3000).tlsName);
			assertEquals("pass-through", tend.translatorName());
		}
	}

	@Test
	void sameHostAndPortCollisionNamesPassThrough() throws Exception {
		FakeNode b = server("BB9B", "peers-clear-std", "[]");
		FakeNode a = server("BB9A", "peers-clear-std",
			"[[BB9B,,[127.0.0.1:" + b.port() + "]],[BB9C,,[127.0.0.1:" + b.port() + "]]]");
		Node seed = node(definition(false, false), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);

		assertTrue(peers.nodes.isEmpty());
		assertEquals(-1, seed.getPeersGeneration());

		List<String> collisions = warnings();
		assertEquals(2, collisions.size(), collisions.toString());

		for (String message : collisions) {
			for (String part : new String[] {"BB9B", "BB9C", "127.0.0.1:" + b.port(), "pass-through", "same address"}) {
				assertTrue(message.contains(part), message);
			}
		}
	}

	@Test
	void alternateSharedHostPortPerNodeCreatesEveryPeer() throws Exception {
		FakeNode b = server("BB9B", "peers-clear-alt", "[]");
		FakeNode c = server("BB9C", "peers-clear-alt", "[]");
		FakeNode a = server("BB9A", "peers-clear-alt",
			"[[BB9B,,[127.0.0.1:" + b.port() + "]],[BB9C,,[127.0.0.1:" + c.port() + "]]]");
		a.reply("peers-clear-std", "5,3000,[[BB9B,,[10.0.0.2:3000]],[BB9C,,[10.0.0.3:3000]]]");
		Node seed = node(definition(true, true), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertEquals(2, peers.nodes.size());
		assertEquals(new Host("127.0.0.1", b.port()), peers.nodes.get("BB9B").getHost());
		assertEquals(new Host("127.0.0.1", c.port()), peers.nodes.get("BB9C").getHost());
		assertEquals(5, seed.getPeersGeneration());
		assertEquals(List.of(), warnings());
	}

	/**
	 * Pass-through translator with an unreachable seed that does not fail when the cluster cannot
	 * connect. On-miss is set through system settings, combined with the tend interval in one
	 * configurator because a second withSystemSettings call replaces the first.
	 */
	private static ClusterDefinition definition(boolean useServicesAlternate, boolean strict) {
		ClusterDefinition def = new ClusterDefinition("127.0.0.1", 1)
			.addressTranslator(PassThroughAddressTranslator.INSTANCE)
			.failIfNotConnected(false)
			.tendTimeout(1000)
			.withSystemSettings(builder -> {
				builder.refresh(ops -> ops.tendInterval(Duration.ofSeconds(5)));

				if (strict) {
					builder.translation(ops -> ops.onMiss(TranslationMissMode.STRICT));
				}
			});

		if (useServicesAlternate) {
			def.usingServicesAlternate();
		}
		return def;
	}

	private FakeNode server(String name, String peersCommand, String peers) throws IOException {
		FakeNode server = new FakeNode(name, peersCommand, "5,3000," + peers);
		servers.add(server);
		return server;
	}

	private Node node(ClusterDefinition def, FakeNode server) throws Exception {
		cluster = def.connect();
		clearWarnings();

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

	private static final class FakeNode implements AutoCloseable {
		final String name;
		private final Map<String,String> replies = new ConcurrentHashMap<>();
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();

		FakeNode(String name, String peersCommand, String peers) throws IOException {
			this.name = name;
			replies.put(peersCommand, peers);
			this.server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		void reply(String command, String value) {
			replies.put(command, value);
		}

		int port() {
			return server.getLocalPort();
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
			catch (IOException ioe) {
			}
		}

		private String value(String command) {
			String reply = replies.get(command);

			if (reply != null) {
				return reply;
			}

			switch (command) {
			case "node":
				return name;
			case "partition-generation":
			case "peers-generation":
				return "1";
			case "build":
				return "8.1.0.0";
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
