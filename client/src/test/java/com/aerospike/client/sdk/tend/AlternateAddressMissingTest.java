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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.aerospike.client.sdk.discovery.Endpoint;
import com.aerospike.client.sdk.util.Version;

public class AlternateAddressMissingTest {
	private final List<FakeNode> servers = new ArrayList<>();
	private final List<Node> nodes = new ArrayList<>();
	private Cluster cluster;

	/**
	 * slf4j-simple writes each log line, prefixed with {@code [thread] LEVEL}, to the current
	 * {@code System.err}. ERROR lines logged on the test thread are the legacy test's errors; the
	 * tend thread's are ignored.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;
	private String errorPrefix;
	private int errorsCleared;

	@BeforeEach
	void setUp() {
		errorPrefix = "[" + Thread.currentThread().getName() + "] ERROR ";
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
	void emptyServiceAltRejectsNode() throws Exception {
		FakeNode b = server("BB9B");
		cluster = connect(true);

		AerospikeException.AlternateAddressMissing e = assertThrows(AerospikeException.AlternateAddressMissing.class,
			() -> new NodeValidator().validateNode(cluster, new Host("127.0.0.1", b.port())));

		assertNames(e.getMessage(), "BB9B", "127.0.0.1 " + b.port(), "useServicesAlternate", "alternate-access-address");
		assertTrue(b.requests().get(0).containsAll(List.of("node", "service-clear-alt")));
	}

	@Test
	void nonEmptyServiceAltPasses() throws Throwable {
		FakeNode b = server("BB9B");
		b.serviceAlt = "127.0.0.1:" + b.port();
		cluster = connect(true);

		NodeValidator nv = new NodeValidator();
		nv.validateNode(cluster, new Host("127.0.0.1", b.port()));
		nv.primaryConn.close();

		assertEquals("BB9B", nv.name);
		assertEquals(new Host("127.0.0.1", b.port()), nv.primaryHost);
	}

	@Test
	void flagOffSendsNoAddressCommand() throws Throwable {
		FakeNode b = server("BB9B");
		cluster = connect(false);

		NodeValidator nv = new NodeValidator();
		nv.validateNode(cluster, new Host("127.0.0.1", b.port()));
		nv.primaryConn.close();

		assertEquals("BB9B", nv.name);

		for (List<String> request : b.requests()) {
			for (String command : request) {
				assertFalse(command.startsWith("service-") || command.startsWith("peers-"), command);
			}
		}
	}

	@Test
	void loadBalancerDetectionSharesAddressCommand() throws Throwable {
		InetAddress any = InetAddress.getByName("0.0.0.0");
		FakeNode on = server("BB9A");
		on.serviceAlt = "0.0.0.0:" + on.port();
		FakeNode off = server("BB9B");
		off.serviceStd = "0.0.0.0:" + off.port();

		cluster = connect(true);
		new NodeValidator().seedNode(cluster, new Host(any.getHostAddress(), on.port()), null).close();
		cluster.close();
		cluster = connect(false);
		new NodeValidator().seedNode(cluster, new Host(any.getHostAddress(), off.port()), null).close();

		assertEquals(off.requests().size(), on.requests().size());
		assertEquals(1, count(on.requests(), "service-clear-alt"));
		assertEquals(0, count(on.requests(), "service-clear-std"));
		assertEquals(1, count(off.requests(), "service-clear-std"));
		assertEquals(0, count(off.requests(), "service-clear-alt"));
	}

	@Test
	void seedWithoutAlternateFailsInit() throws Exception {
		FakeNode a = server("BB9A");
		ClusterDefinition def = definition(true, new Host("127.0.0.1", a.port())).failIfNotConnected(true);

		AerospikeException.AlternateAddressMissing e = assertThrows(AerospikeException.AlternateAddressMissing.class,
			def::connect);

		assertEquals(ResultCode.CLIENT_ERROR, e.getResultCode());
		assertFalse(e.getInDoubt());
		assertNames(e.getMessage(), "BB9A", "useServicesAlternate", "alternate-access-address", "set useServicesAlternate to false");
	}

	@Test
	void seedWithoutAlternateLogsError() throws Exception {
		FakeNode a = server("BB9A");
		cluster = definition(true, new Host("127.0.0.1", a.port())).connect();

		assertEquals(0, cluster.getNodes().length);
		assertEquals(1, errors().size(), stderr.toString(StandardCharsets.UTF_8));
		assertNames(errors().get(0), "BB9A", "useServicesAlternate", "alternate-access-address");
	}

	@Test
	void omittedPeerIsLoggedAndRetried() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A");
		a.peersAlt = "5,3000,[]";
		a.peersStd = "5,3000,[[BB9B,,[10.0.0.2:" + b.port() + "]]]";
		Node seed = node(true, a);

		for (int tend = 1; tend <= 2; tend++) {
			Peers peers = new Peers(16);
			seed.refreshPeers(peers);

			assertTrue(peers.nodes.isEmpty());
			assertEquals(-1, seed.getPeersGeneration());
			assertEquals(tend, errors().size());
		}
		assertEquals(0, b.accepted());
		assertNames(errors().get(0), "BB9B", "useServicesAlternate", "alternate-access-address");
		assertEquals(List.of(List.of("peers-clear-std", "peers-clear-alt"), List.of("peers-clear-std", "peers-clear-alt")),
			a.requests());
	}

	@Test
	void listedPeerWithNoHostsIsLogged() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode c = server("BB9C");
		c.serviceAlt = "127.0.0.1:" + c.port();
		FakeNode a = server("BB9A");
		a.peersAlt = "5,3000,[[BB9B,,[]],[BB9C,,[127.0.0.1:" + c.port() + "]]]";
		a.peersStd = "5,3000,[[BB9B,,[10.0.0.2]],[BB9C,,[10.0.0.3]]]";
		Node seed = node(true, a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertEquals(List.of("BB9C"), new ArrayList<>(peers.nodes.keySet()));
		assertEquals(-1, seed.getPeersGeneration());
		assertEquals(0, b.accepted());
		assertEquals(1, errors().size());
		assertNames(errors().get(0), "BB9B", "useServicesAlternate");
	}

	@Test
	void peerWithoutOwnAlternateIsNotAdded() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A");
		a.peersAlt = "5,3000,[[BB9B,,[127.0.0.1:" + b.port() + "]]]";
		a.peersStd = "5,3000,[[BB9B,,[10.0.0.2]]]";
		Node seed = node(true, a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);

		assertTrue(peers.nodes.isEmpty());
		assertEquals(-1, seed.getPeersGeneration());
		assertEquals(1, errors().size());
		assertNames(errors().get(0), "127.0.0.1 " + b.port(), "BB9B", "useServicesAlternate");
	}

	@Test
	void flagOffPeersUnchanged() throws Exception {
		FakeNode b = server("BB9B");
		FakeNode a = server("BB9A");
		a.peersStd = "5,3000,[[BB9B,,[127.0.0.1:" + b.port() + "]]]";
		Node seed = node(false, a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertEquals(List.of("BB9B"), new ArrayList<>(peers.nodes.keySet()));
		assertEquals(5, seed.getPeersGeneration());
		assertEquals(List.of(List.of("peers-clear-std")), a.requests());
		assertEquals(List.of(), errors());
	}

	@Test
	void exceptionIsClientError() {
		AerospikeException.AlternateAddressMissing e = new AerospikeException.AlternateAddressMissing("BB9B", null);

		assertEquals(ResultCode.CLIENT_ERROR, e.getResultCode());
		assertFalse(e.getInDoubt());
		assertNames(e.getMessage(), "BB9B", "useServicesAlternate", "alternate-access-address");
	}

	@Test
	void strictMissNamesSourceList() {
		AerospikeException.TranslationMiss alt = new AerospikeException.TranslationMiss(
			new Endpoint("BB9B", null, "10.0.0.2", 3000, Endpoint.SourceList.ALTERNATE), "static-map");
		AerospikeException.TranslationMiss std = new AerospikeException.TranslationMiss(
			new Endpoint("BB9B", null, "10.0.0.2", 3000, Endpoint.SourceList.STANDARD), "static-map");

		assertNames(alt.getMessage(), "BB9B", "10.0.0.2:3000", "in the alternate list");
		assertNames(std.getMessage(), "BB9B", "10.0.0.2:3000", "in the standard list");
	}

	private static void assertNames(String message, String... parts) {
		for (String part : parts) {
			assertTrue(message.contains(part), message);
		}
	}

	private static int count(List<List<String>> requests, String command) {
		int count = 0;

		for (List<String> request : requests) {
			for (String c : request) {
				if (c.equals(command)) {
					count++;
				}
			}
		}
		return count;
	}

	private static ClusterDefinition definition(boolean useServicesAlternate, Host seed) {
		ClusterDefinition def = new ClusterDefinition(seed)
			.failIfNotConnected(false)
			.tendTimeout(1000)
			.withSystemSettings(builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofSeconds(5))));

		if (useServicesAlternate) {
			def.usingServicesAlternate();
		}
		return def;
	}

	private static Cluster connect(boolean useServicesAlternate) {
		return definition(useServicesAlternate, new Host("127.0.0.1", 1)).connect();
	}

	private FakeNode server(String name) throws IOException {
		FakeNode server = new FakeNode(name);
		servers.add(server);
		return server;
	}

	private Node node(boolean useServicesAlternate, FakeNode server) throws Exception {
		cluster = connect(useServicesAlternate);
		clearErrors();

		InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port());
		NodeValidator nv = new NodeValidator();
		nv.name = server.name;
		nv.primaryHost = new Host("127.0.0.1", server.port());
		nv.primaryAddress = address;
		nv.primaryConn = new Connection(address, 1000, cluster);
		nv.version = new Version("8.1.0.0");
		Node node = new Node(cluster, nv);
		nodes.add(node);
		server.clearRequests();
		return node;
	}

	private List<String> errors() {
		List<String> errors = stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.startsWith(errorPrefix))
			.toList();
		return errors.subList(Math.min(errorsCleared, errors.size()), errors.size());
	}

	private void clearErrors() {
		errorsCleared = 0;
		errorsCleared = errors().size();
	}

	private static final class FakeNode implements AutoCloseable {
		final String name;
		volatile String peersStd = "5,3000,[]";
		volatile String peersAlt = "5,3000,[]";
		volatile String serviceStd = "";
		volatile String serviceAlt = "";
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();
		private final List<List<String>> requests = new ArrayList<>();
		private int accepted;

		FakeNode(String name) throws IOException {
			this.name = name;
			this.server = new ServerSocket(0, 16);

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		synchronized int accepted() {
			return accepted;
		}

		synchronized List<List<String>> requests() {
			return new ArrayList<>(requests);
		}

		synchronized void clearRequests() {
			requests.clear();
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

				synchronized (this) {
					accepted++;
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

					List<String> commands = new ArrayList<>();

					for (String command : new String(request, StandardCharsets.UTF_8).split("\n")) {
						if (! command.isEmpty()) {
							commands.add(command);
						}
					}

					synchronized (this) {
						requests.add(commands);
					}

					StringBuilder sb = new StringBuilder();

					for (String command : commands) {
						sb.append(command).append('\t').append(value(command)).append('\n');
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
			Map<String,String> values = new HashMap<>();
			values.put("node", name);
			values.put("partition-generation", "1");
			values.put("peers-generation", "1");
			values.put("build", "8.1.0.0");
			values.put("peers-clear-std", peersStd);
			values.put("peers-clear-alt", peersAlt);
			values.put("service-clear-std", serviceStd);
			values.put("service-clear-alt", serviceAlt);
			return values.getOrDefault(command, "ok");
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
