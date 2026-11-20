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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.Endpoint;

public class ClusterViewChangeTest {
	private final Map<String,String> view = new ConcurrentHashMap<>();
	private final Map<String,FakeNode> servers = new ConcurrentHashMap<>();
	private final AtomicInteger generation = new AtomicInteger(1);
	private Cluster cluster;

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}; WARN messages from every thread are the legacy warnings.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;

	final class NodeNameTranslator implements AddressTranslator {
		final List<Integer> viewChanges = new CopyOnWriteArrayList<>();
		volatile boolean fail;

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			FakeNode server = servers.get(advertised.nodeName);
			return (server != null)? Optional.of(new Host("127.0.0.1", server.port())) : Optional.empty();
		}

		@Override
		public void onClusterViewChanged(int viewChange) {
			viewChanges.add(viewChange);

			if (fail) {
				throw new IllegalStateException("cache flush failed");
			}
		}
	}

	@BeforeEach
	void setUp() {
		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}

		for (FakeNode server : servers.values()) {
			server.close();
		}
		System.setErr(originalErr);
	}

	@Test
	void viewChangeTrackedOnlyWhenTranslatorOverrides() {
		try (Cluster cluster = offline().connect()) {
			assertFalse(cluster.getClusterTend().getNotifyViewChange());
		}

		try (Cluster cluster = offline().addressTranslator(advertised -> Optional.empty()).connect()) {
			assertFalse(cluster.getClusterTend().getNotifyViewChange());
		}

		AddressTranslator overrides = new AddressTranslator() {
			@Override
			public Optional<Host> translate(Endpoint advertised) {
				return Optional.empty();
			}

			@Override
			public void onClusterViewChanged(int viewChange) {
			}
		};

		try (Cluster cluster = offline().addressTranslator(overrides).connect()) {
			assertTrue(cluster.getClusterTend().getNotifyViewChange());
		}
	}

	@Test
	void viewChangeCountsNodeAddRemoveAndAddressChange() throws Exception {
		NodeNameTranslator translator = new NodeNameTranslator();
		start(translator);

		assertEquals(2, cluster.getNodes().length);
		awaitTends(3);
		assertEquals(List.of(), translator.viewChanges);

		addNode("BB9C", "10.0.0.3:3000");
		await(() -> cluster.getNodes().length == 3);
		await(() -> translator.viewChanges.size() == 1);
		assertEquals(List.of(1), translator.viewChanges);

		int peersRequests = servers.get("BB9A").requests("peers-clear-std");
		generation.incrementAndGet();
		await(() -> servers.get("BB9A").requests("peers-clear-std") > peersRequests);
		awaitTends(3);
		assertEquals(List.of(1), translator.viewChanges);

		view.remove("BB9C");
		generation.incrementAndGet();
		await(() -> cluster.getNodes().length == 2);
		await(() -> translator.viewChanges.size() == 2);
		assertEquals(List.of(1, 2), translator.viewChanges);

		view.put("BB9B", "10.0.0.22:3000");
		generation.incrementAndGet();
		await(() -> translator.viewChanges.size() == 3);
		assertEquals(List.of(1, 2, 3), translator.viewChanges);
		assertEquals(2, cluster.getNodes().length);

		awaitTends(3);
		assertEquals(List.of(1, 2, 3), translator.viewChanges);
		assertEquals(List.of(), viewWarnings());
	}

	@Test
	void viewChangeExceptionDoesNotStopTend() throws Exception {
		NodeNameTranslator translator = new NodeNameTranslator();
		translator.fail = true;
		start(translator);

		addNode("BB9C", "10.0.0.3:3000");
		await(() -> cluster.getNodes().length == 3);
		await(() -> translator.viewChanges.size() == 1);

		view.remove("BB9C");
		generation.incrementAndGet();
		await(() -> cluster.getNodes().length == 2);
		await(() -> translator.viewChanges.size() == 2);
		assertEquals(List.of(1, 2), translator.viewChanges);

		List<String> logged = viewWarnings();
		assertEquals(2, logged.size(), logged.toString());

		for (String message : logged) {
			assertTrue(message.contains(NodeNameTranslator.class.getName()), message);
			assertTrue(message.contains("cache flush failed"), message);
		}
	}

	private static ClusterDefinition offline() {
		return new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(100);
	}

	private void start(NodeNameTranslator translator) throws Exception {
		server("BB9A", "10.0.0.1:3000");
		server("BB9B", "10.0.0.2:3000");

		cluster = new ClusterDefinition("127.0.0.1", servers.get("BB9A").port())
			.tendTimeout(1000)
			.addressTranslator(translator)
			.withSystemSettings(builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofMillis(250))))
			.connect();
	}

	private void addNode(String name, String address) throws IOException {
		server(name, address);
		generation.incrementAndGet();
	}

	private void server(String name, String address) throws IOException {
		servers.put(name, new FakeNode(name));
		view.put(name, address);
	}

	private void awaitTends(int count) throws InterruptedException {
		int target = servers.get("BB9A").requests("peers-generation") + count;
		await(() -> servers.get("BB9A").requests("peers-generation") >= target);
	}

	/**
	 * WARN messages logged so far that mention {@code onClusterViewChanged}.
	 */
	private List<String> viewWarnings() {
		return stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.contains("] WARN ") && line.contains("onClusterViewChanged"))
			.map(line -> line.substring(line.indexOf(" - ") + 3))
			.toList();
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}

	private final class FakeNode implements AutoCloseable {
		final String name;
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();
		private final Map<String,AtomicInteger> requests = new ConcurrentHashMap<>();

		FakeNode(String name) throws IOException {
			this.name = name;
			this.server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		int requests(String command) {
			AtomicInteger count = requests.get(command);
			return (count != null)? count.get() : 0;
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
							requests.computeIfAbsent(command, k -> new AtomicInteger()).incrementAndGet();
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
				return "1";
			case "peers-generation":
				return String.valueOf(generation.get());
			case "build":
				return "8.1.0.0";
			case "replicas":
				return "";
			case "peers-clear-std":
				return peers();
			default:
				return "ok";
			}
		}

		private String peers() {
			StringBuilder sb = new StringBuilder();
			sb.append(generation.get()).append(",3000,[");
			boolean comma = false;

			for (Map.Entry<String,String> entry : new TreeMap<>(view).entrySet()) {
				if (entry.getKey().equals(name)) {
					continue;
				}

				if (comma) {
					sb.append(',');
				}
				comma = true;
				sb.append('[').append(entry.getKey()).append(",,[").append(entry.getValue()).append("]]");
			}
			return sb.append(']').toString();
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
