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
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.discovery.GuardedResolverProvider;

/**
 * Peer validation of a host name that resolves to several addresses starts a connect to one address
 * at a time, 100 ms apart, and validates the first that completes.
 */
class StaggeredConnectTest {
	private static final String NODE = "BB9D000000000001";
	private static final long STAGGER = 100;
	private static final long MARGIN = 250;

	private InetAddress first;
	private InetAddress second;
	private final List<AutoCloseable> closeables = new ArrayList<>();
	private final List<String> hostnames = new ArrayList<>();
	private Cluster cluster;

	@BeforeEach
	void setUp() throws Exception {
		first = InetAddress.getByName("127.0.0.1");
		second = InetAddress.getByName("127.0.0.2");

		if (! canBind(second)) {
			second = InetAddress.getByName("::1");
		}
		assumeTrue(canBind(second), "Neither 127.0.0.2 nor ::1 can be bound locally");
	}

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}

		for (AutoCloseable closeable : closeables) {
			closeable.close();
		}

		for (String hostname : hostnames) {
			GuardedResolverProvider.unregister(hostname);
		}
	}

	@Test
	void firstAddressAnswersWithOneConnect() throws Throwable {
		Cluster cluster = cluster(1000, 250);
		int port = freePort();
		StubNodeServer live = server(first, port, NODE);
		StubNodeServer other = server(second, port, NODE);
		String hostname = hostname("first", first, second);

		NodeValidator nv = new NodeValidator();
		validate(nv, cluster, new Host(hostname, port));

		assertEquals(first, nv.primaryAddress.getAddress());
		// One connect from the staggered attempt, one for the validation.
		assertEquals(2, live.connections());
		assertEquals(0, other.connections());
	}

	@Test
	void hungAddressDelaysNextByStagger() throws Throwable {
		Cluster cluster = cluster(3000, 250);
		int port = freePort();
		InetAddress hung = hung(second, "192.0.2.1", port);
		StubNodeServer live = server(first, port, NODE);
		String hostname = hostname("hung", hung, first);

		NodeValidator nv = new NodeValidator();
		long begin = System.nanoTime();
		long elapsed = validate(nv, cluster, new Host(hostname, port));
		long started = TimeUnit.NANOSECONDS.toMillis(live.firstAccept() - begin);

		assertTrue(started >= STAGGER && started < STAGGER + MARGIN, "second address started at " + started + "ms");
		assertTrue(elapsed < 250 + MARGIN, "validateNode took " + elapsed + "ms");
		assertEquals(NODE, nv.name);
		assertEquals(first, nv.primaryAddress.getAddress());
	}

	@Test
	void everyAddressStartsWithinBound() throws Throwable {
		Cluster cluster = cluster(200, 1000);
		int port = freePort();
		InetAddress hung1 = hung(second, "192.0.2.1", port);
		InetAddress hung2 = hung(InetAddress.getByName("127.0.0.3"), "192.0.2.2", port);
		StubNodeServer live = server(first, port, NODE);
		String hostname = hostname("three", hung1, hung2, first);

		NodeValidator nv = new NodeValidator();
		long begin = System.nanoTime();
		validate(nv, cluster, new Host(hostname, port));
		long started = TimeUnit.NANOSECONDS.toMillis(live.firstAccept() - begin);

		// The delay is 200 / 3 ms, so the third address starts after two delays and before the bound.
		assertTrue(started >= 2 * (200 / 3) && started < 200, "third address started at " + started + "ms");
		assertEquals(first, nv.primaryAddress.getAddress());
	}

	@Test
	void refusedAddressStartsNextAtOnce() throws Throwable {
		Cluster cluster = cluster(1000, 250);
		int port = freePort();
		StubNodeServer live = server(second, port, NODE);
		String hostname = hostname("refused", first, second);

		NodeValidator nv = new NodeValidator();
		long begin = System.nanoTime();
		validate(nv, cluster, new Host(hostname, port));
		long started = TimeUnit.NANOSECONDS.toMillis(live.firstAccept() - begin);

		assertTrue(started < STAGGER / 2, "second address started at " + started + "ms");
		assertEquals(second, nv.primaryAddress.getAddress());
	}

	@Test
	void everyAddressHungFailsWithinBound() throws Throwable {
		Cluster cluster = cluster(3000, 250);
		int port = freePort();
		String hostname = hostname("dead", hung(first, "192.0.2.1", port), hung(second, "192.0.2.2", port));

		long begin = System.nanoTime();

		try {
			new NodeValidator().validateNode(cluster, new Host(hostname, port));
			fail("validateNode succeeded");
		}
		catch (AerospikeException.Connection e) {
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			assertTrue(elapsed < 250 + MARGIN, "validateNode took " + elapsed + "ms");
			assertTrue(e.getMessage().contains(hostname + ' ' + port), e.getMessage());
			assertTrue(e.getMessage().contains("2 addresses"), e.getMessage());
			assertTrue(e.getMessage().contains("250ms"), e.getMessage());
		}
	}

	@Test
	void lastValidAddressIsPreferred() throws Throwable {
		Cluster cluster = cluster(1000, 250);
		int port = freePort();
		server(first, port, NODE);
		server(second, port, NODE);
		String hostname = hostname("prefer", first, second);

		NodeValidator nv = new NodeValidator();
		validate(nv, cluster, new Host(hostname, port));
		InetAddress chosen = nv.primaryAddress.getAddress();
		InetAddress other = chosen.equals(first) ? second : first;

		GuardedResolverProvider.register(hostname, other, chosen);

		for (int i = 0; i < 10; i++) {
			nv = new NodeValidator();
			validate(nv, cluster, new Host(hostname, port));
			assertEquals(chosen, nv.primaryAddress.getAddress(), "call " + i);
		}
		assertEquals(10, GuardedResolverProvider.lookups(hostname));
	}

	@Test
	void singleAddressConnectsAsBefore() throws Throwable {
		Cluster cluster = cluster(600, 250);
		int port = freePort();
		server(first, port, NODE);
		String hostname = hostname("single", first);

		NodeValidator nv = new NodeValidator();
		validate(nv, cluster, new Host(hostname, port));
		assertEquals(first, nv.primaryAddress.getAddress());
		assertEquals(hostname, nv.hostname);
		assertEquals(1, GuardedResolverProvider.lookups(hostname));

		int hungPort = freePort();
		String hung = hostname("single-hung", hung(first, "192.0.2.1", hungPort));
		long elapsed = failedValidate(cluster, new Host(hung, hungPort));

		assertTrue(elapsed >= 600 - 50, "validateNode took " + elapsed + "ms, not the connect timeout");
		assertEquals(1, GuardedResolverProvider.lookups(hung));
	}

	@Test
	void ipLiteralConnectsAsBefore() throws Throwable {
		Cluster cluster = cluster(600, 250);
		int port = freePort();
		server(first, port, NODE);
		int lookups = GuardedResolverProvider.lookups();

		NodeValidator nv = new NodeValidator();
		GuardedResolverProvider.arm();

		try {
			validate(nv, cluster, new Host("127.0.0.1", port));
		}
		finally {
			GuardedResolverProvider.disarm();
		}
		assertEquals(first, nv.primaryAddress.getAddress());
		assertNull(nv.hostname);

		int hungPort = freePort();
		String literal = hung(first, "192.0.2.1", hungPort).getHostAddress();
		long elapsed = failedValidate(cluster, new Host(literal, hungPort));

		assertTrue(elapsed >= 600 - 50, "validateNode took " + elapsed + "ms, not the connect timeout");
		assertEquals(lookups, GuardedResolverProvider.lookups());
	}

	@Test
	void timeoutBelowTendIntervalBoundsWait() throws Throwable {
		Cluster cluster = cluster(400, 1000);
		int port = freePort();
		String hostname = hostname("timeout", hung(first, "192.0.2.1", port), hung(second, "192.0.2.2", port));

		long begin = System.nanoTime();

		try {
			new NodeValidator().validateNode(cluster, new Host(hostname, port));
			fail("validateNode succeeded");
		}
		catch (AerospikeException.Connection e) {
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			assertTrue(elapsed >= 400 - 50 && elapsed < 400 + MARGIN, "validateNode took " + elapsed + "ms");
			assertTrue(e.getMessage().contains("400ms"), e.getMessage());
		}
	}

	/**
	 * Cluster connected to a stub seed node, with the given tend timeout (the legacy connect timeout)
	 * and tend interval.
	 */
	private Cluster cluster(int timeout, int tendInterval) throws Exception {
		StubNodeServer seed = new StubNodeServer();
		closeables.add(seed);

		cluster = new ClusterDefinition("127.0.0.1", seed.port())
			.tendTimeout(timeout)
			.withSystemSettings(builder -> builder.refresh(ops -> ops.tendInterval(Duration.ofMillis(tendInterval))))
			.connect();
		return cluster;
	}

	private static long validate(NodeValidator nv, Cluster cluster, Host host) throws Throwable {
		long begin = System.nanoTime();
		nv.validateNode(cluster, host);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		nv.primaryConn.close();
		return elapsed;
	}

	private static long failedValidate(Cluster cluster, Host host) throws Throwable {
		long begin = System.nanoTime();

		try {
			new NodeValidator().validateNode(cluster, host);
			fail("validateNode succeeded");
		}
		catch (AerospikeException.Connection e) {
		}
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
	}

	private String hostname(String prefix, InetAddress... addresses) throws Exception {
		String hostname = prefix + '-' + System.nanoTime() + ".connect.test";
		GuardedResolverProvider.register(hostname, addresses);
		hostnames.add(hostname);
		return hostname;
	}

	private StubNodeServer server(InetAddress address, int port, String name) throws IOException {
		StubNodeServer server = new StubNodeServer(address, port, name);
		closeables.add(server);
		return server;
	}

	private InetAddress hung(InetAddress loopback, String testNet, int port) throws IOException {
		return HungAddress.hung(closeables, loopback, testNet, port);
	}

	/**
	 * Free port on both test addresses.
	 */
	private int freePort() throws IOException {
		for (int i = 0; i < 100; i++) {
			int port;

			try (ServerSocket socket = new ServerSocket(0, 1, second)) {
				port = socket.getLocalPort();
			}

			try (ServerSocket socket = new ServerSocket(port, 1, first)) {
				return port;
			}
			catch (BindException be) {
			}
		}
		throw new IOException("No port free on both " + first + " and " + second);
	}

	private static boolean canBind(InetAddress address) {
		try (ServerSocket socket = new ServerSocket(0, 1, address)) {
			return true;
		}
		catch (IOException ioe) {
			return false;
		}
	}
}
