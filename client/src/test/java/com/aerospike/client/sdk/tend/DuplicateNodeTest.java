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

import static com.aerospike.client.sdk.tend.TendDriver.stopTendThread;
import static com.aerospike.client.sdk.tend.TendDriver.tend;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.discovery.GuardedResolverProvider;

/**
 * Tends driven one at a time from the test thread never leave two nodes with one name. The tend
 * thread is stopped once the cluster is created.
 */
public class DuplicateNodeTest {
	private static final String X = "BB9E000000000001";
	private static final String Y = "BB9F000000000001";

	private InetAddress first;
	private InetAddress second;
	private InetAddress local;

	private final List<StubNodeServer> servers = new ArrayList<>();
	private final List<String> hostnames = new ArrayList<>();
	private Cluster cluster;

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}; INFO "Move node" messages are the legacy moves.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;

	@BeforeEach
	void setUp() throws Exception {
		first = InetAddress.getByName("127.0.0.1");
		second = InetAddress.getByName("127.0.0.2");
		local = localAddress();

		if (! canBind(second)) {
			second = InetAddress.getByName("::1");
		}
		assumeTrue(canBind(second), "Neither 127.0.0.2 nor ::1 can be bound locally");

		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}

		for (StubNodeServer server : servers) {
			server.close();
		}

		for (String hostname : hostnames) {
			GuardedResolverProvider.unregister(hostname);
		}

		if (originalErr != null) {
			System.setErr(originalErr);
		}
	}

	@Test
	void oneNodePerNameAfterSecondPhaseReplace() throws Exception {
		Cluster cluster = secondPhaseReplace();

		for (int i = 0; i < 3; i++) {
			tend(cluster);
			assertOneNodePerName(cluster);
		}
		assertEquals(2, cluster.getNodes().length);
	}

	@Test
	void secondPhaseReplaceRemovesNodeInSameTend() throws Exception {
		Cluster cluster = secondPhaseReplace();
		Node oldX = node(cluster, X);
		Node oldY = node(cluster, Y);

		tend(cluster);

		assertOneNodePerName(cluster);
		assertEquals(2, cluster.getNodes().length);
		assertFalse(oldX.isActive());
		assertFalse(oldY.isActive());

		Node newX = node(cluster, X);
		assertNotSame(oldX, newX);
		assertTrue(newX.isActive());
		assertEquals(first, newX.getAddress().getAddress());
		assertEquals(second, node(cluster, Y).getAddress().getAddress());
		assertEquals(1, moves().size(), moves().toString());
	}

	@Test
	void nodeNotInMapIsRemovedWithoutMove() throws Exception {
		String hostname = hostname("stale", first, second);
		StubNodeServer dead = pair(X, X)[1];
		int port = dead.port();

		cluster = definition(new Host(hostname, port)).connect();
		stopTendThread(cluster);

		Node current = node(cluster, X);
		Node stale = createNode(cluster, hostname, second, port);
		setNodes(cluster, current, stale);

		dead.close();
		tend(cluster);

		assertOneNodePerName(cluster);
		assertEquals(1, cluster.getNodes().length);
		assertSame(current, cluster.getNodes()[0]);
		assertFalse(stale.isActive());
		assertTrue(current.isActive());
		assertEquals(List.of(), moves());
	}

	@Test
	void sameNameRemovalsBothApplied() throws Exception {
		String hostname = hostname("twice", first, second);
		StubNodeServer dead = pair(X, X)[0];
		int port = dead.port();

		cluster = definition(new Host(hostname, port)).connect();
		stopTendThread(cluster);

		Node current = node(cluster, X);
		dead.close();

		for (int i = 0; i < 2; i++) {
			tend(cluster);
			assertSame(current, node(cluster, X));
		}

		Node stale = createNode(cluster, hostname, second, port);
		setNodes(cluster, current, stale);
		tend(cluster);

		assertOneNodePerName(cluster);
		assertEquals(1, cluster.getNodes().length);
		assertFalse(current.isActive());
		assertFalse(stale.isActive());

		Node moved = node(cluster, X);
		assertTrue(moved.isActive());
		assertEquals(second, moved.getAddress().getAddress());
		assertEquals(1, moves().size(), moves().toString());
	}

	/**
	 * Node Y on a two-address loopback hostname lists node X under a hostname that resolves to a
	 * local non-loopback address. Y's first address dies, and once Y has failed three tends it moves
	 * to its second address in the tend where X drops its connections. The moved Y lists X on another
	 * port of a loopback address, so the moved Y's peers replace X in the same tend. Returns before
	 * that tend.
	 */
	private Cluster secondPhaseReplace() throws Exception {
		assumeTrue(local != null, "No non-loopback IPv4 address can be bound locally");

		String hostX = hostname("x", local);
		String hostY = hostname("y", first, second);
		StubNodeServer x = server(local, 0, X);
		StubNodeServer x2 = server(first, 0, X);
		int portX = x.port();
		StubNodeServer[] pair = pair(Y, Y);
		StubNodeServer y1 = pair[0];
		StubNodeServer y2 = pair[1];
		int portY = y1.port();

		// X owns every partition, so it is kept while the failing Y does not list it.
		y1.setOwnsPartitions(false);
		y2.setOwnsPartitions(false);
		y1.setPeers("1,3000,[[" + X + ",,[" + hostX + ':' + portX + "]]]");
		y2.setPeers("1,3000,[[" + X + ",,[127.0.0.1:" + x2.port() + "]]]");
		x.setPeers("1,3000,[[" + Y + ",,[" + hostY + ':' + portY + "]]]");
		x2.setPeers("1,3000,[[" + Y + ",,[" + hostY + ':' + portY + "]]]");

		cluster = definition(new Host(hostY, portY)).connect();
		stopTendThread(cluster);

		assertEquals(2, cluster.getNodes().length);
		Node oldX = node(cluster, X);
		Node oldY = node(cluster, Y);
		assertEquals(local, oldX.getAddress().getAddress());
		assertEquals(first, oldY.getAddress().getAddress());

		y1.close();

		for (int i = 0; i < 2; i++) {
			tend(cluster);
			assertSame(oldX, node(cluster, X));
			assertSame(oldY, node(cluster, Y));
		}
		assertEquals(List.of(), moves());

		x.closeConnections();
		return cluster;
	}

	private static void assertOneNodePerName(Cluster cluster) {
		Node[] nodes = cluster.getNodes();
		Set<String> names = new HashSet<>();

		for (Node node : nodes) {
			assertTrue(names.add(node.getName()), "Two nodes named " + node.getName() + " in " + Arrays.toString(nodes));
		}

		HashMap<String,Node> nodesMap = cluster.getClusterTend().getNodesMap();

		for (Node node : nodes) {
			assertSame(node, nodesMap.get(node.getName()));
		}
		assertEquals(nodes.length, nodesMap.size());
	}

	private static ClusterDefinition definition(Host seed) {
		return new ClusterDefinition(seed).tendTimeout(500);
	}

	private String hostname(String prefix, InetAddress... addresses) throws Exception {
		String hostname = prefix + '-' + System.nanoTime() + ".duplicate.test";
		GuardedResolverProvider.register(hostname, addresses);
		hostnames.add(hostname);
		return hostname;
	}

	private StubNodeServer server(InetAddress address, int port, String name) throws IOException {
		StubNodeServer server = new StubNodeServer(address, port, name);
		servers.add(server);
		return server;
	}

	/**
	 * INFO "Move node" messages logged so far. slf4j-simple puts the cluster key-value pair
	 * ({@code aerospike.cluster=...}) before the message.
	 */
	private List<String> moves() {
		return stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.contains("] INFO "))
			.map(line -> line.substring(line.indexOf(" - ") + 3))
			.filter(message -> message.contains("Move node "))
			.map(message -> message.substring(message.indexOf("Move node ")))
			.toList();
	}

	private static Node node(Cluster cluster, String name) {
		Node found = null;

		for (Node node : cluster.getNodes()) {
			if (node.getName().equals(name)) {
				assertTrue(found == null, "Two nodes named " + name);
				found = node;
			}
		}
		return found;
	}

	/**
	 * Node for the given hostname alias, as {@code Node.createNode} builds it.
	 */
	private static Node createNode(Cluster cluster, String hostname, InetAddress address, int port) throws Exception {
		NodeValidator nv = new NodeValidator();
		nv.validateAlias(cluster, new Host(hostname, port), address, cluster.getClusterDefinition().getTendTimeout());
		Node node = new Node(cluster, nv);
		node.createMinConnections();
		return node;
	}

	private static void setNodes(Cluster cluster, Node... nodes) throws Exception {
		Field field = Cluster.class.getDeclaredField("nodes");
		field.setAccessible(true);
		field.set(cluster, nodes);
	}

	/**
	 * Servers with the given names on one port of the first and the second address. A port taken on
	 * the second address is replaced by another.
	 */
	private StubNodeServer[] pair(String firstName, String secondName) throws IOException {
		for (int i = 0; i < 100; i++) {
			StubNodeServer one = server(first, 0, firstName);

			try {
				return new StubNodeServer[] {one, server(second, one.port(), secondName)};
			}
			catch (BindException be) {
				servers.remove(one);
				one.close();
			}
		}
		throw new IOException("No port free on both " + first + " and " + second);
	}

	private static InetAddress localAddress() throws SocketException {
		for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
			if (! ni.isUp()) {
				continue;
			}

			for (InetAddress address : Collections.list(ni.getInetAddresses())) {
				if (address instanceof Inet4Address && ! address.isLoopbackAddress() && ! address.isLinkLocalAddress() &&
					canBind(address)) {
					return address;
				}
			}
		}
		return null;
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
