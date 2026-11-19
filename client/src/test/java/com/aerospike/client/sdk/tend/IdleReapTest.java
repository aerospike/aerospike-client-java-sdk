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

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.command.Pool;
import com.aerospike.client.sdk.util.Version;

/**
 * U-12: idle connections are reaped under the AWS NLB idle timeout of 350 seconds, by default and,
 * with minConnsPerNode set, when maximumSocketIdleTimeOnCheckout is set below that timeout.
 */
class IdleReapTest {
	private static final long NLB_IDLE_TIMEOUT_SECS = 350;
	private static final int MAX_SOCKET_IDLE_UNDER_NLB = 340;

	private ServerSocket server;
	private Cluster cluster;
	private Node node;

	@BeforeEach
	void setUp() throws Exception {
		server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
	}

	@AfterEach
	void tearDown() throws Exception {
		if (node != null) {
			node.close();
		}

		if (cluster != null) {
			cluster.close();
		}
		server.close();
	}

	/**
	 * Disconnected cluster with the given minimum connections per node and checkout idle limit
	 * in seconds, and a node of it on a loopback listener, for pools to read the limits from.
	 */
	private Cluster cluster(int minConnsPerNode, int maxSocketIdleOnCheckout) throws Exception {
		cluster = definition(minConnsPerNode, Duration.ofSeconds(maxSocketIdleOnCheckout)).connect();

		InetSocketAddress address = new InetSocketAddress(server.getInetAddress(), server.getLocalPort());
		NodeValidator nv = new NodeValidator();
		nv.name = "BB9A";
		nv.primaryHost = new Host("127.0.0.1", server.getLocalPort());
		nv.primaryAddress = address;
		nv.primaryConn = new Connection(address, 1000, cluster);
		nv.version = new Version("8.1.0.0");
		node = new Node(cluster, nv);
		return cluster;
	}

	private static ClusterDefinition definition(int minConnsPerNode, Duration maxSocketIdleOnCheckout) {
		return new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder
				.connections(ops -> ops
					.minimumConnectionsPerNode(minConnsPerNode)
					.maximumConnectionsPerNode(4)
					.maximumSocketIdleTimeOnCheckout(maxSocketIdleOnCheckout)));
	}

	private static long idleFor(long secs) {
		return System.nanoTime() - TimeUnit.SECONDS.toNanos(secs);
	}

	private Pool reservePool() {
		Pool pool = new Pool(node, 2, 4);
		pool.incrTotal();
		pool.incrTotal();
		assertEquals(0, pool.excess());
		return pool;
	}

	@Test
	void defaultTrimLimitUnderNlbIdleTimeout() {
		Duration trim = SystemSettings.DEFAULT.getMaximumSocketIdleTime();

		assertEquals(Duration.ofSeconds(55), trim);
		assertTrue(trim.toSeconds() < NLB_IDLE_TIMEOUT_SECS);
		assertEquals(TimeUnit.SECONDS.toNanos(55), new ClusterDefinition("127.0.0.1", 1).getMaxSocketIdleNanosTrim());
	}

	@Test
	void defaultCheckoutLimitIsOff() {
		assertEquals(Duration.ZERO, SystemSettings.DEFAULT.getMaximumSocketIdleTimeOnCheckout());
		assertEquals(0, new ClusterDefinition("127.0.0.1", 1).getMaxSocketIdleNanosTran());
	}

	@Test
	void connectionAboveMinConnsPerNodeIsTrimmed() throws Exception {
		cluster(2, 0);
		Pool pool = new Pool(node, 2, 4);

		for (int i = 0; i < 3; i++) {
			pool.incrTotal();
		}

		assertEquals(1, pool.excess());
		assertFalse(isConnCurrentTrim(pool, idleFor(NLB_IDLE_TIMEOUT_SECS)));
	}

	@Test
	void reserveConnectionKeptWhenMaxSocketIdleIsZero() throws Exception {
		Cluster cluster = cluster(2, 0);
		reservePool();
		assertTrue(cluster.isConnCurrentTran(idleFor(NLB_IDLE_TIMEOUT_SECS)));
	}

	@Test
	void reserveConnectionDiscardedOnCheckoutWhenMaxSocketIdleUnderNlbTimeout() throws Exception {
		assertTrue(MAX_SOCKET_IDLE_UNDER_NLB < NLB_IDLE_TIMEOUT_SECS);

		Cluster cluster = cluster(2, MAX_SOCKET_IDLE_UNDER_NLB);
		reservePool();

		assertFalse(cluster.isConnCurrentTran(idleFor(NLB_IDLE_TIMEOUT_SECS)));
		assertFalse(cluster.isConnCurrentTran(idleFor(MAX_SOCKET_IDLE_UNDER_NLB + 1)));
		assertTrue(cluster.isConnCurrentTran(idleFor(MAX_SOCKET_IDLE_UNDER_NLB - 10)));
	}

	@Test
	void negativeCheckoutLimitRejected() {
		ClusterDefinition def = definition(2, Duration.ofSeconds(-1));

		AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
		assertTrue(ae.getMessage().contains("Invalid maximumSocketIdleTimeOnCheckout"), ae.getMessage());
	}

	private static boolean isConnCurrentTrim(Pool pool, long lastUsed) throws Exception {
		Method method = Pool.class.getDeclaredMethod("isConnCurrentTrim", long.class);
		method.setAccessible(true);
		return (boolean)method.invoke(pool, lastUsed);
	}
}
