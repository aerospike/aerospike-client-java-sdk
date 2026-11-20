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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.discovery.TranslationMissMode;

class PeerParserTest {
	private static final String PEERS = "peers-clear-std\t5,3000,[[BB9A,,[10.0.0.1:3001,10.0.0.9]],[BB9C,,[10.0.0.3]],[BB9B,,[10.0.0.2]]]\n";

	private static List<Peer> parse(TranslationMissMode mode) throws Exception {
		ClusterDefinition def = new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.ipMap(Map.of("10.0.0.1", "192.168.1.1", "10.0.0.2", "192.168.1.2"))
			.withSystemSettings(builder -> builder.translation(ops -> ops.onMiss(mode)));

		try (Cluster cluster = def.connect()) {
			List<Peer> peers = new ArrayList<>();
			PeerParser parser = PeerReply.parse(cluster, PEERS, peers);
			assertEquals(5, parser.generation);
			return peers;
		}
	}

	@Test
	void strictMissDropsOnlyMissedHost() throws Exception {
		List<Peer> peers = parse(TranslationMissMode.STRICT);

		assertEquals(3, peers.size());
		assertEquals("BB9A", peers.get(0).nodeName);
		assertEquals(List.of(new Host("192.168.1.1", 3001)), peers.get(0).hosts);
		assertEquals("BB9C", peers.get(1).nodeName);
		assertTrue(peers.get(1).hosts.isEmpty());
		assertEquals("BB9B", peers.get(2).nodeName);
		assertEquals(List.of(new Host("192.168.1.2", 3000)), peers.get(2).hosts);
	}

	@Test
	void passThroughMissKeepsAdvertisedHost() throws Exception {
		List<Peer> peers = parse(TranslationMissMode.PASS_THROUGH);

		assertEquals(3, peers.size());
		assertEquals(List.of(new Host("192.168.1.1", 3001), new Host("10.0.0.9", 3000)), peers.get(0).hosts);
		assertEquals(List.of(new Host("10.0.0.3", 3000)), peers.get(1).hosts);
		assertEquals(List.of(new Host("192.168.1.2", 3000)), peers.get(2).hosts);
	}
}
