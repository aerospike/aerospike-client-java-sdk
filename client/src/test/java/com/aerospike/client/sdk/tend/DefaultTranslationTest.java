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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.StaticMapAddressTranslator;
import com.aerospike.client.sdk.discovery.TranslationMissMode;

public class DefaultTranslationTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Map<String,String> IP_MAP = Map.of("10.0.0.1", "192.168.1.1", "node2.internal", "node2.external",
		"2001:db8::1", "2001:db8::99");
	private static final String PEERS = "peers-clear-std\t5,3000,[[BB9A,tls1,[10.0.0.1:3001,10.0.0.9]]," +
		"[BB9B,,[node2.internal,node3.internal:3002]],[BB9C,,[[2001:db8::1]:3003,[2001:db8::2]]]]\n";
	private static final String[][] ADDRESSES = new String[][] {
		{"BB9A", "tls1", "10.0.0.1"}, {"BB9A", "tls1", "10.0.0.9"}, {"BB9B", null, "node2.internal"},
		{"BB9B", null, "node3.internal"}, {"BB9C", null, "2001:db8::1"}, {"BB9C", null, "2001:db8::2"}
	};

	private static ClusterDefinition definition() {
		return new ClusterDefinition(DEAD_SEED)
			.failIfNotConnected(false)
			.tendTimeout(100);
	}

	private static ClusterDefinition definition(TranslationMissMode mode) {
		return definition().withSystemSettings(builder -> builder.translation(ops -> ops.onMiss(mode)));
	}

	/**
	 * U-3: a cluster built in code with no translator, no seed candidate provider and no YAML
	 * translates through static-map and starts no thread besides tend.
	 */
	@Test
	void codeBuiltClusterDefaultsToStaticMap() throws Exception {
		ClusterDefinition def = definition().ipMap(IP_MAP);

		assertNull(def.getAddressTranslator());
		assertEquals("", SystemSettings.DEFAULT.getDiscovery().getProvider(), "default seed provider is static");
		assertNull(System.getenv("AEROSPIKE_SDK_CONFIG_URL"));

		Set<Thread> before = Thread.getAllStackTraces().keySet();

		try (Cluster cluster = def.connect()) {
			assertEquals(List.of("tend"), newThreadNames(before));

			ClusterTend tend = cluster.getClusterTend();
			assertTrue(addressTranslator(tend) instanceof StaticMapAddressTranslator);
			assertEquals("static-map", tend.translatorName());

			Host hit = tend.translateAddress("BB9A", "tls1", "10.0.0.1", 3000);
			assertEquals(new Host("192.168.1.1", 3000), hit);
			assertEquals("tls1", hit.tlsName);

			Host miss = tend.translateAddress("BB9A", "tls1", "10.0.0.9", 3001);
			assertEquals(new Host("10.0.0.9", 3001), miss);
			assertEquals("tls1", miss.tlsName);

			assertEquals(List.of("tend"), newThreadNames(before));
		}
	}

	/**
	 * U-7: ipMap and an explicit StaticMapAddressTranslator over the same map produce identical
	 * hosts for the same peers, hits and misses, for every on_miss mode.
	 */
	@Test
	void ipMapMatchesExplicitStaticMap() throws Exception {
		for (TranslationMissMode mode : TranslationMissMode.values()) {
			ClusterDefinition legacy = definition(mode).ipMap(IP_MAP);
			ClusterDefinition explicit = definition(mode).addressTranslator(new StaticMapAddressTranslator(IP_MAP));

			try (Cluster legacyCluster = legacy.connect();
				Cluster explicitCluster = explicit.connect()) {
				List<Peer> legacyPeers = PeerReply.parse(legacyCluster, PEERS);
				List<Peer> explicitPeers = PeerReply.parse(explicitCluster, PEERS);

				assertEquals(describe(legacyPeers), describe(explicitPeers));

				if (mode == TranslationMissMode.STRICT) {
					assertEquals(List.of("BB9A [192.168.1.1 3001 'tls1']", "BB9B [node2.external 3000 '']",
						"BB9C [2001:db8::99 3003 '']"), describe(legacyPeers));
				}
				else {
					assertEquals(List.of("BB9A [192.168.1.1 3001 'tls1', 10.0.0.9 3000 'tls1']",
						"BB9B [node2.external 3000 '', node3.internal 3002 '']",
						"BB9C [2001:db8::99 3003 '', 2001:db8::2 3000 '']"), describe(legacyPeers));
				}

				for (String[] a : ADDRESSES) {
					assertEquals(translate(legacyCluster, a), translate(explicitCluster, a));
				}
			}
		}
	}

	private static AddressTranslator addressTranslator(ClusterTend tend) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("addressTranslator");
		field.setAccessible(true);
		return (AddressTranslator)field.get(tend);
	}

	private static String translate(Cluster cluster, String[] address) {
		try {
			Host host = cluster.getClusterTend().translateAddress(address[0], address[1], address[2], 3000);
			return host + " " + host.tlsName;
		}
		catch (AerospikeException.TranslationMiss tm) {
			return tm.getMessage();
		}
	}

	private static List<String> describe(List<Peer> peers) {
		List<String> list = new ArrayList<>();

		for (Peer peer : peers) {
			List<String> hosts = new ArrayList<>();

			for (Host host : peer.hosts) {
				hosts.add(host + " '" + host.tlsName + "'");
			}
			list.add(peer.nodeName + ' ' + hosts);
		}
		return list;
	}

	private static List<String> newThreadNames(Set<Thread> before) {
		List<String> names = new ArrayList<>();

		for (Thread thread : new HashSet<>(Thread.getAllStackTraces().keySet())) {
			if (! before.contains(thread) && thread.isAlive()) {
				names.add(thread.getName());
			}
		}
		names.sort(null);
		return names;
	}
}
