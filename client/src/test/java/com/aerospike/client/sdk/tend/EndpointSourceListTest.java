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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.Endpoint;
import com.aerospike.client.sdk.discovery.PortMapAddressTranslator;
import com.aerospike.client.sdk.discovery.TranslationMissMode;

/**
 * U-13: {@link PeerParser} sets {@link Endpoint#sourceList} from useServicesAlternate, and a
 * port-map keyed from the wrong list is detectable.
 */
class EndpointSourceListTest {
	private static final String STD = "peers-clear-std\t5,3000,[[BB9A,,[10.0.0.1]],[BB9B,,[10.0.0.2:3001]]]\n";
	private static final String ALT = "peers-clear-alt\t5,3000,[[BB9A,,[203.0.113.1:30001]],[BB9B,,[203.0.113.2:30002]]]\n";
	private static final Map<String,String> STD_PORT_MAP = Map.of("10.0.0.1:3000", "lb.example.com:31001",
		"10.0.0.2:3001", "lb.example.com:31002");

	private static ClusterDefinition definition(boolean useServicesAlternate, AddressTranslator translator) {
		ClusterDefinition def = new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.addressTranslator(translator);

		if (useServicesAlternate) {
			def.usingServicesAlternate();
		}
		return def;
	}

	private static ClusterDefinition strict(boolean useServicesAlternate, AddressTranslator translator) {
		return definition(useServicesAlternate, translator)
			.withSystemSettings(builder -> builder.translation(ops -> ops.onMiss(TranslationMissMode.STRICT)));
	}

	private static List<Endpoint> endpoints(boolean useServicesAlternate, String reply) throws Exception {
		List<Endpoint> seen = new ArrayList<>();
		AddressTranslator recording = advertised -> {
			seen.add(advertised);
			return Optional.of(new Host(advertised.host, advertised.tlsName, advertised.port));
		};

		try (Cluster cluster = definition(useServicesAlternate, recording).connect()) {
			PeerReply.parse(cluster, reply);
		}
		return seen;
	}

	@Test
	void standardFlagSetsStandardSourceList() throws Exception {
		List<Endpoint> seen = endpoints(false, STD);

		assertEquals(2, seen.size());
		assertEquals("BB9A 10.0.0.1:3000 STANDARD", seen.get(0).toString());
		assertEquals("BB9B 10.0.0.2:3001 STANDARD", seen.get(1).toString());
	}

	@Test
	void alternateFlagSetsAlternateSourceList() throws Exception {
		List<Endpoint> seen = endpoints(true, STD + ALT);

		assertEquals(2, seen.size());
		assertEquals("BB9A 203.0.113.1:30001 ALTERNATE", seen.get(0).toString());
		assertEquals("BB9B 203.0.113.2:30002 ALTERNATE", seen.get(1).toString());
	}

	@Test
	void portMapKeyedFromStandardListMissesAlternateEndpoints() throws Exception {
		try (Cluster cluster = strict(false, new PortMapAddressTranslator(STD_PORT_MAP)).connect()) {
			List<Peer> peers = PeerReply.parse(cluster, STD);

			assertEquals(List.of(new Host("lb.example.com", 31001)), peers.get(0).hosts);
			assertEquals(List.of(new Host("lb.example.com", 31002)), peers.get(1).hosts);
		}

		try (Cluster cluster = strict(true, new PortMapAddressTranslator(STD_PORT_MAP)).connect()) {
			List<String> warnings = new ArrayList<>();
			List<Peer> peers = parse(cluster, STD + ALT, warnings);

			assertEquals(2, peers.size());
			assertTrue(peers.get(0).hosts.isEmpty());
			assertTrue(peers.get(1).hosts.isEmpty());
			assertEquals(2, warnings.size(), warnings.toString());
			assertNames(warnings.get(0), "port-map", "BB9A", "203.0.113.1:30001", "in the alternate list");
			assertNames(warnings.get(1), "port-map", "BB9B", "203.0.113.2:30002", "in the alternate list");
		}
	}

	@Test
	void passThroughMissOnWrongSourceListWarnsOncePerNode() throws Exception {
		try (Cluster cluster = definition(true, new PortMapAddressTranslator(STD_PORT_MAP)).connect()) {
			List<String> warnings = new ArrayList<>();
			List<Peer> peers = parse(cluster, STD + ALT, warnings);

			assertEquals(List.of(new Host("203.0.113.1", 30001)), peers.get(0).hosts);
			assertEquals(List.of(new Host("203.0.113.2", 30002)), peers.get(1).hosts);
			assertEquals(2, warnings.size(), warnings.toString());
			assertNames(warnings.get(0), "port-map", "BB9A", "203.0.113.1:30001", "in the alternate list", "pass_through");
			assertNames(warnings.get(1), "port-map", "BB9B", "203.0.113.2:30002", "in the alternate list", "pass_through");

			List<String> again = new ArrayList<>();
			parse(cluster, STD + ALT, again);
			assertEquals(List.of(), again);

			List<String> moved = new ArrayList<>();
			parse(cluster, STD + ALT.replace("203.0.113.2:30002", "203.0.113.2:30003"), moved);
			assertEquals(1, moved.size(), moved.toString());
			assertNames(moved.get(0), "BB9B", "203.0.113.2:30003");
		}
	}

	@Test
	void defaultTranslatorMissDoesNotWarn() throws Exception {
		try (Cluster cluster = definition(true, null).connect()) {
			List<String> warnings = new ArrayList<>();
			List<Peer> peers = parse(cluster, STD + ALT, warnings);

			assertEquals(List.of(new Host("203.0.113.1", 30001)), peers.get(0).hosts);
			assertEquals(List.of(), warnings);
		}
	}

	/**
	 * Parse the reply and add to {@code warnings} the WARN messages logged on the test thread
	 * meanwhile. slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}.
	 */
	private static List<Peer> parse(Cluster cluster, String reply, List<String> warnings) throws Exception {
		String prefix = "[" + Thread.currentThread().getName() + "] WARN ";
		ByteArrayOutputStream stderr = new ByteArrayOutputStream();
		PrintStream originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));

		try {
			return PeerReply.parse(cluster, reply);
		}
		finally {
			System.setErr(originalErr);
			stderr.toString(StandardCharsets.UTF_8).lines()
				.filter(line -> line.startsWith(prefix))
				.map(line -> line.substring(line.indexOf(" - ") + 3))
				.forEach(warnings::add);
		}
	}

	private static void assertNames(String message, String... parts) {
		for (String part : parts) {
			assertTrue(message.contains(part), message);
		}
	}
}
