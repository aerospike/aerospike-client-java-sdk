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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.Endpoint;
import com.aerospike.client.sdk.discovery.Endpoint.SourceList;
import com.aerospike.client.sdk.discovery.StaticMapAddressTranslator;
import com.aerospike.client.sdk.discovery.TranslationMissMode;

public class AddressTranslationTest {
	private static final Endpoint MISSED = new Endpoint("BB9", "tls1", "10.0.0.9", 3000, SourceList.STANDARD);
	private static final AddressTranslator STATIC_MAP = new StaticMapAddressTranslator(Map.of("10.0.0.1", "192.168.1.1"));

	/**
	 * {@link ClusterTend#translate} is an instance method, so the translator tests share one
	 * cluster with an unreachable seed. Its own translator and on-miss mode are not used.
	 */
	private static Cluster cluster;
	private static ClusterTend tend;

	static final class EmptyTranslator implements AddressTranslator {
		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.empty();
		}
	}

	@BeforeAll
	static void connect() {
		cluster = disconnected().connect();
		tend = cluster.getClusterTend();
	}

	@AfterAll
	static void close() {
		if (cluster != null) {
			cluster.close();
		}
	}

	/**
	 * Definition with an unreachable seed that does not fail when the cluster cannot connect.
	 */
	private static ClusterDefinition disconnected() {
		return new ClusterDefinition("127.0.0.1", 1)
			.failIfNotConnected(false)
			.tendTimeout(100);
	}

	@Test
	void customTranslatorCannotChangeTlsName() {
		AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", "rewritten", 4000));
		Host host = tend.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("192.168.1.1", host.name);
		assertEquals(4000, host.port);
		assertEquals("tls1", host.tlsName);
	}

	@Test
	void customTranslatorCannotAddTlsName() {
		AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", "rewritten", advertised.port));
		Host host = tend.translate(translator, new Endpoint("BB9", null, "10.0.0.1", 3000, SourceList.STANDARD));

		assertNull(host.tlsName);
	}

	@Test
	void customTranslatorCannotDropTlsName() {
		AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", advertised.port));
		Host host = tend.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals("tls1", host.tlsName);
	}

	@Test
	void endpointCarriesAdvertisedFields() {
		Endpoint[] seen = new Endpoint[1];
		AddressTranslator translator = advertised -> {
			seen[0] = advertised;
			return Optional.of(new Host(advertised.host, advertised.tlsName, advertised.port));
		};
		tend.translate(translator, new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.ALTERNATE));

		assertEquals("BB9", seen[0].nodeName);
		assertEquals("tls1", seen[0].tlsName);
		assertEquals("10.0.0.1", seen[0].host);
		assertEquals(3000, seen[0].port);
		assertEquals(SourceList.ALTERNATE, seen[0].sourceList);
	}

	@Test
	void passThroughMissReturnsAdvertised() {
		Host[] hosts = new Host[] {
			tend.translate(STATIC_MAP, TranslationMissMode.PASS_THROUGH, MISSED),
			tend.translate(new EmptyTranslator(), TranslationMissMode.PASS_THROUGH, MISSED),
			tend.translate(new StaticMapAddressTranslator(null), MISSED)
		};

		for (Host h : hosts) {
			assertEquals("10.0.0.9", h.name);
			assertEquals(3000, h.port);
			assertEquals("tls1", h.tlsName);
		}

		Host host = tend.translate(STATIC_MAP, TranslationMissMode.PASS_THROUGH,
			new Endpoint("BB9", null, "10.0.0.9", 3000, SourceList.STANDARD));
		assertEquals("10.0.0.9", host.name);
		assertNull(host.tlsName);
	}

	@Test
	void strictHitTranslates() {
		Host host = tend.translate(STATIC_MAP, TranslationMissMode.STRICT,
			new Endpoint("BB9", "tls1", "10.0.0.1", 3000, SourceList.STANDARD));

		assertEquals(new Host("192.168.1.1", "tls1", 3000), host);
	}

	@Test
	void strictHitEqualToAdvertisedIsHit() {
		AddressTranslator translator = new StaticMapAddressTranslator(Map.of("10.0.0.9", "10.0.0.9"));

		assertEquals(new Host("10.0.0.9", "tls1", 3000), tend.translate(translator, TranslationMissMode.STRICT, MISSED));
	}

	@Test
	void translatedPortOutOfRangeIsMiss() {
		for (int port : new int[] {0, 65536}) {
			AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", port));

			assertEquals(new Host("10.0.0.9", "tls1", 3000),
				tend.translate(translator, TranslationMissMode.PASS_THROUGH, MISSED));
			assertThrows(AerospikeException.TranslationMiss.class,
				() -> tend.translate(translator, TranslationMissMode.STRICT, MISSED));
		}

		for (int port : new int[] {1, 65535}) {
			AddressTranslator translator = advertised -> Optional.of(new Host("192.168.1.1", port));

			assertEquals(new Host("192.168.1.1", "tls1", port),
				tend.translate(translator, TranslationMissMode.STRICT, MISSED));
		}
	}

	@Test
	void strictMissFailsNamingStaticMap() {
		AerospikeException.TranslationMiss tm = assertThrows(AerospikeException.TranslationMiss.class,
			() -> tend.translate(STATIC_MAP, TranslationMissMode.STRICT, MISSED));
		String message = tm.getMessage();

		for (String part : new String[] {"BB9", "10.0.0.9:3000", "standard", "static-map", "translation.on_miss", "pass_through"}) {
			assertTrue(message.contains(part), message);
		}
	}

	@Test
	void strictMissFailsNamingCustomTranslator() {
		AerospikeException.TranslationMiss tm = assertThrows(AerospikeException.TranslationMiss.class,
			() -> tend.translate(new EmptyTranslator(), TranslationMissMode.STRICT,
				new Endpoint("BB9", null, "10.0.0.9", 3000, SourceList.ALTERNATE)));
		String message = tm.getMessage();

		assertTrue(message.contains(EmptyTranslator.class.getName()), message);
		assertTrue(message.contains("alternate"), message);
	}

	@Test
	void nullResultFailsNamingTranslator() {
		AddressTranslator translator = new AddressTranslator() {
			@Override
			public Optional<Host> translate(Endpoint advertised) {
				return null;
			}
		};

		for (TranslationMissMode mode : TranslationMissMode.values()) {
			AerospikeException ae = assertThrows(AerospikeException.class, () -> tend.translate(translator, mode, MISSED));

			assertFalse(ae instanceof AerospikeException.TranslationMiss);
			assertTrue(ae.getMessage().contains(translator.getClass().getName()), ae.getMessage());
		}
	}

	@Test
	void translatorExceptionPropagates() {
		IllegalStateException ise = new IllegalStateException("broken");
		AddressTranslator translator = advertised -> {
			throw ise;
		};

		assertSame(ise, assertThrows(IllegalStateException.class,
			() -> tend.translate(translator, TranslationMissMode.STRICT, MISSED)));
	}

	@Test
	void settingsDefaultAndDefinitionCopy() {
		assertEquals(TranslationMissMode.PASS_THROUGH, SystemSettings.DEFAULT.getTranslation().getOnMiss());

		ClusterDefinition def = disconnected()
			.withSystemSettings(builder -> builder
				.translation(ops -> ops.onMiss(TranslationMissMode.STRICT)));

		try (Cluster strict = def.connect()) {
			ClusterDefinition applied = strict.getClusterDefinition();
			assertEquals(TranslationMissMode.STRICT, applied.getTranslationOnMiss());
			assertEquals(TranslationMissMode.STRICT, new ClusterDefinition(applied).getTranslationOnMiss());
		}
	}

	@Test
	void clusterAppliesOnMiss() {
		// null: on-miss not set, so the default (pass-through) applies.
		for (TranslationMissMode mode : new TranslationMissMode[] {null, TranslationMissMode.PASS_THROUGH}) {
			try (Cluster c = connect(mode)) {
				ClusterTend t = c.getClusterTend();
				assertEquals(new Host("10.0.0.9", "tls1", 3000), t.translateAddress("BB9", "tls1", "10.0.0.9", 3000));
				assertEquals(new Host("192.168.1.1", "tls1", 3000), t.translateAddress("BB9", "tls1", "10.0.0.1", 3000));
			}
		}

		try (Cluster c = connect(TranslationMissMode.STRICT)) {
			ClusterTend t = c.getClusterTend();
			assertThrows(AerospikeException.TranslationMiss.class, () -> t.translateAddress("BB9", "tls1", "10.0.0.9", 3000));
			assertEquals(new Host("192.168.1.1", "tls1", 3000), t.translateAddress("BB9", "tls1", "10.0.0.1", 3000));
		}
	}

	private static Cluster connect(TranslationMissMode mode) {
		ClusterDefinition def = disconnected().ipMap(Map.of("10.0.0.1", "192.168.1.1"));

		if (mode != null) {
			def.withSystemSettings(builder -> builder.translation(ops -> ops.onMiss(mode)));
		}
		return def.connect();
	}
}
