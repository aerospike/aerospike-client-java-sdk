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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.DiscoverySettings;
import com.aerospike.client.sdk.discovery.DiscoverySettings.DiscoveryTweaks;
import com.aerospike.client.sdk.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.sdk.discovery.StaticDiscoveryConfig;
import com.aerospike.client.sdk.discovery.StubDnsServer;

class DnsSeedRefresherTest {
	private static final Host[] INIT = new Host[] {new Host("10.0.0.1", 3000)};
	private static final ClusterDefinition DEF = new ClusterDefinition("127.0.0.1", 1);

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}; WARN messages from every thread are the legacy warnings.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private StubDnsServer dns;

	@BeforeEach
	void setUp() throws Exception {
		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
		snapshot.set(INIT);
		dns = new StubDnsServer();
	}

	@AfterEach
	void tearDown() throws Exception {
		dns.close();
		System.setErr(originalErr);
	}

	/**
	 * Effective settings, as the cluster builds them: tend execution mode plus {@code discovery},
	 * merged with the defaults.
	 */
	private static SystemSettings settings(Consumer<DiscoveryTweaks> discovery) {
		return SystemSettings.builder()
			.discovery(ops -> ops.executionMode(DiscoveryExecutionMode.TEND))
			.discovery(discovery)
			.build()
			.mergeWith(SystemSettings.DEFAULT);
	}

	/**
	 * DNS provider for a.example.com and b.example.com on the stub server.
	 */
	private Consumer<DiscoveryTweaks> dns(int timeout) {
		return ops -> ops
			.provider(StaticDiscoveryConfig.DNS)
			.dns(dns -> dns
				.names(List.of("a.example.com", "b.example.com"))
				.timeout(Duration.ofMillis(timeout))
				.servers(List.of(this.dns.address())));
	}

	private DnsSeedCandidateProvider provider(int timeout) {
		return new DnsSeedCandidateProvider(settings(dns(timeout)).getDiscovery().getDns());
	}

	private static ClusterDefinition offline(Consumer<DiscoveryTweaks> discovery, Host... seeds) {
		return new ClusterDefinition(seeds)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder
				.discovery(ops -> ops.executionMode(DiscoveryExecutionMode.TEND))
				.discovery(discovery));
	}

	@Test
	void nxdomainOnEveryNameKeepsPreviousSeedList() throws Exception {
		dns.a("a.example.com", "10.0.0.2").a("b.example.com", "10.0.0.3");
		DiscoverySettings ds = settings(ops -> ops.refreshInterval(Duration.ofMillis(1000))).getDiscovery();
		SeedRefresher r = new SeedRefresher(provider(80), DEF, ds, snapshot::set);
		r.validate(1000);
		r.start(INIT);

		r.tend(1, 1000);
		Host[] lastGood = snapshot.get();
		assertArrayEquals(new Host[] {new Host("10.0.0.2", 3000), new Host("10.0.0.3", 3000)}, lastGood);

		dns.clear();
		r.tend(2, 1000);

		List<String> warnings = warnings();
		assertSame(lastGood, snapshot.get());
		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).startsWith("Seed candidate refresh failed: "), warnings.get(0));
		assertTrue(warnings.get(0).contains("DNS seed discovery failed for every name: " +
			"a.example.com: NXDOMAIN; b.example.com: NXDOMAIN"), warnings.get(0));
		assertEquals(0, r.getOverrunCount());
		assertFalse(r.isTripped());

		dns.a("a.example.com", "10.0.0.4");
		r.tend(3, 1000);
		assertArrayEquals(new Host[] {new Host("10.0.0.4", 3000)}, snapshot.get());
	}

	@Test
	void validateRejectsTimeoutAboveTendDeadline() {
		DiscoverySettings ds = settings(ops -> {}).getDiscovery();
		SeedRefresher r = new SeedRefresher(provider(150), DEF, ds, snapshot::set);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> r.validate(1000));
		assertEquals("DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
			ae.getBaseMessage());

		new SeedRefresher(provider(100), DEF, ds, snapshot::set).validate(1000);

		DiscoverySettings longer = settings(ops -> ops.tendRefreshDeadline(Duration.ofMillis(200))).getDiscovery();
		new SeedRefresher(provider(150), DEF, longer, snapshot::set).validate(1000);
	}

	@Test
	void providerNameIsDns() {
		assertEquals(StaticDiscoveryConfig.DNS, SeedRefresher.providerName(provider(80)));
	}

	@Test
	void timeoutAboveTendDeadlineFailsClusterConstruction() throws Exception {
		dns.a("a.example.com", "127.0.0.1");
		ClusterDefinition def = offline(dns(150), new Host("127.0.0.1", 1));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> def.connect().close());
		assertEquals("DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
			ae.getBaseMessage());
	}

	@Test
	void initFailureWithoutStaticSeedFailsConstruction() {
		ClusterDefinition def = offline(dns(80));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> def.connect().close());
		List<String> warnings = warnings();

		assertEquals(ResultCode.SEED_DISCOVERY_FAILED, ae.getResultCode());
		assertTrue(ae.getMessage().contains("Seed candidate provider dns failed and no seed host is left"), ae.getMessage());
		assertTrue(warnings.stream().anyMatch(m -> m.startsWith("Seed candidate provider dns failed: ") &&
			m.contains("DNS seed discovery failed for every name: a.example.com: NXDOMAIN; b.example.com: NXDOMAIN")),
			warnings.toString());
	}

	@Test
	void initFailureWithStaticSeedContinues() throws Exception {
		try (Cluster cluster = offline(dns(80), new Host("127.0.0.1", 1)).connect()) {
			List<String> warnings = warnings();
			assertArrayEquals(new Host[] {new Host("127.0.0.1", 1)}, seeds(cluster));
			assertTrue(warnings.stream().anyMatch(m -> m.startsWith("Seed candidate provider dns failed: ")),
				warnings.toString());
		}
	}

	@Test
	void initResolvesSeedsAsIpLiterals() throws Exception {
		dns.a("a.example.com", "127.0.0.1").srv("b.example.com", 1, 1, 1, "x.example.com");
		ClusterDefinition def = offline(ops -> ops
			.provider(StaticDiscoveryConfig.DNS)
			.dns(dns -> dns
				.names(List.of("a.example.com"))
				.port(2)
				.servers(List.of(this.dns.address()))),
			new Host("127.0.0.1", 1));

		try (Cluster cluster = def.connect()) {
			assertArrayEquals(new Host[] {new Host("127.0.0.1", 1), new Host("127.0.0.1", 2)}, seeds(cluster));
		}
	}

	/**
	 * Messages of the WARN lines logged so far, from every thread.
	 */
	private List<String> warnings() {
		return stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.contains("] WARN "))
			.map(line -> line.substring(line.indexOf(" - ") + 3))
			.toList();
	}

	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
	}
}
