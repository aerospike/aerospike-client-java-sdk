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
package com.aerospike.client.sdk.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.DnsSeedCandidateProvider.RecordType;
import com.aerospike.client.sdk.discovery.DnsSettings.DnsTweaks;

public class DnsSeedCandidateProviderTest {
	private static final SeedCandidateContext CTX = new SeedCandidateContext(null, new Host[0], 0);
	private static final Pattern IP_LITERAL = Pattern.compile("^(\\d{1,3}(\\.\\d{1,3}){3}|[0-9a-fA-F:]*:[0-9a-fA-F:.]*)$");
	private static final long MARGIN = 50;

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private PrintStream originalErr;
	private StubDnsServer dns;

	/**
	 * The legacy WARN log callback. slf4j-simple prints each {@code [thread] LEVEL logger - message}
	 * line to the current {@code System.err}; WARN messages are collected as they are logged.
	 */
	private final class LogCapture extends OutputStream {
		private final ByteArrayOutputStream line = new ByteArrayOutputStream();

		@Override
		public synchronized void write(int b) {
			if (b == '\n') {
				String text = line.toString(StandardCharsets.UTF_8);
				line.reset();

				if (text.contains("] WARN ")) {
					warnings.add(text.substring(text.indexOf(" - ") + 3));
				}
			}
			else {
				line.write(b);
			}
		}
	}

	@BeforeEach
	void setUp() throws Exception {
		originalErr = System.err;
		System.setErr(new PrintStream(new LogCapture(), true, StandardCharsets.UTF_8));
		dns = new StubDnsServer();
	}

	@AfterEach
	void tearDown() throws Exception {
		dns.close();
		System.setErr(originalErr);
	}

	/**
	 * DNS settings as the client builds them: {@code tweaks} merged with the defaults.
	 */
	private static DnsSettings settings(Consumer<DnsTweaks> tweaks) {
		return SystemSettings.builder()
			.discovery(ops -> ops.dns(tweaks))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery()
			.getDns();
	}

	private static DnsSeedCandidateProvider provider(Consumer<DnsTweaks> tweaks) {
		return new DnsSeedCandidateProvider(settings(tweaks));
	}

	/**
	 * The legacy builder(names): the names and the stub server. {@code extra} adds to these.
	 */
	private DnsSeedCandidateProvider provider(Consumer<DnsTweaks> extra, String... names) {
		return provider(ops -> {
			ops.names(List.of(names)).servers(List.of(dns.address()));
			extra.accept(ops);
		});
	}

	private DnsSeedCandidateProvider provider(String... names) {
		return provider(ops -> {}, names);
	}

	/**
	 * The legacy getServers(). The SDK provider keeps the configured servers but has no getter.
	 */
	@SuppressWarnings("unchecked")
	private static List<String> servers(DnsSeedCandidateProvider provider) throws Exception {
		Field field = DnsSeedCandidateProvider.class.getDeclaredField("servers");
		field.setAccessible(true);
		return (List<String>)field.get(provider);
	}

	private static void assertIpLiterals(List<Host> hosts) {
		for (Host host : hosts) {
			assertTrue(IP_LITERAL.matcher(host.name).matches(), "not an IP literal: " + host.name);
		}
	}

	@Test
	void aAndAaaaRecords() throws Exception {
		dns.a("seeds.example.com", "10.0.0.9", "10.0.0.2").aaaa("seeds.example.com", "2001:db8::1");
		DnsSeedCandidateProvider provider = provider(ops -> ops.port(3100), "seeds.example.com");

		List<Host> hosts = provider.refreshSeedCandidates(CTX);

		assertEquals(List.of(new Host("10.0.0.2", 3100), new Host("10.0.0.9", 3100), new Host("2001:db8::1", 3100)),
			hosts);
		assertEquals(hosts, provider.refreshSeedCandidates(CTX));
		assertIpLiterals(hosts);
		assertTrue(warnings.isEmpty(), warnings.toString());
	}

	@Test
	void defaultPortIs3000() throws Exception {
		dns.a("seeds.example.com", "10.0.0.1");
		DnsSeedCandidateProvider provider = provider("seeds.example.com");

		assertEquals(List.of(new Host("10.0.0.1", 3000)), provider.refreshSeedCandidates(CTX));
		assertEquals(RecordType.A, provider.getRecordType());
		assertEquals(DnsSeedCandidateProvider.DEFAULT_PORT, provider.getPort());
		assertEquals(DnsSeedCandidateProvider.DEFAULT_TIMEOUT, provider.getTimeout());
		assertNull(provider.getTlsName());
	}

	@Test
	void multipleNamesInOrderAndDeduplicated() throws Exception {
		dns.a("b.example.com", "10.0.0.2", "10.0.0.3");
		dns.a("a.example.com", "10.0.0.1", "10.0.0.2");
		DnsSeedCandidateProvider provider = provider("b.example.com", "a.example.com.");

		assertEquals(List.of(new Host("10.0.0.2", 3000), new Host("10.0.0.3", 3000), new Host("10.0.0.1", 3000)),
			provider.refreshSeedCandidates(CTX));
	}

	@Test
	void srvTargetsFromAdditionalSectionResponse() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 20, 5, 3002, "node2.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 9, 3003, "node3.example.com")
			.additionalA("_aerospike._tcp.example.com", "node1.example.com", "10.0.1.1")
			.additionalA("_aerospike._tcp.example.com", "node2.example.com", "10.0.1.2")
			.additionalA("_aerospike._tcp.example.com", "node3.example.com", "10.0.1.3")
			.a("node1.example.com", "10.0.1.1")
			.a("node2.example.com", "10.0.1.2")
			.a("node3.example.com", "10.0.1.3");
		DnsSeedCandidateProvider provider = provider(ops -> ops.recordType(RecordType.SRV).port(4000),
			"_aerospike._tcp.example.com");

		List<Host> hosts = provider.refreshSeedCandidates(CTX);

		assertEquals(List.of(new Host("10.0.1.3", 3003), new Host("10.0.1.1", 3001), new Host("10.0.1.2", 3002)), hosts);
		assertIpLiterals(hosts);
		assertEquals(hosts, provider.refreshSeedCandidates(CTX));
	}

	@Test
	void srvTargetsFromFollowUpQueries() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
			.a("node1.example.com", "10.0.1.1")
			.aaaa("node2.example.com", "2001:db8::2");
		DnsSeedCandidateProvider provider = provider(ops -> ops.recordType(RecordType.SRV), "_aerospike._tcp.example.com");

		List<Host> hosts = provider.refreshSeedCandidates(CTX);

		assertEquals(List.of(new Host("10.0.1.1", 3001), new Host("2001:db8::2", 3002)), hosts);
		assertIpLiterals(hosts);
		assertTrue(dns.queries().contains("node1.example.com./" + StubDnsServer.A), dns.queries().toString());
		assertTrue(dns.queries().contains("node2.example.com./" + StubDnsServer.AAAA), dns.queries().toString());
	}

	@Test
	void srvTargetThatDoesNotResolveIsDropped() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3002, "gone.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3003, ".")
			.a("node1.example.com", "10.0.1.1");
		DnsSeedCandidateProvider provider = provider(ops -> ops.recordType(RecordType.SRV), "_aerospike._tcp.example.com");

		List<Host> hosts = provider.refreshSeedCandidates(CTX);

		assertEquals(List.of(new Host("10.0.1.1", 3001)), hosts);
		assertEquals(1, warnings.size(), warnings.toString());
		assertEquals("DNS seed name _aerospike._tcp.example.com SRV target gone.example.com. dropped: NXDOMAIN",
			warnings.get(0));
	}

	@Test
	void partialFailureReturnsResolvedNamesAndWarns() throws Exception {
		dns.a("good.example.com", "10.0.0.1")
			.rcode("bad.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL)
			.rcode("bad.example.com", StubDnsServer.AAAA, StubDnsServer.SERVFAIL);
		DnsSeedCandidateProvider provider = provider("missing.example.com", "good.example.com", "bad.example.com");

		List<Host> hosts = provider.refreshSeedCandidates(CTX);

		assertEquals(List.of(new Host("10.0.0.1", 3000)), hosts);
		assertEquals(List.of("DNS seed name missing.example.com failed: NXDOMAIN",
			"DNS seed name bad.example.com failed: SERVFAIL"), warnings);
	}

	@Test
	void everyNameFailingThrowsNamingEach() throws Exception {
		dns.rcode("bad.example.com", StubDnsServer.A, StubDnsServer.SERVFAIL)
			.rcode("bad.example.com", StubDnsServer.AAAA, StubDnsServer.SERVFAIL)
			.srv("other.example.com", 1, 1, 1, "x.example.com");
		DnsSeedCandidateProvider provider = provider("missing.example.com", "bad.example.com", "other.example.com");

		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));

		assertEquals("DNS seed discovery failed for every name: missing.example.com: NXDOMAIN; " +
			"bad.example.com: SERVFAIL; other.example.com: empty answer", ae.getBaseMessage());
	}

	@Test
	void unansweredQueryReturnsWithinTimeout() throws Exception {
		try (StubDnsServer silent = new StubDnsServer(true)) {
			int timeout = 80;
			DnsSeedCandidateProvider provider = provider(ops -> ops
				.names(List.of("a.example.com", "b.example.com"))
				.timeout(Duration.ofMillis(timeout))
				.servers(List.of(silent.address())));

			for (int i = 0; i < 3; i++) {
				long begin = System.nanoTime();
				AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
				long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

				assertTrue(elapsed <= timeout + MARGIN, "elapsed " + elapsed + "ms");
				assertEquals("DNS seed discovery failed for every name: a.example.com: timeout; b.example.com: timeout",
					ae.getBaseMessage());
			}
		}
	}

	@Test
	void unansweredSrvTargetsShareOneBudget() throws Exception {
		dns.srv("_aerospike._tcp.example.com", 10, 1, 3001, "node1.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3002, "node2.example.com")
			.srv("_aerospike._tcp.example.com", 10, 1, 3003, "node3.example.com");

		for (String target : List.of("node1.example.com", "node2.example.com", "node3.example.com")) {
			dns.rcode(target, StubDnsServer.A, StubDnsServer.NO_ANSWER).rcode(target, StubDnsServer.AAAA, StubDnsServer.NO_ANSWER);
		}

		int timeout = 60;
		DnsSeedCandidateProvider provider = provider(ops -> ops
			.recordType(RecordType.SRV)
			.timeout(Duration.ofMillis(timeout)), "_aerospike._tcp.example.com");

		long begin = System.nanoTime();
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue(elapsed <= timeout + MARGIN, "elapsed " + elapsed + "ms");
		assertEquals("DNS seed discovery failed for every name: _aerospike._tcp.example.com: no SRV target resolved",
			ae.getBaseMessage());
		assertEquals(3, warnings.size(), warnings.toString());

		for (String warning : warnings) {
			assertTrue(warning.endsWith("dropped: timeout"), warning);
		}
	}

	@Test
	void truncatedAnswerWithUnansweredTcpReturnsWithinTimeout() throws Exception {
		try (StubDnsServer silent = new StubDnsServer(true).hangTcp();
			StubDnsServer truncated = new StubDnsServer().a("a.example.com", "10.0.0.1").truncate().hangTcp()) {
			int timeout = 200;
			DnsSeedCandidateProvider provider = provider(ops -> ops
				.names(List.of("a.example.com"))
				.timeout(Duration.ofMillis(timeout))
				.servers(List.of(silent.address(), truncated.address())));

			long begin = System.nanoTime();
			assertThrows(AerospikeException.class, () -> provider.refreshSeedCandidates(CTX));
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			assertTrue(elapsed <= timeout + MARGIN, "elapsed " + elapsed + "ms");
			assertFalse(truncated.queries().isEmpty(), truncated.queries().toString());
		}
	}

	@Test
	void tlsNameCarriedOnEveryHost() throws Exception {
		dns.a("seeds.example.com", "10.0.0.1", "10.0.0.2")
			.srv("_aerospike._tcp.example.com", 10, 1, 4333, "seeds.example.com");

		for (RecordType type : RecordType.values()) {
			String name = (type == RecordType.A)? "seeds.example.com" : "_aerospike._tcp.example.com";
			DnsSeedCandidateProvider provider = provider(ops -> ops.recordType(type).tlsName("aerospike-tls"), name);
			List<Host> hosts = provider.refreshSeedCandidates(CTX);

			assertFalse(hosts.isEmpty());

			for (Host host : hosts) {
				assertEquals("aerospike-tls", host.tlsName);
			}
			assertEquals("aerospike-tls", provider.getTlsName());
		}
	}

	@Test
	void needsPeriodicRefreshAndSupportsDeadline() {
		DnsSeedCandidateProvider provider = provider("seeds.example.com");

		assertTrue(provider.needsPeriodicRefresh());
		assertTrue(provider.supportsDeadline());
	}

	@Test
	void startsNoThread() throws Exception {
		dns.a("seeds.example.com", "10.0.0.1");
		DnsSeedCandidateProvider provider = provider("seeds.example.com");
		provider.refreshSeedCandidates(CTX);

		ThreadMXBean bean = ManagementFactory.getThreadMXBean();
		long started = bean.getTotalStartedThreadCount();

		for (int i = 0; i < 5; i++) {
			provider.refreshSeedCandidates(CTX);
		}
		assertEquals(started, bean.getTotalStartedThreadCount());
	}

	@Test
	void systemResolversReadAtConstruction() throws Exception {
		DnsSeedCandidateProvider provider = provider(ops -> ops.names(List.of("seeds.example.com")));

		assertEquals(List.of(), servers(provider));
	}

	/**
	 * The SDK reports the timeout as a Duration. The null recordType case builds the settings
	 * without the defaults, which would otherwise replace the null.
	 */
	@Test
	void invalidConfigRejectedAtConstruction() throws Exception {
		assertRejected(ops -> ops.names(List.of()), "Invalid DNS seed provider names: at least one name is required");
		assertRejected(ops -> ops.names(null), "Invalid DNS seed provider names: at least one name is required");
		assertRejected(ops -> ops.names(List.of("a.example.com", "")), "Invalid DNS seed provider name: ''");
		assertRejected(ops -> ops.names(List.of("a.example.com", "A.example.com")), "duplicate");
		assertRejected(ops -> ops.names(List.of("a.example.com", "a.example.com.")), "duplicate name 'a.example.com.'");
		assertRejected(ops -> ops.port(0), "Invalid DNS seed provider port: 0");
		assertRejected(ops -> ops.port(65536), "Invalid DNS seed provider port: 65536");
		assertRejected(ops -> ops.timeout(Duration.ZERO), "Invalid DNS seed provider timeout: PT0S");
		assertRejected(ops -> ops.tlsName(""), "Invalid DNS seed provider tlsName: empty");
		assertRejected(ops -> ops.servers(List.of("dns.example.com")),
			"Invalid DNS seed provider server: 'dns.example.com': expected an IP address");
		assertRejected(ops -> ops.servers(List.of("10.0.0.256")), "Invalid DNS seed provider server: '10.0.0.256'");
		assertRejected(ops -> ops.servers(List.of("10.0.0.1:x")), "Invalid DNS seed provider server: '10.0.0.1:x': invalid port");
		assertRejected(ops -> ops.servers(List.of("10.0.0.1:70000")), "invalid port");
		assertRejected(ops -> ops.servers(List.of("[2001:db8::1")), "Invalid DNS seed provider server: '[2001:db8::1'");
		assertRejected(ops -> ops.servers(List.of("2001:db8::zz")), "expected an IP address");
		assertRejected(ops -> ops.servers(Collections.singletonList(null)), "Invalid DNS seed provider server: null");

		DnsSettings.Builder builder = DnsSettings.builder();
		new DnsSettings.DnsTweaksImpl(builder).names(List.of("seeds.example.com")).recordType(null);
		AerospikeException ae = assertThrows(AerospikeException.class, () -> new DnsSeedCandidateProvider(builder.build()));
		assertTrue(ae.getBaseMessage().contains("Invalid DNS seed provider recordType: null"), ae.getBaseMessage());

		List<String> servers = Arrays.asList("10.0.0.1", "10.0.0.2:5353", "2001:db8::1", "[2001:db8::2]:5353", "[::1]");
		DnsSeedCandidateProvider provider = provider(ops -> ops.names(List.of("a.example.com")).servers(servers));
		assertEquals(servers, servers(provider));
	}

	private static void assertRejected(Consumer<DnsTweaks> config, String message) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> provider(ops -> {
			ops.names(List.of("seeds.example.com")).servers(List.of("127.0.0.1"));
			config.accept(ops);
		}));
		assertTrue(ae.getBaseMessage().contains(message), ae.getBaseMessage());
	}
}
