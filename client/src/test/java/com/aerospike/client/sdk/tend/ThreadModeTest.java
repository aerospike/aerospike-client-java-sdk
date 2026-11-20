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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import com.aerospike.client.sdk.discovery.SeedCandidateContext;
import com.aerospike.client.sdk.discovery.SeedCandidateProvider;
import com.aerospike.client.sdk.discovery.SeedMergePolicy;
import com.aerospike.client.sdk.discovery.StaticDiscoveryConfig;
import com.aerospike.client.sdk.discovery.StaticSeedCandidateProvider;

class ThreadModeTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host S = new Host("10.0.1.1", 3000);
	private static final Host A = new Host("10.0.0.1", 3000);
	private static final Host B = new Host("10.0.0.2", 3000);
	private static final String OVERRUN = "Seed candidate refresh took ";
	private static final String FAILED = "Seed candidate refresh failed: ";
	private static final String SEED_PREFIX = "Seed 127.0.0.1 ";
	private static final String NAME = "thread-mode";
	private static final int BOUND = 150;

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private final List<String> events = new CopyOnWriteArrayList<>();
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private final List<Provider> providers = new ArrayList<>();
	private PrintStream originalErr;

	/**
	 * The legacy test's log callback. slf4j-simple prints each {@code [thread] LEVEL logger - message}
	 * line to the current {@code System.err} with one println, so each line is handled as it is
	 * logged, in order with the provider's refresh events. Cluster messages start with key-value
	 * pairs such as {@code aerospike.cluster=null}, which are removed.
	 */
	private final class LogCapture extends OutputStream {
		private final ByteArrayOutputStream line = new ByteArrayOutputStream();

		@Override
		public synchronized void write(int b) {
			if (b == '\n') {
				log(line.toString(StandardCharsets.UTF_8));
				line.reset();
			}
			else {
				line.write(b);
			}
		}

		private void log(String line) {
			boolean warn = line.contains("] WARN ");

			if (! warn && ! line.contains("] ERROR ")) {
				return;
			}

			String message = line.substring(line.indexOf(" - ") + 3);

			while (message.startsWith("aerospike.")) {
				message = message.substring(message.indexOf(' ') + 1);
			}

			if (warn && message.startsWith(SEED_PREFIX)) {
				events.add("seed " + message.substring(SEED_PREFIX.length(), message.indexOf(' ', SEED_PREFIX.length())));
			}
			else {
				warnings.add(message);
			}
		}
	}

	@BeforeEach
	void setUp() {
		originalErr = System.err;
		System.setErr(new PrintStream(new LogCapture(), true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() {
		for (Provider provider : providers) {
			provider.release.countDown();
		}
		System.setErr(originalErr);
	}

	@FunctionalInterface
	private interface Script {
		List<Host> call(int call, Provider provider) throws Exception;
	}

	private final class Provider implements SeedCandidateProvider {
		final AtomicInteger calls = new AtomicInteger();
		final AtomicInteger returned = new AtomicInteger();
		final AtomicInteger closes = new AtomicInteger();
		final AtomicLong tendNanos = new AtomicLong();
		final List<String> threads = new CopyOnWriteArrayList<>();
		final CountDownLatch release = new CountDownLatch(1);
		volatile String closeThread;
		private final boolean periodic;
		private final Script script;

		Provider(boolean periodic, Script script) {
			this.periodic = periodic;
			this.script = script;
			providers.add(this);
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			int call = calls.incrementAndGet();
			String name = Thread.currentThread().getName();
			threads.add(name);
			events.add("refresh " + call);
			long begin = System.nanoTime();

			try {
				return script.call(call, this);
			}
			catch (RuntimeException e) {
				throw e;
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
			finally {
				if (name.equals("tend")) {
					tendNanos.addAndGet(System.nanoTime() - begin);
				}
				returned.incrementAndGet();
			}
		}

		List<Host> sleep(long millis, Host... hosts) throws InterruptedException {
			Thread.sleep(millis);
			return List.of(hosts);
		}

		List<Host> block(Host... hosts) throws InterruptedException {
			release.await(60, TimeUnit.SECONDS);
			return List.of(hosts);
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return periodic;
		}

		@Override
		public boolean supportsDeadline() {
			return true;
		}

		@Override
		public void close() {
			closeThread = Thread.currentThread().getName();
			closes.incrementAndGet();
		}
	}

	/**
	 * The legacy policy(): unreachable seeds, 100 ms tend timeout, 250 ms tend interval, 60 s
	 * discovery refresh interval and {@code provider}, when not null, registered as {@link #NAME}.
	 * {@code discovery} adds to the discovery settings.
	 */
	private static ClusterDefinition definition(SeedCandidateProvider provider, Consumer<DiscoveryTweaks> discovery,
		Host... seeds) {
		return new ClusterDefinition(seeds)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)))
				.discovery(ops -> {
					ops.refreshInterval(Duration.ofMillis(60000));

					if (provider != null) {
						ops.provider(NAME).addProviderFactory(NAME, options -> provider);
					}
					discovery.accept(ops);
				}));
	}

	private static ClusterDefinition definition(SeedCandidateProvider provider, Host... seeds) {
		return definition(provider, ops -> {}, seeds);
	}

	private static DiscoverySettings discovery(Consumer<DiscoveryTweaks> discovery) {
		return SystemSettings.builder()
			.discovery(discovery)
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery();
	}

	private SeedRefresher refresher(Provider provider, Host[] staticSeeds, int interval, int timeout) {
		SeedRefresher r = new SeedRefresher(provider, new ClusterDefinition(), discovery(ops -> ops
			.refreshInterval(Duration.ofMillis(interval))
			.refreshTimeout(Duration.ofMillis(timeout))), staticSeeds, snapshot::set);
		r.validate(interval);
		return r;
	}

	private Host[] start(SeedRefresher r) {
		Host[] seeds = r.discover();
		snapshot.set(seeds);
		r.start(seeds);
		return seeds;
	}

	private long count(String prefix) {
		return warnings.stream().filter(w -> w.startsWith(prefix)).count();
	}

	/**
	 * The legacy test checked the ClientPolicy field defaults and copy constructor. The SDK
	 * execution mode and refresh timeout are discovery system settings; a copied cluster definition
	 * keeps them.
	 */
	@Test
	void policyDefaultsAndCopy() {
		DiscoverySettings ds = SystemSettings.DEFAULT.getDiscovery();
		assertEquals(DiscoveryExecutionMode.THREAD, ds.getExecutionMode());
		assertEquals(Duration.ofMillis(10000), ds.getRefreshTimeout());

		ClusterDefinition def = definition(null, ops -> ops
			.executionMode(DiscoveryExecutionMode.TEND)
			.refreshTimeout(Duration.ofMillis(700)), DEAD_SEED);

		try (Cluster cluster = new ClusterDefinition(def).connect()) {
			DiscoverySettings copy = cluster.getSystemSettings().getDiscovery();
			assertEquals(DiscoveryExecutionMode.TEND, copy.getExecutionMode());
			assertEquals(Duration.ofMillis(700), copy.getRefreshTimeout());
		}
	}

	@Test
	void defaultPluginsStartNoDiscoveryThread() throws Exception {
		SeedCandidateProvider[] list = new SeedCandidateProvider[] {
			null, new StaticSeedCandidateProvider(DEAD_SEED), new Provider(false, (call, p) -> List.of(DEAD_SEED))
		};

		for (SeedCandidateProvider provider : list) {
			Set<Thread> before = Thread.getAllStackTraces().keySet();

			try (Cluster cluster = definition(provider, DEAD_SEED).connect()) {
				assertNull(seedRefresher(cluster).getThread());
				assertEquals(List.of("tend"), newThreadNames(before));
			}
		}
	}

	@Test
	void periodicProviderRunsOnDiscoveryThread() throws Exception {
		Provider provider = new Provider(true, (call, p) -> List.of(new Host("127.0.0.1", 10 + call)));
		Set<Thread> before = Thread.getAllStackTraces().keySet();

		try (Cluster cluster = definition(provider, ops -> ops.refreshInterval(Duration.ofMillis(250)), DEAD_SEED)
			.connect()) {
			assertEquals(List.of("discovery", "tend"), newThreadNames(before));
			assertTrue(seedRefresher(cluster).getThread().isDaemon());
			await(() -> provider.calls.get() >= 4);
			await(() -> seeds(cluster).length == 2 && seeds(cluster)[1].port >= 13);
		}
		assertEquals(Collections.singleton("discovery"), new HashSet<>(provider.threads));
	}

	@Test
	void blockingProviderNeverBlocksTend() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(DEAD_SEED) : p.block(B));

		try (Cluster cluster = definition(provider, ops -> ops.refreshInterval(Duration.ofMillis(250)), DEAD_SEED)
			.connect()) {
			await(() -> provider.calls.get() == 2);
			int tends = cluster.getInvalidNodeCount();
			long begin = System.nanoTime();
			await(() -> cluster.getInvalidNodeCount() >= tends + 8);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			assertEquals(2, provider.calls.get());
			assertEquals(1, provider.returned.get());
			assertEquals(0, provider.tendNanos.get());
			assertFalse(provider.threads.contains("tend"));
			assertTrue(elapsed < 8 * 250 + 1000, elapsed + "ms");
		}
	}

	@Test
	void urgentRequestDuringRefreshIsDropped() throws Exception {
		CountDownLatch running = new CountDownLatch(1);
		Provider provider = new Provider(true, (call, p) -> {
			if (call == 2) {
				running.countDown();
				return p.block(B);
			}
			return List.of(A);
		});
		SeedRefresher r = refresher(provider, new Host[0], 1000, 1000);

		try {
			start(r);
			assertTrue(running.await(10, TimeUnit.SECONDS));
			r.requestRefresh();
			provider.release.countDown();
			await(() -> provider.returned.get() >= 2);
			Thread.sleep(300);
			assertEquals(2, provider.calls.get());

			long begin = System.nanoTime();
			r.requestRefresh();
			await(() -> provider.calls.get() == 3);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			assertTrue(elapsed < 300, elapsed + "ms");
		}
		finally {
			r.close();
		}
	}

	@Test
	void lateSuccessIsPublished() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(A) : (call == 2)? p.sleep(150, B) : p.block());
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			assertArrayEquals(new Host[] {A}, start(r));
			await(() -> provider.calls.get() == 3);

			assertArrayEquals(new Host[] {B}, snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(1, count(OVERRUN), warnings.toString());
			assertTrue(warnings.stream().anyMatch(w ->
				w.startsWith(OVERRUN) && w.contains("exceeding refresh timeout 100ms")), warnings.toString());
		}
		finally {
			r.close();
		}
	}

	@Test
	void lateEmptyResultIsPublished() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(A) : (call == 2)? p.sleep(150) : p.block());
		SeedRefresher r = refresher(provider, new Host[] {S}, 50, 100);

		try {
			assertArrayEquals(new Host[] {S, A}, start(r));
			await(() -> provider.calls.get() == 3);

			assertArrayEquals(new Host[] {S}, snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(1, count(OVERRUN), warnings.toString());
		}
		finally {
			r.close();
		}
	}

	@Test
	void replaceLateEmptyResultEmptiesSnapshot() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(A) : (call == 2)? p.sleep(150) : p.block());
		SeedRefresher r = new SeedRefresher(provider, new ClusterDefinition(), discovery(ops -> ops
			.mergePolicy(SeedMergePolicy.REPLACE)
			.refreshInterval(Duration.ofMillis(50))
			.refreshTimeout(Duration.ofMillis(100))), new Host[0], snapshot::set);
		r.validate(50);

		try {
			assertArrayEquals(new Host[] {A}, start(r));
			await(() -> provider.calls.get() == 3);

			assertArrayEquals(new Host[0], snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(1, count(OVERRUN), warnings.toString());
		}
		finally {
			r.close();
		}
	}

	@Test
	void lateErrorKeepsLastGood() throws Exception {
		Provider provider = new Provider(true, (call, p) -> {
			if (call == 1) {
				return List.of(A);
			}
			if (call == 2) {
				p.sleep(150);
				throw new IllegalStateException("cloud API down");
			}
			return p.block(B);
		});
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			Host[] lastGood = start(r);
			await(() -> provider.calls.get() == 3);

			assertSame(lastGood, snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(1, count(OVERRUN), warnings.toString());
			assertEquals(1, count(FAILED), warnings.toString());
		}
		finally {
			r.close();
		}
	}

	@Test
	void errorKeepsLastGood() throws Exception {
		Provider provider = new Provider(true, (call, p) -> {
			if (call == 1) {
				return List.of(A);
			}
			if (call == 2) {
				throw new IllegalStateException("cloud API down");
			}
			return p.block(B);
		});
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			Host[] lastGood = start(r);
			await(() -> provider.calls.get() == 3);

			assertSame(lastGood, snapshot.get());
			assertEquals(0, r.getOverrunCount());
			assertEquals(1, count(FAILED), warnings.toString());
		}
		finally {
			r.close();
		}
	}

	@Test
	void overrunsNeverTrip() throws Exception {
		Provider provider = new Provider(true, (call, p) ->
			(call == 1)? List.of(A) : (call <= 5)? p.sleep(150, new Host("10.0.0.9", call)) : p.block());
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			start(r);
			await(() -> provider.calls.get() == 6);

			assertEquals(4, r.getOverrunCount());
			assertFalse(r.isTripped());
			assertEquals(4, count(OVERRUN), warnings.toString());
			assertArrayEquals(new Host[] {new Host("10.0.0.9", 5)}, snapshot.get());
		}
		finally {
			r.close();
		}
	}

	@Test
	void slowFirstRefreshUsesFallbackAndPublishesLate() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? p.sleep(1000, A) : p.block());
		SeedRefresher r = refresher(provider, new Host[] {S}, 60000, 200);

		try {
			long begin = System.nanoTime();
			Host[] seeds = start(r);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			assertTrue(elapsed >= 200 && elapsed <= 200 + BOUND, elapsed + "ms");
			assertArrayEquals(new Host[] {S}, seeds);
			assertEquals(1, count("Seed candidate provider "), warnings.toString());
			assertTrue(warnings.stream().anyMatch(w ->
				w.contains("did not complete within the discovery refresh timeout 200ms")), warnings.toString());

			await(() -> Arrays.equals(new Host[] {S, A}, snapshot.get()));
			assertEquals(1, r.getOverrunCount());
		}
		finally {
			r.close();
		}
	}

	@Test
	void slowFirstRefreshClientUsesFallbackSeeds() throws Exception {
		Host late = new Host("127.0.0.1", 2);
		Provider provider = new Provider(true, (call, p) -> (call == 1)? p.sleep(1000, late) : p.block());

		long begin = System.nanoTime();
		Cluster cluster = definition(provider, ops -> ops.refreshTimeout(Duration.ofMillis(200)), DEAD_SEED).connect();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		try {
			assertTrue(elapsed <= 200 + BOUND, elapsed + "ms");
			assertArrayEquals(new Host[] {DEAD_SEED}, seeds(cluster));
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, late}, seeds(cluster)));
		}
		finally {
			cluster.close();
		}
	}

	@Test
	void slowFirstRefreshWithoutFallbackFails() throws Exception {
		Provider provider = new Provider(true, (call, p) -> p.sleep(500, A));
		ClusterDefinition def = definition(provider, ops -> ops.refreshTimeout(Duration.ofMillis(200)));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		long begin = System.nanoTime();
		AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertEquals(ResultCode.SEED_DISCOVERY_FAILED, ae.getResultCode());
		assertTrue(elapsed <= 200 + BOUND, elapsed + "ms");
		await(() -> provider.closes.get() == 1);
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(1, provider.calls.get());
		assertEquals("discovery", provider.closeThread);
	}

	@Test
	void reseedDoesNotWaitForRefreshInFlight() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(new Host("127.0.0.1", 11)) : p.block(B));

		try (Cluster cluster = definition(provider, DEAD_SEED).connect()) {
			Host[] seeds = seeds(cluster);
			await(() -> events.contains("refresh 2"));
			int from = events.indexOf("refresh 2");
			await(() -> events.subList(from, events.size()).stream().filter(e -> e.equals("seed 11")).count() >= 3);

			assertEquals(2, provider.calls.get());
			assertEquals(1, provider.returned.get());
			assertSame(seeds, seeds(cluster));
			assertEquals(0, cluster.getNodes().length);
		}

		List<String> attempts = events.subList(0, events.indexOf("refresh 2"));
		assertEquals(List.of("refresh 1", "seed 1", "seed 11"), attempts.subList(0, 3));
	}

	@Test
	void dnsTimeoutAboveRefreshTimeoutRejected() {
		Consumer<DiscoveryTweaks> dns = ops -> ops
			.provider(StaticDiscoveryConfig.DNS)
			.dns(d -> d
				.names(List.of("a.example.com"))
				.timeout(Duration.ofMillis(300))
				.servers(List.of("127.0.0.1:1")));

		ClusterDefinition def = definition(null, ops -> {
			dns.accept(ops);
			ops.refreshTimeout(Duration.ofMillis(200));
		}, DEAD_SEED);

		AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
		assertEquals("DNS seed provider timeout 300 must be less or equal to the discovery refresh timeout 200",
			ae.getBaseMessage());

		DnsSeedCandidateProvider provider = new DnsSeedCandidateProvider(discovery(dns).getDns());

		new SeedRefresher(provider, new ClusterDefinition(), discovery(ops -> ops
			.refreshTimeout(Duration.ofMillis(300))), snapshot::set).validate(1000);

		new SeedRefresher(provider, new ClusterDefinition(), discovery(ops -> ops
			.executionMode(DiscoveryExecutionMode.TEND)
			.refreshTimeout(Duration.ofMillis(200))
			.tendRefreshDeadline(Duration.ofMillis(300))), snapshot::set).validate(1000);
	}

	@Test
	void rejectInvalidRefreshTimeout() {
		for (int timeout : new int[] {0, -1}) {
			ClusterDefinition def = definition(new Provider(true, (call, p) -> List.of(DEAD_SEED)),
				ops -> ops.refreshTimeout(Duration.ofMillis(timeout)), DEAD_SEED);

			AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
			assertEquals("Invalid discovery refresh timeout: " + timeout + ". Must be > 0", ae.getBaseMessage());
		}
	}

	@Test
	void tendModeRunsOnTendWithNoThread() throws Exception {
		Provider provider = new Provider(true, (call, p) -> List.of(DEAD_SEED));
		Set<Thread> before = Thread.getAllStackTraces().keySet();

		try (Cluster cluster = definition(provider, ops -> ops
			.executionMode(DiscoveryExecutionMode.TEND)
			.refreshInterval(Duration.ofMillis(250)), DEAD_SEED).connect()) {
			assertNull(seedRefresher(cluster).getThread());
			await(() -> provider.calls.get() >= 3);
			assertEquals(List.of("tend"), newThreadNames(before));
		}
		assertEquals(Thread.currentThread().getName(), provider.threads.get(0));
		assertTrue(provider.threads.subList(1, provider.threads.size()).stream().allMatch(t -> t.equals("tend")));
	}

	@Test
	void closeWaitsForRefreshInFlight() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(DEAD_SEED) : p.sleep(400, B));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		Cluster cluster = definition(provider, ops -> ops
			.refreshInterval(Duration.ofMillis(250))
			.refreshTimeout(Duration.ofMillis(1000)), DEAD_SEED).connect();
		Thread thread = seedRefresher(cluster).getThread();
		await(() -> provider.calls.get() == 2);

		long begin = System.nanoTime();
		cluster.close();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertEquals(2, provider.returned.get());
		assertEquals(1, provider.closes.get());
		assertEquals("discovery", provider.closeThread);
		assertFalse(thread.isAlive());
		assertTrue(elapsed < 1000, elapsed + "ms");
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(2, provider.calls.get());
	}

	@Test
	void closeWaitsAtMostRefreshTimeout() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? List.of(DEAD_SEED) : p.block(B));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		Cluster cluster = definition(provider, ops -> ops
			.refreshInterval(Duration.ofMillis(250))
			.refreshTimeout(Duration.ofMillis(500)), DEAD_SEED).connect();
		Thread thread = seedRefresher(cluster).getThread();
		await(() -> provider.calls.get() == 2);

		long begin = System.nanoTime();
		cluster.close();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue(elapsed >= 500 && elapsed <= 500 + BOUND, elapsed + "ms");
		assertTrue(thread.isAlive());
		assertEquals(0, provider.closes.get());

		provider.release.countDown();
		await(() -> ! thread.isAlive());
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(1, provider.closes.get());
		assertEquals("discovery", provider.closeThread);
		assertEquals(2, provider.calls.get());
	}

	@Test
	void closeStopsIdleThread() throws Exception {
		Provider provider = new Provider(true, (call, p) -> List.of(DEAD_SEED));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		Cluster cluster = definition(provider, DEAD_SEED).connect();
		Thread thread = seedRefresher(cluster).getThread();
		assertTrue(thread.isAlive());

		cluster.close();
		await(() -> ! thread.isAlive());
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(1, provider.closes.get());
	}

	/**
	 * The cluster's current seeds. The legacy test read {@code Cluster.getSeeds()}.
	 */
	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
	}

	/**
	 * The legacy test read {@code cluster.seedRefresher}.
	 */
	private static SeedRefresher seedRefresher(Cluster cluster) throws Exception {
		Field field = Cluster.class.getDeclaredField("seedRefresher");
		field.setAccessible(true);
		return (SeedRefresher)field.get(cluster);
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

	@FunctionalInterface
	private interface Condition {
		boolean get() throws Exception;
	}

	private static void await(Condition condition) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.get()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}
}
