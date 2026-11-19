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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.DiscoverySettings;

class DiscoveryRefreshTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host REFRESHED = new Host("127.0.0.1", 2);
	private static final ClusterDefinition DEF = new ClusterDefinition(DEAD_SEED);

	private static ClusterDefinition definition(TestSeedProvider provider) {
		return definition(provider, builder -> {});
	}

	/**
	 * Unreachable seed, tend execution mode, 250 ms tend interval and {@code provider}, when not
	 * null, registered as {@link TestSeedProvider#NAME}. {@code settings} adds to the system
	 * settings; repeated {@code discovery} and {@code refresh} calls add to the same settings.
	 */
	private static ClusterDefinition definition(TestSeedProvider provider, Consumer<SystemSettings.Builder> settings) {
		return new ClusterDefinition(DEAD_SEED)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> {
				builder
					.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)))
					.discovery(ops -> ops.executionMode(DiscoveryExecutionMode.TEND));

				if (provider != null) {
					builder.discovery(ops -> ops
						.provider(TestSeedProvider.NAME)
						.addProviderFactory(TestSeedProvider.NAME, options -> provider));
				}
				settings.accept(builder);
			});
	}

	private static Consumer<SystemSettings.Builder> intervals(int tendInterval, int refreshInterval) {
		return builder -> builder
			.refresh(ops -> ops.tendInterval(Duration.ofMillis(tendInterval)))
			.discovery(ops -> ops.refreshInterval(Duration.ofMillis(refreshInterval)));
	}

	@Test
	void settingsDefaults() {
		DiscoverySettings ds = SystemSettings.DEFAULT.getDiscovery();
		assertEquals("", ds.getProvider(), "default seed provider is static");
		assertEquals(Duration.ofMillis(30000), ds.getRefreshInterval());
		assertEquals(Duration.ofMillis(100), ds.getTendRefreshDeadline());
	}

	@Test
	void settingsOverrideDefaults() {
		TestSeedProvider provider = new TestSeedProvider(false, false, DEAD_SEED);
		DiscoverySettings ds = SystemSettings.builder()
			.discovery(ops -> ops
				.provider(TestSeedProvider.NAME)
				.addProviderFactory(TestSeedProvider.NAME, options -> provider)
				.refreshInterval(Duration.ofMillis(5000))
				.tendRefreshDeadline(Duration.ofMillis(50)))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery();

		assertEquals(TestSeedProvider.NAME, ds.getProvider());
		assertEquals(provider, ds.getProviderFactories().get(TestSeedProvider.NAME).apply(null));
		assertEquals(Duration.ofMillis(5000), ds.getRefreshInterval());
		assertEquals(Duration.ofMillis(50), ds.getTendRefreshDeadline());
	}

	@Test
	void rejectRefreshIntervalBelowTendInterval() {
		ClusterDefinition def = definition(new TestSeedProvider(true, true, DEAD_SEED), intervals(1000, 999));

		AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
		assertTrue(ae.getMessage().contains("Discovery refresh interval 999"), ae.getMessage());
	}

	@Test
	void allowRefreshIntervalBelowTendIntervalWithoutPeriodicRefresh() {
		definition(null, intervals(1000, 999)).connect().close();
	}

	@Test
	void rejectInvalidTendRefreshDeadline() {
		for (int deadline : new int[] {0, -1, 501}) {
			ClusterDefinition def = definition(new TestSeedProvider(true, true, DEAD_SEED),
				deadline(1000, deadline));

			AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
			assertTrue(ae.getMessage().contains("Invalid discovery tend refresh deadline: " + deadline), ae.getMessage());
		}

		definition(new TestSeedProvider(true, true, DEAD_SEED), deadline(1000, 500)).connect().close();
		definition(null, deadline(1000, 501)).connect().close();
	}

	private static Consumer<SystemSettings.Builder> deadline(int tendInterval, int deadline) {
		return builder -> builder
			.refresh(ops -> ops.tendInterval(Duration.ofMillis(tendInterval)))
			.discovery(ops -> ops.tendRefreshDeadline(Duration.ofMillis(deadline)));
	}

	@Test
	void rejectPeriodicProviderWithoutDeadlineSupport() {
		ClusterDefinition def = definition(new TestSeedProvider(true, false, DEAD_SEED));

		AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
		assertTrue(ae.getMessage().contains("must support a deadline"), ae.getMessage());

		definition(new TestSeedProvider(false, false, DEAD_SEED)).connect().close();
	}

	@Test
	void noPeriodicRefreshStartsNoThread() throws Exception {
		for (TestSeedProvider provider : new TestSeedProvider[] {null, new TestSeedProvider(false, true, DEAD_SEED)}) {
			ClusterDefinition def = definition(provider, intervals(250, 250));

			Set<Thread> before = Thread.getAllStackTraces().keySet();
			Cluster cluster = def.connect();

			try {
				assertEquals(List.of("tend"), newThreadNames(before));
				ClusterTend tend = cluster.getClusterTend();
				int tends = tend.getInvalidNodeCount();
				await(() -> tend.getInvalidNodeCount() >= tends + 3);

				assertEquals(List.of("tend"), newThreadNames(before));
				assertArrayEquals(new Host[] {DEAD_SEED}, seeds(cluster));

				if (provider != null) {
					assertEquals(1, provider.calls.get());
				}
			}
			finally {
				cluster.close();
			}
		}
	}

	@Test
	void periodicRefreshRunsOnTendWithNoNewThread() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClusterDefinition def = definition(provider, intervals(250, 500));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		Cluster cluster = def.connect();

		try {
			assertEquals(List.of("tend"), newThreadNames(before));
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, REFRESHED}, seeds(cluster)));
		}
		finally {
			cluster.close();
		}
	}

	@Test
	void tripScopedToClusterInstance() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		Consumer<SystemSettings.Builder> settings = builder -> builder
			.discovery(ops -> ops
				.refreshInterval(Duration.ofMillis(250))
				.tendRefreshDeadline(Duration.ofMillis(50)));

		Cluster cluster = definition(provider, settings).connect();

		try {
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, REFRESHED}, seeds(cluster)));
			provider.sleepMillis = 100;
			SeedRefresher refresher = seedRefresher(cluster);
			await(refresher::isTripped);
			assertEquals(SeedRefresher.MAX_OVERRUNS, refresher.getOverrunCount());
			assertArrayEquals(new Host[] {DEAD_SEED}, seeds(cluster));

			int calls = provider.calls.get();
			ClusterTend tend = cluster.getClusterTend();
			int tends = tend.getInvalidNodeCount();
			await(() -> tend.getInvalidNodeCount() >= tends + 3);
			assertEquals(calls, provider.calls.get());
		}
		finally {
			cluster.close();
		}

		provider.sleepMillis = 0;
		TestSeedProvider fresh = new TestSeedProvider(true, true);
		fresh.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		Cluster freshCluster = definition(fresh, settings).connect();

		try {
			SeedRefresher refresher = seedRefresher(freshCluster);
			assertFalse(refresher.isTripped());
			assertEquals(0, refresher.getOverrunCount());
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, REFRESHED}, seeds(freshCluster)));
			assertFalse(refresher.isTripped());
		}
		finally {
			freshCluster.close();
		}
	}

	/**
	 * The SDK's default TLS name is the {@code TlsBuilder} TLS name, not the host name as in the
	 * legacy client, so this sets one.
	 */
	@Test
	void refreshedSeedsGetDefaultTlsName() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClusterDefinition def = definition(provider, intervals(250, 250))
			.withTlsConfig(tls -> tls.tlsName("tls1"));

		Cluster cluster = def.connect();

		try {
			assertEquals("tls1", seeds(cluster)[0].tlsName, "initial seed");
			await(() -> seeds(cluster).length == 2);
			assertEquals(REFRESHED, seeds(cluster)[1]);
			assertEquals("tls1", seeds(cluster)[1].tlsName, "refreshed seed");
		}
		finally {
			cluster.close();
		}
	}

	@Test
	void concurrentRefreshNeverExposesPartialList() throws Exception {
		Cluster cluster = definition(null).connect();
		ClusterTend tend = cluster.getClusterTend();

		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> {
			List<Host> list = new ArrayList<>();

			for (int i = 0; i <= call % 7; i++) {
				list.add(new Host("10.0.0." + i, call));
			}
			return list;
		};

		DiscoverySettings ds = SystemSettings.builder()
			.discovery(ops -> ops
				.executionMode(DiscoveryExecutionMode.TEND)
				.refreshInterval(Duration.ofMillis(1000))
				.tendRefreshDeadline(Duration.ofMillis(60000)))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery();
		SeedRefresher refresher = new SeedRefresher(provider, DEF, ds, tend::setSeeds);

		AtomicBoolean running = new AtomicBoolean(true);
		AtomicReference<String> failure = new AtomicReference<>();
		Set<Integer> observed = ConcurrentHashMap.newKeySet();
		Thread[] readers = new Thread[4];

		for (int r = 0; r < readers.length; r++) {
			readers[r] = new Thread(() -> {
				while (running.get()) {
					Host[] seeds = seeds(cluster);
					int call = seeds[0].port;

					if (call == DEAD_SEED.port) {
						continue;
					}

					observed.add(call);

					if (seeds.length != call % 7 + 1) {
						failure.compareAndSet(null, "length " + seeds.length + " for refresh " + call);
					}

					for (int i = 0; i < seeds.length; i++) {
						Host host = seeds[i];

						if (host == null || host.port != call || ! host.name.equals("10.0.0." + i)) {
							failure.compareAndSet(null, "mixed host " + host + " in refresh " + call);
						}
					}
				}
			});
			readers[r].start();
		}

		refresher.start(seeds(cluster));
		Thread writer = new Thread(() -> {
			for (int tendCount = 1; running.get() && provider.calls.get() < 20000; tendCount++) {
				refresher.tend(tendCount, 1000);
			}
		});
		writer.start();

		try {
			await(() -> provider.calls.get() >= 20000);
		}
		finally {
			running.set(false);
			writer.join(5000);

			for (Thread reader : readers) {
				reader.join(5000);
			}
			cluster.close();
		}

		assertNull(failure.get(), failure.get());
		assertTrue(observed.size() > 10, "observed " + observed.size());
	}

	private static final Field SEEDS;
	private static final Field SEED_REFRESHER;

	static {
		try {
			SEEDS = ClusterTend.class.getDeclaredField("seeds");
			SEEDS.setAccessible(true);
			SEED_REFRESHER = Cluster.class.getDeclaredField("seedRefresher");
			SEED_REFRESHER.setAccessible(true);
		}
		catch (NoSuchFieldException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	private static Host[] seeds(Cluster cluster) {
		try {
			return (Host[])SEEDS.get(cluster.getClusterTend());
		}
		catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
	}

	private static SeedRefresher seedRefresher(Cluster cluster) throws Exception {
		return (SeedRefresher)SEED_REFRESHER.get(cluster);
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

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}
}
