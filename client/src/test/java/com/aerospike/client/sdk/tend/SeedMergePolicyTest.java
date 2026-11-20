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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.DiscoverySettings;
import com.aerospike.client.sdk.discovery.DiscoverySettings.DiscoveryTweaks;
import com.aerospike.client.sdk.discovery.SeedMergePolicy;

class SeedMergePolicyTest {
	private static final Host S1 = new Host("10.0.0.1", 3000);
	private static final Host S2 = new Host("10.0.0.2", 3000);
	private static final Host D1 = new Host("10.0.1.1", 3000);
	private static final Host D2 = new Host("10.0.1.2", 3000);
	private static final Host[] STATIC = new Host[] {S1, S2};
	private static final Host[] DISCOVERED = new Host[] {D1, new Host("10.0.0.2", "tls", 3000), D2, D1};
	private static final Host[] EMPTY = new Host[0];
	private static final ClusterDefinition DEF = new ClusterDefinition(STATIC);

	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private final AtomicInteger publishCount = new AtomicInteger();

	private static DiscoverySettings discovery(Consumer<DiscoveryTweaks> tweaks) {
		return SystemSettings.builder()
			.discovery(ops -> {
				ops.executionMode(DiscoveryExecutionMode.TEND);
				tweaks.accept(ops);
			})
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery();
	}

	private SeedRefresher create(SeedMergePolicy mergePolicy, TestSeedProvider provider, Host[] staticSeeds) {
		DiscoverySettings ds = discovery(ops -> ops
			.mergePolicy(mergePolicy)
			.refreshInterval(Duration.ofMillis(1000))
			.tendRefreshDeadline(Duration.ofMillis(20)));
		return refresher(provider, ds, staticSeeds);
	}

	private SeedRefresher refresher(TestSeedProvider provider, DiscoverySettings ds, Host[] staticSeeds) {
		return new SeedRefresher(provider, DEF, ds, staticSeeds, hosts -> {
			publishCount.incrementAndGet();
			snapshot.set(hosts);
		});
	}

	private Host[] init(SeedRefresher refresher, Host[] discovered) {
		Host[] seeds = refresher.merge(discovered, true);
		snapshot.set(seeds);
		refresher.start(seeds);
		return seeds;
	}

	/**
	 * The legacy test checked the ClientPolicy field default, setter and copy constructor. The SDK
	 * merge policy is a discovery system setting; a copied cluster definition keeps it.
	 */
	@Test
	void policyDefaultSetterAndCopy() {
		assertEquals(SeedMergePolicy.MERGE, SystemSettings.DEFAULT.getDiscovery().getMergePolicy());
		assertEquals(SeedMergePolicy.MERGE, discovery(ops -> {}).getMergePolicy());

		for (SeedMergePolicy mergePolicy : new SeedMergePolicy[] {SeedMergePolicy.DISCOVERY_ONLY, SeedMergePolicy.REPLACE}) {
			assertEquals(mergePolicy, discovery(ops -> ops.mergePolicy(mergePolicy)).getMergePolicy());

			ClusterDefinition def = new ClusterDefinition("127.0.0.1", 1)
				.failIfNotConnected(false)
				.tendTimeout(100)
				.withSystemSettings(builder -> builder.discovery(ops -> ops.mergePolicy(mergePolicy)));

			try (Cluster cluster = new ClusterDefinition(def).connect()) {
				assertEquals(mergePolicy, cluster.getSystemSettings().getDiscovery().getMergePolicy());
			}
		}
	}

	@Test
	void mergeOrder() {
		SeedRefresher r = create(SeedMergePolicy.MERGE, new TestSeedProvider(true, true), STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1, D2}, r.merge(DISCOVERED, true));
		assertSame(S2, r.merge(DISCOVERED, true)[1]);
		assertArrayEquals(new Host[] {S1, S2, D1, D2}, r.merge(DISCOVERED, false));
		assertArrayEquals(STATIC, r.merge(EMPTY, true));
		assertArrayEquals(STATIC, r.merge(EMPTY, false));
	}

	@Test
	void replaceOrder() {
		SeedRefresher r = create(SeedMergePolicy.REPLACE, new TestSeedProvider(true, true), STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1, D2}, r.merge(DISCOVERED, true));
		assertArrayEquals(new Host[] {D1, S2, D2}, r.merge(DISCOVERED, false));
		assertEquals("tls", r.merge(DISCOVERED, false)[1].tlsName);
		assertArrayEquals(STATIC, r.merge(EMPTY, true));
		assertArrayEquals(EMPTY, r.merge(EMPTY, false));
	}

	@Test
	void discoveryOnlyOrder() {
		SeedRefresher r = create(SeedMergePolicy.DISCOVERY_ONLY, new TestSeedProvider(true, true), STATIC);
		assertArrayEquals(new Host[] {D1, S2, D2}, r.merge(DISCOVERED, true));
		assertArrayEquals(new Host[] {D1, S2, D2}, r.merge(DISCOVERED, false));
		assertArrayEquals(STATIC, r.merge(EMPTY, true));
		assertArrayEquals(STATIC, r.merge(EMPTY, false));
	}

	@Test
	void staticSeedsCopiedFromCaller() {
		Host[] hosts = new Host[] {S1, S2};
		SeedRefresher r = create(SeedMergePolicy.MERGE, new TestSeedProvider(true, true), hosts);
		hosts[0] = D2;
		assertArrayEquals(STATIC, r.merge(EMPTY, false));
		assertArrayEquals(new Host[] {D2, S2}, hosts);
	}

	@Test
	void mergeRefreshPublishesStaticThenDiscovered() {
		TestSeedProvider provider = new TestSeedProvider(true, true, D2);
		SeedRefresher r = create(SeedMergePolicy.MERGE, provider, STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1}, init(r, new Host[] {D1}));

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {S1, S2, D2}, snapshot.get());

		provider.result = call -> List.of();
		r.tend(2, 1000);
		assertArrayEquals(STATIC, snapshot.get());
	}

	@Test
	void replaceDropsStaticAfterInitAndPublishesEmpty() {
		TestSeedProvider provider = new TestSeedProvider(true, true, D2, S1);
		SeedRefresher r = create(SeedMergePolicy.REPLACE, provider, STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1}, init(r, new Host[] {D1}));

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {D2, S1}, snapshot.get());

		provider.result = call -> List.of();
		r.tend(2, 1000);
		assertArrayEquals(EMPTY, snapshot.get());
		assertEquals(2, publishCount.get());
	}

	@Test
	void discoveryOnlyFallsBackToStaticOnEmpty() {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		SeedRefresher r = create(SeedMergePolicy.DISCOVERY_ONLY, provider, STATIC);
		assertArrayEquals(STATIC, init(r, EMPTY));

		provider.result = call -> List.of(D1, D2);
		r.tend(1, 1000);
		assertArrayEquals(new Host[] {D1, D2}, snapshot.get());

		provider.result = call -> List.of();
		r.tend(2, 1000);
		assertArrayEquals(STATIC, snapshot.get());
	}

	@Test
	void failedOrOverrunRefreshKeepsSnapshotAndEmptyRefreshIsMerged() {
		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			for (Host[] staticSeeds : new Host[][] {STATIC, EMPTY}) {
				TestSeedProvider provider = new TestSeedProvider(true, true, D1);
				SeedRefresher r = create(mergePolicy, provider, staticSeeds);
				init(r, new Host[] {D2});

				r.tend(1, 1000);
				Host[] previous = snapshot.get();
				String label = mergePolicy + " static " + staticSeeds.length;

				provider.error = new RuntimeException("fail");
				r.tend(2, 1000);
				assertSame(previous, snapshot.get(), label);

				provider.error = null;
				provider.result = call -> null;
				r.tend(3, 1000);
				assertSame(previous, snapshot.get(), label);

				provider.result = call -> List.of();
				provider.sleepMillis = 40;
				r.tend(4, 1000);
				assertSame(previous, snapshot.get(), label);
				assertTrue(snapshot.get().length > 0, label);

				provider.sleepMillis = 0;
				r.tend(5, 1000);
				assertArrayEquals((mergePolicy == SeedMergePolicy.REPLACE)? EMPTY : staticSeeds, snapshot.get(), label);
			}
		}
	}

	@Test
	void tripRestoresMergedInitSnapshot() {
		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			TestSeedProvider provider = new TestSeedProvider(true, true, D2);
			SeedRefresher r = create(mergePolicy, provider, STATIC);
			Host[] initSeeds = init(r, new Host[] {D1});

			r.tend(1, 1000);
			provider.sleepMillis = 40;

			for (int i = 1; i <= SeedRefresher.MAX_OVERRUNS; i++) {
				r.tend(1 + i, 1000);
			}
			assertTrue(r.isTripped());
			assertSame(initSeeds, snapshot.get(), mergePolicy.toString());
		}
		assertArrayEquals(new Host[] {D1}, snapshot.get());
	}

	@Test
	void mergeRunsOnlyWhenSnapshotPublished() {
		TestSeedProvider provider = new TestSeedProvider(true, true, D1);
		SeedRefresher r = refresher(provider, discovery(ops -> ops.refreshInterval(Duration.ofMillis(10000))), STATIC);
		Host[] initSeeds = init(r, EMPTY);

		for (int tendCount = 1; tendCount < 10; tendCount++) {
			r.tend(tendCount, 1000);
			assertSame(initSeeds, snapshot.get());
		}
		assertEquals(0, publishCount.get());

		r.tend(10, 1000);
		assertEquals(1, publishCount.get());
		assertArrayEquals(new Host[] {S1, S2, D1}, snapshot.get());
	}

	/**
	 * The SDK's default TLS name is the {@code TlsBuilder} TLS name, not the host name as in the
	 * legacy client, so this sets one.
	 */
	@Test
	void noProviderSnapshotEqualsStaticHosts() throws Exception {
		Host[] hosts = new Host[] {new Host("127.0.0.1", 3), new Host("127.0.0.1", 1), new Host("127.0.0.1", 2)};

		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			for (boolean tls : new boolean[] {false, true}) {
				ClusterDefinition def = new ClusterDefinition(hosts)
					.failIfNotConnected(false)
					.tendTimeout(100)
					.withSystemSettings(builder -> builder.discovery(ops -> ops
						.executionMode(DiscoveryExecutionMode.TEND)
						.mergePolicy(mergePolicy)));

				if (tls) {
					def.withTlsConfig(builder -> builder.tlsName("tls1"));
				}

				try (Cluster cluster = def.connect()) {
					Host[] seeds = seeds(cluster);
					assertArrayEquals(hosts, seeds, mergePolicy.toString());

					for (int i = 0; i < hosts.length; i++) {
						assertNull(hosts[i].tlsName);
						assertEquals(tls ? "tls1" : null, seeds[i].tlsName);
					}
				}
			}
		}
	}

	@Test
	void clusterTripRestoresMergedInitSnapshot() throws Exception {
		Host deadSeed = new Host("127.0.0.1", 1);
		Host initDiscovered = new Host("127.0.0.1", 2);
		Host refreshed = new Host("127.0.0.1", 3);
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(initDiscovered) : List.of(refreshed);
		ClusterDefinition def = new ClusterDefinition(deadSeed)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)))
				.discovery(ops -> ops
					.executionMode(DiscoveryExecutionMode.TEND)
					.refreshInterval(Duration.ofMillis(250))
					.tendRefreshDeadline(Duration.ofMillis(50))
					.provider(TestSeedProvider.NAME)
					.addProviderFactory(TestSeedProvider.NAME, options -> provider)));

		try (Cluster cluster = def.connect()) {
			await(() -> Arrays.equals(new Host[] {deadSeed, refreshed}, seeds(cluster)));

			provider.sleepMillis = 100;
			SeedRefresher refresher = seedRefresher(cluster);
			await(refresher::isTripped);
			assertArrayEquals(new Host[] {deadSeed, initDiscovered}, seeds(cluster));
		}
	}

	/**
	 * The cluster's current seeds. The legacy test read {@code Cluster.getSeeds()}.
	 */
	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
	}

	private static SeedRefresher seedRefresher(Cluster cluster) throws Exception {
		Field field = Cluster.class.getDeclaredField("seedRefresher");
		field.setAccessible(true);
		return (SeedRefresher)field.get(cluster);
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
