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
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
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
import com.aerospike.client.sdk.discovery.SeedCandidateContext;
import com.aerospike.client.sdk.discovery.SeedCandidateProvider;
import com.aerospike.client.sdk.discovery.SeedMergePolicy;
import com.aerospike.client.sdk.discovery.StaticSeedCandidateProvider;

class SeedCandidateProviderSpiTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host OTHER_SEED = new Host("127.0.0.1", 2);
	private static final String NAME = "spi";

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}; WARN messages from every thread are the legacy warnings.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;

	static final class Provider implements SeedCandidateProvider {
		final List<SeedCandidateContext> contexts = new CopyOnWriteArrayList<>();
		final AtomicInteger closes = new AtomicInteger();
		private final boolean periodic;
		volatile RuntimeException refreshError;
		volatile RuntimeException closeError;
		volatile int callsAtClose = -1;

		Provider(boolean periodic) {
			this.periodic = periodic;
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			contexts.add(ctx);

			if (refreshError != null) {
				throw refreshError;
			}
			return List.of();
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
			callsAtClose = contexts.size();
			closes.incrementAndGet();

			if (closeError != null) {
				throw closeError;
			}
		}
	}

	@BeforeEach
	void setUp() {
		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
	}

	@AfterEach
	void tearDown() {
		System.setErr(originalErr);
	}

	/**
	 * Tend execution mode, 100 ms tend timeout, 250 ms tend interval, 500 ms discovery refresh
	 * interval and {@code provider} registered as {@link #NAME}. {@code settings} adds to the
	 * system settings; repeated {@code discovery} calls add to the same settings.
	 */
	private static ClusterDefinition definition(SeedCandidateProvider provider, Consumer<SystemSettings.Builder> settings,
		Host... seeds) {
		return new ClusterDefinition(seeds)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> {
				builder
					.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)))
					.discovery(ops -> ops
						.executionMode(DiscoveryExecutionMode.TEND)
						.refreshInterval(Duration.ofMillis(500))
						.provider(NAME)
						.addProviderFactory(NAME, options -> provider));
				settings.accept(builder);
			});
	}

	private static ClusterDefinition definition(SeedCandidateProvider provider, Host... seeds) {
		return definition(provider, builder -> {}, seeds);
	}

	/**
	 * Discovery settings for a {@link SeedRefresher} built directly, as {@link #definition} sets them.
	 */
	private static DiscoverySettings discovery(SeedMergePolicy mergePolicy) {
		return SystemSettings.builder()
			.discovery(ops -> ops
				.executionMode(DiscoveryExecutionMode.TEND)
				.refreshInterval(Duration.ofMillis(500))
				.mergePolicy(mergePolicy))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery();
	}

	@Test
	void providerFailureAtInitKeepsStaticSeeds() throws Exception {
		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			Provider provider = new Provider(false);
			provider.refreshError = new IllegalStateException("cloud API down");
			stderr.reset();

			try (Cluster cluster = definition(provider, builder -> builder.discovery(ops -> ops.mergePolicy(mergePolicy)),
				DEAD_SEED).connect()) {
				assertArrayEquals(new Host[] {DEAD_SEED}, seeds(cluster));
				assertEquals(1, provider.contexts.size());
				assertTrue(warnings().stream().anyMatch(m ->
					m.startsWith("Seed candidate provider " + Provider.class.getName() + " failed") &&
					m.contains("cloud API down")), mergePolicy + " " + warnings());
			}
		}
	}

	@Test
	void providerFailureAtInitWithNoSeedFails() {
		Provider provider = new Provider(false);
		IllegalStateException error = new IllegalStateException("cloud API down");
		provider.refreshError = error;

		AerospikeException ae = assertThrows(AerospikeException.class, () -> definition(provider).connect());

		assertEquals(ResultCode.SEED_DISCOVERY_FAILED, ae.getResultCode());
		assertTrue(ae.getMessage().contains(Provider.class.getName()), ae.getMessage());
		assertTrue(ae.getMessage().contains("cloud API down"), ae.getMessage());
		assertSame(error, ae.getCause());
		assertTrue(warnings().stream().anyMatch(m ->
			m.startsWith("Seed candidate provider " + Provider.class.getName() + " failed")), warnings().toString());
	}

	@Test
	void emptyResultWithNoSeedIsNotDiscoveryFailure() throws Exception {
		Provider provider = new Provider(false);

		try (Cluster cluster = definition(provider).connect()) {
			assertEquals(0, seeds(cluster).length);
		}
	}

	@Test
	void contextAtInitAndOnTend() throws Exception {
		Provider provider = new Provider(true);
		ClusterDefinition def = definition(provider, DEAD_SEED, OTHER_SEED).clusterName("ctx-cluster");

		try (Cluster cluster = def.connect()) {
			await(() -> provider.contexts.size() >= 3);
		}

		List<SeedCandidateContext> contexts = new ArrayList<>(provider.contexts);
		long previous = -1;

		for (int i = 0; i < contexts.size(); i++) {
			SeedCandidateContext ctx = contexts.get(i);

			assertEquals("ctx-cluster", ctx.clusterName);
			assertEquals(List.of(DEAD_SEED, OTHER_SEED), ctx.configuredSeeds);

			if (i == 0) {
				assertEquals(0, ctx.tendIteration);
			}
			else {
				assertTrue(ctx.tendIteration > previous, contexts.toString());
				assertEquals(0, ctx.tendIteration % 2, contexts.toString());
			}
			previous = ctx.tendIteration;
		}
		assertThrows(UnsupportedOperationException.class, () -> contexts.get(0).configuredSeeds.add(DEAD_SEED));
	}

	@Test
	void contextClusterNameNullWhenNotSet() {
		Provider provider = new Provider(false);

		try (Cluster cluster = definition(provider, DEAD_SEED).connect()) {
			assertNull(provider.contexts.get(0).clusterName);
			assertEquals(0, provider.contexts.get(0).tendIteration);
		}
	}

	@Test
	void supportsDeadlineDefaultsToFalse() {
		SeedCandidateProvider provider = new SeedCandidateProvider() {
			@Override
			public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
				return List.of();
			}

			@Override
			public boolean needsPeriodicRefresh() {
				return true;
			}
		};
		assertFalse(provider.supportsDeadline());

		AerospikeException ae = assertThrows(AerospikeException.class, () -> definition(provider, DEAD_SEED).connect());
		assertEquals("A seed candidate provider that needs periodic refresh must support a deadline in " +
			"discovery execution mode tend", ae.getBaseMessage());

		definition(provider, builder -> builder.discovery(ops -> ops.executionMode(DiscoveryExecutionMode.THREAD)),
			DEAD_SEED).connect().close();
	}

	@Test
	void hostWithPortOutOfRangeIsDropped() {
		Host low = new Host("10.0.0.1", 0);
		Host min = new Host("10.0.0.2", 1);
		Host high = new Host("10.0.0.3", 65536);
		Host max = new Host("10.0.0.4", 65535);
		TestSeedProvider provider = new TestSeedProvider(true, true, low, min, high, max);
		AtomicReference<Host[]> snapshot = new AtomicReference<>();
		SeedRefresher r = new SeedRefresher(provider, new ClusterDefinition(), discovery(SeedMergePolicy.REPLACE),
			new Host[0], snapshot::set);

		Host[] seeds = r.discover();
		assertArrayEquals(new Host[] {min, max}, seeds);
		r.start(seeds);

		provider.result = call -> List.of(high, max, low);
		r.tend(2, 250);
		assertArrayEquals(new Host[] {max}, snapshot.get());

		String prefix = "Seed candidate provider " + TestSeedProvider.class.getName() + " returned host ";
		assertEquals(List.of(
			prefix + "10.0.0.1 with port 0 outside 1-65535. Host dropped",
			prefix + "10.0.0.3 with port 65536 outside 1-65535. Host dropped",
			prefix + "10.0.0.3 with port 65536 outside 1-65535. Host dropped",
			prefix + "10.0.0.1 with port 0 outside 1-65535. Host dropped"), warnings());
	}

	@Test
	void builtInProviderHostWithPortOutOfRangeIsDropped() {
		Host valid = new Host("10.0.0.2", 3000);
		SeedRefresher r = new SeedRefresher(new StaticSeedCandidateProvider(new Host("10.0.0.1", 65536), valid),
			new ClusterDefinition(), discovery(SeedMergePolicy.DISCOVERY_ONLY), new Host[0], hosts -> {});

		assertArrayEquals(new Host[] {valid}, r.discover());
		assertEquals(List.of(
			"Seed candidate provider static returned host 10.0.0.1 with port 65536 outside 1-65535. Host dropped"),
			warnings());
	}

	@Test
	void closeCalledOnceAfterRefreshStops() throws Exception {
		Provider provider = new Provider(true);
		Cluster cluster = definition(provider, DEAD_SEED).connect();
		await(() -> provider.contexts.size() >= 2);

		cluster.close();
		await(() -> provider.closes.get() == 1);
		assertEquals(provider.callsAtClose, provider.contexts.size());

		int tends = cluster.getInvalidNodeCount();
		Thread.sleep(1000);
		cluster.close();

		assertEquals(1, provider.closes.get());
		assertEquals(provider.callsAtClose, provider.contexts.size());
		assertTrue(cluster.getInvalidNodeCount() <= tends + 1);
	}

	@Test
	void closeExceptionIsLoggedNotThrown() throws Exception {
		Provider provider = new Provider(false);
		provider.closeError = new IllegalStateException("close boom");

		Cluster cluster = definition(provider, DEAD_SEED).connect();
		cluster.close();

		await(() -> provider.closes.get() == 1);
		await(() -> warnings().stream().anyMatch(m -> m.contains("close boom")));
		assertTrue(warnings().stream().anyMatch(m ->
			m.startsWith("Seed candidate provider " + Provider.class.getName() + " close failed") &&
			m.contains("close boom")), warnings().toString());
	}

	@Test
	void sharedProviderClosedByEachClient() throws Exception {
		Provider provider = new Provider(false);
		Cluster c1 = definition(provider, DEAD_SEED).connect();
		Cluster c2 = definition(provider, DEAD_SEED).connect();

		c1.close();
		await(() -> provider.closes.get() == 1);
		c2.close();
		await(() -> provider.closes.get() == 2);
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

	/**
	 * The cluster's current seeds. The legacy test read {@code Cluster.getSeeds()}.
	 */
	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}
}
