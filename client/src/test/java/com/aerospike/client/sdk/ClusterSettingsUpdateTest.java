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
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.SeedCandidateContext;
import com.aerospike.client.sdk.discovery.SeedCandidateProvider;
import com.aerospike.client.sdk.tend.ClusterTend;

/**
 * Dynamic system settings updates ({@link Cluster#applySystemSettings}) on a running cluster: a
 * new seed candidate provider replaces the old one, and a rejected update changes nothing.
 */
class ClusterSettingsUpdateTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host NEW_HOST = new Host("127.0.0.2", 3000);

	static final class Provider implements SeedCandidateProvider {
		final AtomicInteger calls = new AtomicInteger();
		final AtomicInteger closes = new AtomicInteger();
		private final boolean periodic;
		private final boolean deadline;
		private final List<Host> hosts;

		Provider(boolean periodic, boolean deadline, Host... hosts) {
			this.periodic = periodic;
			this.deadline = deadline;
			this.hosts = List.of(hosts);
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			calls.incrementAndGet();
			return hosts;
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return periodic;
		}

		@Override
		public boolean supportsDeadline() {
			return deadline;
		}

		@Override
		public void close() {
			closes.incrementAndGet();
		}
	}

	/**
	 * Settings using {@code provider}, registered as {@code name}, with a 1 s tend interval and
	 * refresh interval unless {@code extra} changes them.
	 */
	private static Consumer<SystemSettings.Builder> configure(String name, Provider provider,
		Consumer<SystemSettings.Builder> extra) {
		return builder -> {
			builder
				.refresh(ops -> ops.tendInterval(Duration.ofSeconds(1)))
				.discovery(ops -> ops
					.provider(name)
					.addProviderFactory(name, options -> provider)
					.refreshInterval(Duration.ofSeconds(1))
					.refreshTimeout(Duration.ofSeconds(1)));
			extra.accept(builder);
		};
	}

	private static SystemSettings settings(String name, Provider provider, Consumer<SystemSettings.Builder> extra) {
		SystemSettings.Builder builder = SystemSettings.builder();
		configure(name, provider, extra).accept(builder);
		return builder.build().mergeWith(SystemSettings.DEFAULT);
	}

	private static Cluster connect(Provider provider) {
		return new ClusterDefinition(DEAD_SEED)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(configure("old", provider, builder -> {}))
			.connect();
	}

	@Test
	void newProviderReplacesOldInTendMode() throws Exception {
		Provider oldProvider = new Provider(false, false, DEAD_SEED);
		Provider newProvider = new Provider(false, false, NEW_HOST);

		try (Cluster cluster = connect(oldProvider)) {
			cluster.applySystemSettings(settings("new", newProvider, builder -> {}));

			assertEquals(1, newProvider.calls.get(), "new provider refreshed");
			assertArrayEquals(new Host[] {DEAD_SEED, NEW_HOST}, seeds(cluster), "new seeds published");
			assertEquals(1, oldProvider.closes.get(), "old provider closed");
			assertEquals(0, newProvider.closes.get());
		}
		assertEquals(1, oldProvider.closes.get());
		assertEquals(1, newProvider.closes.get(), "new provider closed with the cluster");
	}

	@Test
	void newProviderReplacesOldInThreadMode() throws Exception {
		Provider oldProvider = new Provider(true, false, DEAD_SEED);
		Provider newProvider = new Provider(true, false, NEW_HOST);

		try (Cluster cluster = connect(oldProvider)) {
			cluster.applySystemSettings(settings("new", newProvider, builder -> {}));

			assertEquals(1, newProvider.calls.get(), "new discovery thread made its first refresh");
			assertArrayEquals(new Host[] {DEAD_SEED, NEW_HOST}, seeds(cluster), "new seeds published");
			assertEquals(1, oldProvider.closes.get(), "old discovery thread stopped and provider closed");
			assertEquals(0, newProvider.closes.get());
		}
		assertEquals(1, oldProvider.closes.get());
		assertEquals(1, newProvider.closes.get(), "new provider closed with the cluster");
	}

	@Test
	void rejectedProviderLeavesClusterUnchanged() throws Exception {
		Provider oldProvider = new Provider(false, false, DEAD_SEED);
		Provider newProvider = new Provider(true, false, NEW_HOST);

		try (Cluster cluster = connect(oldProvider)) {
			SystemSettings before = cluster.getSystemSettings();
			Host[] seedsBefore = seeds(cluster);

			// A periodic provider without deadline support is invalid in tend execution mode.
			SystemSettings rejected = settings("new", newProvider, builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofMillis(500)))
				.discovery(ops -> ops.executionMode(DiscoveryExecutionMode.TEND)));

			assertThrows(AerospikeException.class, () -> cluster.applySystemSettings(rejected));
			assertSame(before, cluster.getSystemSettings(), "effective settings kept");
			assertEquals(1000, cluster.getClusterDefinition().getTendInterval(), "tend interval kept");
			assertArrayEquals(seedsBefore, seeds(cluster), "seeds kept");
			assertEquals(1, newProvider.closes.get(), "rejected provider closed");
			assertEquals(0, oldProvider.closes.get(), "old provider still in use");
		}
		assertEquals(1, oldProvider.closes.get());
		assertEquals(1, newProvider.closes.get());
	}

	@Test
	void tendIntervalAboveRefreshIntervalIsRejected() throws Exception {
		Provider provider = new Provider(true, false, DEAD_SEED);

		try (Cluster cluster = connect(provider)) {
			SystemSettings before = cluster.getSystemSettings();

			// Only the tend interval changes, so the existing refresher is checked against it.
			SystemSettings rejected = SystemSettings.builder()
				.refresh(ops -> ops.tendInterval(Duration.ofSeconds(2)))
				.build()
				.mergeWith(before);

			assertThrows(AerospikeException.class, () -> cluster.applySystemSettings(rejected));
			assertSame(before, cluster.getSystemSettings());
			assertEquals(1000, cluster.getClusterDefinition().getTendInterval());
			assertEquals(0, provider.closes.get());
		}
		assertEquals(1, provider.closes.get());
	}

	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
	}
}
