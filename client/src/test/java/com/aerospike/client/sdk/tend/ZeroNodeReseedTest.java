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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;

public class ZeroNodeReseedTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final String SEED_PREFIX = "Seed 127.0.0.1 ";

	private final List<String> events = new ArrayList<>();
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
			if (! line.contains("] WARN ")) {
				return;
			}

			String message = line.substring(line.indexOf(" - ") + 3);

			while (message.startsWith("aerospike.")) {
				message = message.substring(message.indexOf(' ') + 1);
			}

			if (message.startsWith(SEED_PREFIX)) {
				event("seed " + message.substring(SEED_PREFIX.length(), message.indexOf(' ', SEED_PREFIX.length())));
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
		System.setErr(originalErr);
	}

	private void event(String event) {
		synchronized (events) {
			events.add(event);
		}
	}

	private List<String> events() {
		synchronized (events) {
			return new ArrayList<>(events);
		}
	}

	private static ClusterDefinition definition(TestSeedProvider provider, DiscoveryExecutionMode mode,
		int refreshInterval) {
		return new ClusterDefinition(DEAD_SEED)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)))
				.discovery(ops -> ops
					.executionMode(mode)
					.refreshInterval(Duration.ofMillis(refreshInterval))
					.provider(TestSeedProvider.NAME)
					.addProviderFactory(TestSeedProvider.NAME, options -> provider)));
	}

	@Test
	void reseedUsesSnapshotHeldAtStartOfTend() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> {
			event("refresh " + call);
			return List.of(new Host("127.0.0.1", 10 + call));
		};

		Set<Thread> before = Thread.getAllStackTraces().keySet();

		try (Cluster cluster = definition(provider, DiscoveryExecutionMode.TEND, 250).connect()) {
			await(() -> provider.calls.get() >= 4);
			assertEquals(List.of("tend"), newThreadNames(before));
		}

		List<String> expected = List.of(
			"refresh 1",
			"seed 1", "seed 11",
			"seed 1", "seed 11", "refresh 2",
			"seed 1", "seed 12", "refresh 3",
			"seed 1", "seed 13", "refresh 4");
		assertEquals(expected, events().subList(0, expected.size()));
	}

	@Test
	void threadModeReseedUsesSnapshotFromBeforeUrgentRefresh() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> {
			event("refresh " + call);
			return List.of(new Host("127.0.0.1", 10 + call));
		};

		try (Cluster cluster = definition(provider, DiscoveryExecutionMode.THREAD, 60000).connect()) {
			await(() -> seedAttempts().size() >= 6);
			assertEquals(2, provider.calls.get());
		}

		assertEquals(List.of("seed 1", "seed 11", "seed 1", "seed 11", "seed 1", "seed 12"),
			seedAttempts().subList(0, 6));
		assertTrue(events().indexOf("refresh 2") < events().indexOf("seed 12"));
	}

	@Test
	void reseedDoesNotWaitForRefreshInterval() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, new Host("127.0.0.1", 11));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		long begin = System.nanoTime();

		try (Cluster cluster = definition(provider, DiscoveryExecutionMode.TEND, 60000).connect()) {
			Host[] seeds = seeds(cluster);
			await(() -> events().size() >= 8);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			assertTrue(elapsed < 5000, "three re-seeds took " + elapsed + "ms");
			assertEquals(1, provider.calls.get());
			assertSame(seeds, seeds(cluster));
			assertEquals(0, cluster.getNodes().length);
			assertEquals(List.of("tend"), newThreadNames(before));
		}

		List<String> attempts = events();

		for (int i = 0; i < attempts.size(); i++) {
			assertEquals(i % 2 == 0 ? "seed 1" : "seed 11", attempts.get(i));
		}
	}

	private List<String> seedAttempts() {
		List<String> attempts = events();
		attempts.removeIf(e -> ! e.startsWith("seed "));
		return attempts;
	}

	/**
	 * The cluster's current seeds. The legacy test read {@code Cluster.getSeeds()}.
	 */
	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
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
