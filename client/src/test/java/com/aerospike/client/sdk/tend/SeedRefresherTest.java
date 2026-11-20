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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.DiscoverySettings;

class SeedRefresherTest {
	private static final Host[] INIT = new Host[] {new Host("10.0.0.1", 3000)};
	private static final Host A = new Host("10.0.0.2", 3000);
	private static final Host B = new Host("10.0.0.3", 3000);
	private static final ClusterDefinition DEF = new ClusterDefinition(INIT);

	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}. The refresher runs in tend mode, so it logs on the test thread.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;
	private String thread;

	@BeforeEach
	void setUp() {
		thread = "[" + Thread.currentThread().getName() + "] ";
		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
		snapshot.set(INIT);
	}

	@AfterEach
	void tearDown() {
		System.setErr(originalErr);
	}

	private static DiscoverySettings discovery(int refreshInterval, int tendRefreshDeadline) {
		return SystemSettings.builder()
			.discovery(ops -> ops
				.executionMode(DiscoveryExecutionMode.TEND)
				.refreshInterval(Duration.ofMillis(refreshInterval))
				.tendRefreshDeadline(Duration.ofMillis(tendRefreshDeadline)))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery();
	}

	private SeedRefresher create(TestSeedProvider provider, int refreshInterval) {
		SeedRefresher refresher = new SeedRefresher(provider, DEF, discovery(refreshInterval, 20), snapshot::set);
		refresher.start(INIT);
		return refresher;
	}

	@Test
	void callsProviderOncePerThirtyTends() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, 30000);

		for (int tendCount = 1; tendCount <= 89; tendCount++) {
			r.tend(tendCount, 1000);
			assertEquals(tendCount / 30, provider.calls.get(), "tendCount " + tendCount);
		}
		r.tend(90, 1000);
		assertEquals(3, provider.calls.get());
		assertArrayEquals(new Host[] {A}, snapshot.get());
	}

	@Test
	void providerThrowableKeepsSnapshot() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, 1000);

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {A}, snapshot.get());
		Host[] lastGood = snapshot.get();

		Throwable[] throwables = new Throwable[] {
			new RuntimeException("runtime"), new Exception("checked"), new StackOverflowError(), null
		};

		for (Throwable t : throwables) {
			provider.error = t;
			provider.result = call -> (t == null)? null : List.of(B);
			r.tend(2, 1000);
			assertSame(lastGood, snapshot.get());
		}
		assertEquals(5, provider.calls.get());
		assertEquals(0, r.getOverrunCount());
		assertFalse(r.isTripped());
		assertEquals(4, logged("WARN").size(), logged("WARN").toString());

		provider.error = null;
		provider.result = call -> List.of(B);
		r.tend(3, 1000);
		assertArrayEquals(new Host[] {B}, snapshot.get());
	}

	@Test
	void overrunBlocksTendAndKeepsLastGood() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, 1000);

		r.tend(1, 1000);
		Host[] lastGood = snapshot.get();
		assertArrayEquals(new Host[] {A}, lastGood);

		provider.sleepMillis = 100;
		provider.result = call -> List.of(B);

		long begin = System.nanoTime();
		r.tend(2, 1000);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue(elapsed >= 100, "tend returned after " + elapsed + "ms");
		assertSame(lastGood, snapshot.get());
		assertEquals(1, r.getOverrunCount());
		assertFalse(r.isTripped());
		assertEquals(1, logged("WARN").size(), logged("WARN").toString());

		r.tend(3, 1000);
		assertEquals(2, r.getOverrunCount());

		provider.sleepMillis = 0;
		r.tend(4, 1000);
		assertEquals(0, r.getOverrunCount());
		assertArrayEquals(new Host[] {B}, snapshot.get());
	}

	@Test
	void threeConsecutiveOverrunsTrip() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, 1000);

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {A}, snapshot.get());

		provider.sleepMillis = 40;

		for (int i = 1; i <= SeedRefresher.MAX_OVERRUNS; i++) {
			r.tend(1 + i, 1000);
			assertEquals(i, r.getOverrunCount());
		}

		assertTrue(r.isTripped());
		assertSame(INIT, snapshot.get());
		assertEquals(1, logged("ERROR").size(), logged("ERROR").toString());

		provider.sleepMillis = 0;
		int calls = provider.calls.get();

		for (int tendCount = 10; tendCount < 100; tendCount++) {
			r.tend(tendCount, 1000);
		}
		assertEquals(calls, provider.calls.get());
		assertSame(INIT, snapshot.get());

		SeedRefresher fresh = new SeedRefresher(provider, DEF, SystemSettings.builder()
			.discovery(ops -> ops.executionMode(DiscoveryExecutionMode.TEND))
			.build()
			.mergeWith(SystemSettings.DEFAULT)
			.getDiscovery(), snapshot::set);
		fresh.start(INIT);
		assertFalse(fresh.isTripped());
		assertEquals(0, fresh.getOverrunCount());
		fresh.tend(30, 1000);
		assertEquals(calls + 1, provider.calls.get());
		assertArrayEquals(new Host[] {A}, snapshot.get());
	}

	@Test
	void noPeriodicRefreshDoesNoWork() {
		TestSeedProvider provider = new TestSeedProvider(false, true, A);
		SeedRefresher r = create(provider, 1000);

		for (int tendCount = 1; tendCount <= 100; tendCount++) {
			r.tend(tendCount, 1000);
		}
		assertEquals(0, provider.calls.get());
		assertSame(INIT, snapshot.get());
	}

	/**
	 * Messages logged on the test thread at the given level.
	 */
	private List<String> logged(String level) {
		String prefix = thread + level + ' ';
		return stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.startsWith(prefix))
			.map(line -> line.substring(line.indexOf(" - ") + 3))
			.toList();
	}
}
