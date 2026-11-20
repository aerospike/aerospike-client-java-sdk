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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.Endpoint;

public class AddressTranslatorCloseTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);

	/**
	 * slf4j-simple writes to the current {@code System.err} on every call, so warnings are
	 * captured by redirecting it for the duration of each test.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;

	static final class Translator implements AddressTranslator {
		final AtomicInteger closes = new AtomicInteger();
		volatile Cluster cluster;
		volatile String closeThread;
		volatile boolean activeAtClose;
		volatile RuntimeException closeError;

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.empty();
		}

		@Override
		public void close() {
			closeThread = Thread.currentThread().getName();
			activeAtClose = cluster != null && cluster.isActive();
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

	private static ClusterDefinition definition(AddressTranslator translator) {
		return definition(translator, builder -> {});
	}

	/**
	 * Unreachable seed, 100 ms tend timeout, 250 ms tend interval. {@code settings} adds to the
	 * system settings; a later {@code withSystemSettings} call would replace them.
	 */
	private static ClusterDefinition definition(AddressTranslator translator, Consumer<SystemSettings.Builder> settings) {
		return new ClusterDefinition(DEAD_SEED)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.addressTranslator(translator)
			.withSystemSettings(builder -> {
				builder.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)));
				settings.accept(builder);
			});
	}

	@Test
	void closeCalledOnceAfterTendStops() throws Exception {
		Translator translator = new Translator();
		Cluster cluster = definition(translator).connect();
		translator.cluster = cluster;
		Thread tend = tendThread(cluster);
		assertEquals(0, translator.closes.get());

		cluster.close();
		await(() -> translator.closes.get() == 1);
		assertEquals("tend", translator.closeThread);
		assertFalse(translator.activeAtClose);
		tend.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse(tend.isAlive());

		cluster.close();
		Thread.sleep(500);
		assertEquals(1, translator.closes.get());
	}

	@Test
	void closeCalledOnceWhenConstructionFails() {
		// The legacy minConnsPerNode > maxConnsPerNode case is omitted: the SDK does not reject it.
		List<Consumer<Translator>> failures = List.of(
			translator -> {
				TestSeedProvider provider = new TestSeedProvider(true, false, DEAD_SEED);
				definition(translator, builder -> builder
					.discovery(ops -> ops
						.provider(TestSeedProvider.NAME)
						.addProviderFactory(TestSeedProvider.NAME, options -> provider)
						.executionMode(DiscoveryExecutionMode.TEND)))
					.connect();
			},
			translator -> definition(translator).failIfNotConnected(true).connect(),
			translator -> definition(translator).forceSingleNode(true).connect());

		for (int i = 0; i < failures.size(); i++) {
			Consumer<Translator> failure = failures.get(i);
			Translator translator = new Translator();

			assertThrows(AerospikeException.class, () -> failure.accept(translator), "failure " + i);
			assertEquals(1, translator.closes.get(), "failure " + i);
			assertEquals(Thread.currentThread().getName(), translator.closeThread, "failure " + i);
		}
	}

	@Test
	void closeExceptionDoesNotStopShutdown() throws Exception {
		Translator translator = new Translator();
		translator.closeError = new IllegalStateException("close boom");
		Cluster cluster = definition(translator).connect();
		Thread tend = tendThread(cluster);

		cluster.close();
		await(() -> translator.closes.get() == 1);
		tend.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse(tend.isAlive());

		String log = stderr.toString(StandardCharsets.UTF_8);
		assertTrue(log.lines().anyMatch(m ->
			m.contains("WARN") &&
			m.contains("Address translator " + Translator.class.getName() + " close failed") &&
			m.contains("close boom")), log);

		Translator failing = new Translator();
		failing.closeError = new IllegalStateException("close boom");
		ClusterDefinition def = definition(failing).forceSingleNode(true);

		AerospikeException ae = assertThrows(AerospikeException.class, def::connect);
		assertTrue(ae.getMessage().contains("Seed " + DEAD_SEED + " failed"), ae.getMessage());
		assertEquals(1, failing.closes.get());
	}

	private static Thread tendThread(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("tendThread");
		field.setAccessible(true);
		return (Thread)field.get(cluster.getClusterTend());
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}
}
