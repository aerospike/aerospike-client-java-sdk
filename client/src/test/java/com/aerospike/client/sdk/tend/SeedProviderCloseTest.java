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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;

public class SeedProviderCloseTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);

	/**
	 * A construction failure: a change to the cluster definition.
	 */
	private record Failure(String name, Consumer<ClusterDefinition> def) {
		@Override
		public String toString() {
			return name;
		}
	}

	private static final Failure NOT_CONNECTED = new Failure("NOT_CONNECTED", def -> def.failIfNotConnected(true));
	private static final Failure SINGLE_NODE = new Failure("SINGLE_NODE", def -> def.forceSingleNode(true));
	private static final Failure NONE = new Failure("NONE", def -> {});

	/**
	 * The legacy minConnsPerNode > maxConnsPerNode case is omitted: the SDK does not reject it.
	 */
	private static final List<Failure> FAILURES = List.of(NOT_CONNECTED, SINGLE_NODE);

	@Test
	void tendModeClosesProviderOnceWhenConstructionFails() throws Exception {
		for (Failure failure : FAILURES) {
			assertClosedOnce(new TestSeedProvider(true, true, DEAD_SEED), DiscoveryExecutionMode.TEND, failure);
		}
	}

	@Test
	void rejectedProviderIsClosedOnce() throws Exception {
		assertClosedOnce(new TestSeedProvider(true, false, DEAD_SEED), DiscoveryExecutionMode.TEND, NONE);
	}

	@Test
	void nonPeriodicProviderIsClosedOnceWhenConstructionFails() throws Exception {
		for (Failure failure : FAILURES) {
			assertClosedOnce(new TestSeedProvider(false, false, DEAD_SEED), DiscoveryExecutionMode.THREAD, failure);
		}
	}

	@Test
	void threadModeClosesProviderOnceWhenConstructionFails() throws Exception {
		for (Failure failure : FAILURES) {
			assertClosedOnce(new TestSeedProvider(true, false, DEAD_SEED), DiscoveryExecutionMode.THREAD, failure);
		}
	}

	private static void assertClosedOnce(TestSeedProvider provider, DiscoveryExecutionMode mode, Failure failure)
		throws Exception {
		ClusterDefinition def = new ClusterDefinition(DEAD_SEED)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(builder -> builder
				.refresh(ops -> ops.tendInterval(Duration.ofMillis(250)))
				.discovery(ops -> ops
					.executionMode(mode)
					.provider(TestSeedProvider.NAME)
					.addProviderFactory(TestSeedProvider.NAME, options -> provider)));
		failure.def().accept(def);

		assertThrows(AerospikeException.class, def::connect, failure.toString());

		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (provider.closes.get() == 0) {
			assertTrue(System.nanoTime() < deadline, failure + ": provider not closed in time");
			Thread.sleep(5);
		}
		Thread.sleep(300);
		assertEquals(1, provider.closes.get(), failure.toString());
	}
}
