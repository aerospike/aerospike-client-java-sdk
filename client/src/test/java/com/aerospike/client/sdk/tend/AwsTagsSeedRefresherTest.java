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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.sdk.discovery.AwsSettings;
import com.aerospike.client.sdk.discovery.DiscoveryExecutionMode;
import com.aerospike.client.sdk.discovery.DiscoverySettings;
import com.aerospike.client.sdk.discovery.DiscoverySettings.DiscoveryTweaks;
import com.aerospike.client.sdk.discovery.StaticDiscoveryConfig;
import com.aerospike.client.sdk.discovery.StubAwsServer;
import com.aerospike.client.sdk.discovery.StubAwsServer.Reply;

class AwsTagsSeedRefresherTest {
	private static final Host[] INIT = new Host[] {new Host("10.0.0.1", 3000)};
	private static final int MARGIN = 50;
	private static final Map<String,List<String>> FILTER = Map.of("tag:aerospike-cluster", List.of("prod-a"));
	private static final ClusterDefinition DEF = new ClusterDefinition("127.0.0.1", 1);

	/**
	 * slf4j-simple writes {@code [thread] LEVEL logger - message} lines to the current
	 * {@code System.err}; WARN and ERROR messages from every thread are the legacy warnings.
	 */
	private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
	private PrintStream originalErr;
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private StubAwsServer stub;

	@BeforeEach
	void setUp() throws Exception {
		originalErr = System.err;
		System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
		snapshot.set(INIT);
		stub = new StubAwsServer();
	}

	@AfterEach
	void tearDown() {
		stub.close();
		System.setErr(originalErr);
	}

	/**
	 * Effective settings, as the client builds them: {@code discovery} merged with the defaults.
	 */
	private static SystemSettings settings(Consumer<DiscoveryTweaks> discovery) {
		return SystemSettings.builder().discovery(discovery).build().mergeWith(SystemSettings.DEFAULT);
	}

	private Consumer<DiscoveryTweaks> aws(int timeout, String endpoint) {
		return ops -> ops
			.provider(StaticDiscoveryConfig.AWS_TAGS)
			.aws(aws -> aws
				.region("us-east-1")
				.filters(FILTER)
				.credentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", null)
				.endpoint(endpoint)
				.timeout(Duration.ofMillis(timeout)));
	}

	private AwsEc2TagSeedCandidateProvider provider(int timeout, String endpoint) {
		return provider(settings(aws(timeout, endpoint)));
	}

	private static AwsEc2TagSeedCandidateProvider provider(SystemSettings settings) {
		return new AwsEc2TagSeedCandidateProvider(settings.getDiscovery().getAws());
	}

	private static ClusterDefinition offline(Consumer<SystemSettings.Builder> settings, Host... seeds) {
		return new ClusterDefinition(seeds)
			.failIfNotConnected(false)
			.tendTimeout(100)
			.withSystemSettings(settings);
	}

	@Test
	void validateRejectsTimeoutAboveRefreshTimeout() {
		DiscoverySettings ds = SystemSettings.DEFAULT.getDiscovery();
		SeedRefresher r = new SeedRefresher(provider(10500, stub.url()), DEF, ds, snapshot::set);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> r.validate(1000));
		assertEquals("AWS EC2 seed provider timeout 10500 must be less or equal to the discovery refresh timeout 10000",
			ae.getBaseMessage());

		new SeedRefresher(provider(10000, stub.url()), DEF, ds, snapshot::set).validate(1000);

		DiscoverySettings longer = settings(ops -> ops.refreshTimeout(Duration.ofMillis(11000))).getDiscovery();
		new SeedRefresher(provider(10500, stub.url()), DEF, longer, snapshot::set).validate(1000);
	}

	@Test
	void providerNameIsAwsTags() {
		assertEquals(StaticDiscoveryConfig.AWS_TAGS, SeedRefresher.providerName(provider(80, stub.url())));
	}

	@Test
	void timeoutAboveRefreshTimeoutFailsClientConstruction() {
		ClusterDefinition def = offline(builder -> builder.discovery(aws(10500, stub.url())), new Host("127.0.0.1", 1));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> def.connect().close());
		assertEquals("AWS EC2 seed provider timeout 10500 must be less or equal to the discovery refresh timeout 10000",
			ae.getBaseMessage());
	}

	@Test
	void tendModeFailsClientConstruction() {
		stub.ec2Default(new Reply(200, StubAwsServer.describe(null,
			StubAwsServer.instance("i-1", "127.0.0.1", null, null, null))));
		ClusterDefinition def = offline(builder -> builder.discovery(aws(80, stub.url())
			.andThen(ops -> ops.executionMode(DiscoveryExecutionMode.TEND))), new Host("127.0.0.1", 1));

		AerospikeException ae = assertThrows(AerospikeException.class, () -> def.connect().close());
		assertEquals("A seed candidate provider that needs periodic refresh must support a deadline in " +
			"discovery execution mode tend", ae.getBaseMessage());
		assertEquals(0, stub.requests().size());
	}

	@Test
	void runsOnDiscoveryThreadAndPublishesInstances() throws Exception {
		stub.ec2Default(new Reply(200, StubAwsServer.describe(null,
			StubAwsServer.instance("i-1", "10.0.0.2", null, null, null))));
		DiscoverySettings ds = settings(ops -> ops.refreshInterval(Duration.ofMillis(100))).getDiscovery();
		SeedRefresher r = new SeedRefresher(provider(2000, stub.url()), DEF, ds, snapshot::set);
		r.validate(100);

		try {
			assertArrayEquals(new Host[] {new Host("10.0.0.2", 3000)}, r.discover());
			r.start(INIT);

			stub.ec2Default(new Reply(200, StubAwsServer.describe(null,
				StubAwsServer.instance("i-1", "10.0.0.2", null, null, null),
				StubAwsServer.instance("i-2", "10.0.0.3", null, null, null))));
			Host[] expected = new Host[] {new Host("10.0.0.2", 3000), new Host("10.0.0.3", 3000)};
			await(() -> Arrays.equals(expected, snapshot.get()));

			Host[] lastGood = snapshot.get();
			stub.ec2Default(new Reply(403, StubAwsServer.error("UnauthorizedOperation",
				"You are not authorized to perform this operation.", "ea966190-f9aa-478e-9ede-example")));
			await(() -> warnings().stream().anyMatch(w -> w.contains("UnauthorizedOperation")));
			String warning = warnings().stream().filter(w -> w.contains("UnauthorizedOperation")).findFirst().get();

			assertSame(lastGood, snapshot.get());
			assertTrue(warning.startsWith("Seed candidate refresh failed: "), warning);
			assertTrue(warning.contains("ea966190-f9aa-478e-9ede-example"), warning);
			assertEquals(0, r.getOverrunCount());
		}
		finally {
			r.close();
		}
		await(() -> ! r.getThread().isAlive());
	}

	@Test
	void clientConstructionSeedsFromProvider() throws Exception {
		stub.ec2Default(new Reply(200, StubAwsServer.describe(null,
			StubAwsServer.instance("i-1", "127.0.0.1", null, null, null))));
		ClusterDefinition def = offline(builder -> builder.discovery(ops -> ops
			.provider(StaticDiscoveryConfig.AWS_TAGS)
			.aws(aws -> aws
				.region("us-east-1")
				.filters(FILTER)
				.credentials("AKIDEXAMPLE", "secret", null)
				.endpoint(stub.url())
				.port(1))));

		try (Cluster cluster = def.connect()) {
			assertArrayEquals(new Host[] {new Host("127.0.0.1", 1)}, seeds(cluster));
			assertTrue(seedRefresher(cluster).getThread().isAlive());
		}
	}

	@Test
	void unansweredStubBoundedByProviderTimeout() throws Exception {
		try (ServerSocket silent = StubAwsServer.silent()) {
			SystemSettings settings = settings(ops -> ops
				.refreshInterval(Duration.ofMillis(100))
				.refreshTimeout(Duration.ofMillis(200))
				.provider(StaticDiscoveryConfig.AWS_TAGS)
				.aws(aws -> aws
					.region("us-east-1")
					.filters(FILTER)
					.credentials("AKIDEXAMPLE", "secret", null)
					.endpoint("http://127.0.0.1:" + silent.getLocalPort())
					.timeout(Duration.ofMillis(50))
					.initTimeout(Duration.ofMillis(50))));
			SeedRefresher r = new SeedRefresher(provider(settings), DEF, settings.getDiscovery(), INIT, snapshot::set);
			r.validate(100);

			try {
				long begin = System.nanoTime();
				Host[] seeds = r.discover();
				long elapsed = (System.nanoTime() - begin) / 1000000;
				assertTrue(elapsed <= 50 + MARGIN, elapsed + "ms");
				assertArrayEquals(INIT, seeds);

				snapshot.set(seeds);
				r.start(seeds);
				await(() -> warnings().stream().filter(w ->
					w.startsWith("Seed candidate refresh failed: ") && w.contains("timed out after 50ms")).count() >= 2);
				assertSame(seeds, snapshot.get());
				assertEquals(0, r.getOverrunCount());
			}
			finally {
				r.close();
			}
		}
	}

	/**
	 * Messages of the WARN and ERROR lines logged so far.
	 */
	private List<String> warnings() {
		return stderr.toString(StandardCharsets.UTF_8).lines()
			.filter(line -> line.contains("] WARN ") || line.contains("] ERROR "))
			.map(line -> line.substring(line.indexOf(" - ") + 3))
			.toList();
	}

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

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not met in time");
			Thread.sleep(5);
		}
	}
}
