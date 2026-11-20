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
package com.aerospike.client.sdk.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.tend.ClusterTend;

public class PluginGuideExamplesTest {
	static final class HostListSeedProvider implements SeedCandidateProvider {
		private volatile List<Host> hosts;

		HostListSeedProvider(Map<String,Object> options) {
			List<Host> list = new ArrayList<>();

			for (Object host : (List<?>)options.get("hosts")) {
				list.addAll(Host.parseServiceHosts((String)host));
			}
			this.hosts = List.copyOf(list);
		}

		void setHosts(List<Host> hosts) {
			this.hosts = List.copyOf(hosts);
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			return hosts;
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return true;
		}

		@Override
		public boolean supportsDeadline() {
			return true;
		}
	}

	static final class NatTableTranslator implements AddressTranslator {
		private final Map<String,Host> table = new HashMap<>();

		NatTableTranslator(Map<String,Object> options) {
			Map<?,?> map = (Map<?,?>)options.get("table");

			for (Map.Entry<?,?> entry : map.entrySet()) {
				table.put((String)entry.getKey(), Host.parseServiceHosts((String)entry.getValue()).get(0));
			}
		}

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.ofNullable(table.get(advertised.host + ':' + advertised.port));
		}
	}

	/**
	 * The legacy test selected the registered provider and translator by name from YAML
	 * ({@code static.client.discovery.provider: host-list}, {@code static.client.translation.provider:
	 * nat-table}). The SDK's YAML has no provider keys and the SDK has no translator registration, so
	 * this is the code-configured equivalent: the provider factory is registered and selected by name
	 * with the same options in the system settings, and the translator built from the same table is
	 * set on the cluster definition.
	 */
	@Test
	void registeredProviderAndTranslatorSelectedInCode() throws Exception {
		Map<String,Object> table = Map.of("table",
			Map.of("192.0.2.1:3000", "lb.example.com:30001", "192.0.2.2:3000", "lb.example.com:30002"));

		ClusterDefinition def = new ClusterDefinition(new Host("192.0.2.10", 3000))
			.failIfNotConnected(false)
			.tendTimeout(100)
			.addressTranslator(new NatTableTranslator(table))
			.withSystemSettings(builder -> builder
				.discovery(ops -> ops
					.provider("host-list")
					.options(Map.of("hosts", List.of("192.0.2.1:3000", "192.0.2.2:3000")))
					.mergePolicy(SeedMergePolicy.MERGE)
					.addProviderFactory("host-list", HostListSeedProvider::new))
				.translation(ops -> ops.onMiss(TranslationMissMode.STRICT)));

		try (Cluster cluster = def.connect()) {
			ClusterDefinition merged = cluster.getClusterDefinition();

			assertTrue(merged.getSeedCandidateProvider() instanceof HostListSeedProvider);
			assertTrue(merged.getAddressTranslator() instanceof NatTableTranslator);
			assertEquals(SeedMergePolicy.MERGE, cluster.getSystemSettings().getDiscovery().getMergePolicy());
			assertEquals(TranslationMissMode.STRICT, merged.getTranslationOnMiss());

			SeedCandidateContext ctx = new SeedCandidateContext(null, new Host[0], 0);
			assertEquals(List.of(new Host("192.0.2.1", 3000), new Host("192.0.2.2", 3000)),
				merged.getSeedCandidateProvider().refreshSeedCandidates(ctx));
			assertEquals(List.of(new Host("192.0.2.10", 3000), new Host("192.0.2.1", 3000), new Host("192.0.2.2", 3000)),
				List.of(seeds(cluster)));

			ClusterTend tend = cluster.getClusterTend();
			assertEquals(new Host("lb.example.com", 30002), tend.translateAddress("BB9020011AC4202", null, "192.0.2.2", 3000));
			assertThrows(AerospikeException.TranslationMiss.class,
				() -> tend.translateAddress("BB9030011AC4202", null, "192.0.2.3", 3000));
		}
	}

	@Test
	void providerReturnsHostsSetByApplication() {
		HostListSeedProvider provider = new HostListSeedProvider(Map.of("hosts", List.of("192.0.2.1:3000")));
		SeedCandidateContext ctx = new SeedCandidateContext(null, new Host[0], 0);

		assertTrue(provider.needsPeriodicRefresh());
		assertTrue(provider.supportsDeadline());
		assertEquals(List.of(new Host("192.0.2.1", 3000)), provider.refreshSeedCandidates(ctx));

		provider.setHosts(List.of(new Host("192.0.2.3", 3000), new Host("192.0.2.4", 3000)));
		assertEquals(List.of(new Host("192.0.2.3", 3000), new Host("192.0.2.4", 3000)),
			provider.refreshSeedCandidates(new SeedCandidateContext(null, new Host[0], 30)));
	}

	@Test
	void translatorHitAndMiss() {
		NatTableTranslator translator = new NatTableTranslator(
			Map.of("table", Map.of("192.0.2.1:3000", "lb.example.com:30001")));

		Optional<Host> hit = translator.translate(
			new Endpoint("BB9010011AC4202", "node1.example.com", "192.0.2.1", 3000, Endpoint.SourceList.ALTERNATE));
		assertEquals("lb.example.com", hit.get().name);
		assertEquals(30001, hit.get().port);

		Optional<Host> miss = translator.translate(
			new Endpoint("BB9030011AC4202", null, "192.0.2.3", 3000, Endpoint.SourceList.STANDARD));
		assertFalse(miss.isPresent());
	}

	/**
	 * The cluster's current seeds. The SDK has no public getter for them.
	 */
	private static Host[] seeds(Cluster cluster) throws Exception {
		Field field = ClusterTend.class.getDeclaredField("seeds");
		field.setAccessible(true);
		return (Host[])field.get(cluster.getClusterTend());
	}
}
