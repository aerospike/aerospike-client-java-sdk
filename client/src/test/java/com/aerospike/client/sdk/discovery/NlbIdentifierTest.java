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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.policy.BehaviorYamlConfig;

/**
 * U-11: no public discovery or translation identifier matches {@code /nlb/i}.
 */
public class NlbIdentifierTest {
	private static final Pattern NLB = Pattern.compile("nlb", Pattern.CASE_INSENSITIVE);

	@Test
	void patternMatchesAnyCase() {
		assertTrue(NLB.matcher("awsNlbIdle").find());
		assertTrue(NLB.matcher("NLB").find());
		assertTrue(NLB.matcher("idle_nlb_ms").find());
	}

	@Test
	void discoveryPackageHasNoNlbIdentifier() throws Exception {
		List<String> ids = new ArrayList<>();

		for (Class<?> cls : discoveryTypes()) {
			ids.add(cls.getSimpleName());
			addMembers(cls, ids);
		}

		assertTrue(ids.containsAll(List.of("AddressTranslator", "SeedCandidateProvider",
			"SourceList", "ALTERNATE", "translate", "PassThroughAddressTranslator", "INSTANCE")), ids.toString());
		assertNone(ids);
	}

	/**
	 * The legacy test checked ClientPolicy; the SDK's discovery and translation options are on
	 * ClusterDefinition, plus the discovery and translation settings checked above.
	 */
	@Test
	void clusterDefinitionHasNoNlbIdentifier() {
		List<String> ids = new ArrayList<>();
		addMembers(ClusterDefinition.class, ids);

		assertTrue(ids.containsAll(List.of("ipMap", "addressTranslator", "usingServicesAlternate", "getIpMap",
			"getAddressTranslator", "getTranslationOnMiss", "getSeedCandidateProvider", "isUseServicesAlternate")),
			ids.toString());
		assertNone(ids);
	}

	/**
	 * The legacy test read the YAML keys from its static config serializers. The SDK's YAML keys are
	 * the snake_case names of the BehaviorYamlConfig discovery and translation properties; the
	 * provider names are the StaticDiscoveryConfig and StaticTranslationConfig constants.
	 */
	@Test
	void yamlKeysHaveNoNlbIdentifier() throws Exception {
		List<String> ids = new ArrayList<>();

		for (Class<?> cls : new Class<?>[] {BehaviorYamlConfig.SystemDiscoveryConfig.class,
			BehaviorYamlConfig.DnsConfig.class, BehaviorYamlConfig.AwsConfig.class,
			BehaviorYamlConfig.SystemTranslationConfig.class}) {
			for (Field field : cls.getDeclaredFields()) {
				if (! field.isSynthetic() && ! Modifier.isStatic(field.getModifiers())) {
					ids.add(snakeCase(field.getName()));
				}
			}
		}

		for (Class<?> cls : new Class<?>[] {StaticDiscoveryConfig.class, StaticTranslationConfig.class}) {
			addMembers(cls, ids);

			for (Field field : cls.getDeclaredFields()) {
				if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
					ids.add((String)field.get(null));
				}
			}
		}

		assertTrue(ids.containsAll(List.of("merge_policy", "refresh_interval", "refresh_timeout",
			"tend_refresh_deadline", "names", "record_type", "port", "tls_name", "timeout", "servers", "region",
			"filters", "address_field", "on_miss", "static_map", "port_map", "use_services_alternate", "static", "dns",
			"aws-tags", "static-map", "port-map", "pass-through")), ids.toString());
		assertNone(ids);
	}

	private static String snakeCase(String name) {
		return name.replaceAll("([A-Z])", "_$1").toLowerCase();
	}

	private static List<Class<?>> discoveryTypes() throws Exception {
		Path dir = Paths.get(AddressTranslator.class.getResource("AddressTranslator.class").toURI()).getParent();
		String pkg = AddressTranslator.class.getPackageName();
		List<Class<?>> types = new ArrayList<>();

		try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.class")) {
			for (Path file : files) {
				String name = file.getFileName().toString();
				Class<?> cls = Class.forName(pkg + '.' + name.substring(0, name.length() - 6), false,
					AddressTranslator.class.getClassLoader());

				if (Modifier.isPublic(cls.getModifiers())) {
					types.add(cls);
				}
			}
		}
		return types;
	}

	private static void addMembers(Class<?> cls, List<String> ids) {
		for (Field field : cls.getDeclaredFields()) {
			if (Modifier.isPublic(field.getModifiers())) {
				ids.add(field.getName());
			}
		}

		for (Method method : cls.getDeclaredMethods()) {
			if (Modifier.isPublic(method.getModifiers()) && ! method.isSynthetic()) {
				ids.add(method.getName());
			}
		}
	}

	private static void assertNone(List<String> ids) {
		for (String id : ids) {
			assertFalse(NLB.matcher(id).find(), id);
		}
	}
}
