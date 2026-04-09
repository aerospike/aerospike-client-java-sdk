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

import java.io.File;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.aerospike.client.sdk.policy.Behavior;

public class ClusterTest {
	public static Args args = Args.Instance;
	public static Cluster cluster;
	public static Session session;
	static boolean initializedBySuite = false;

	@BeforeAll
	public static void initCluster() {
		if (session != null) {
			return; // Already initialized by suite
		}

		Log.setCallback(null);

		Host[] hosts = Host.parseHosts(args.host, args.port);

		ClusterDefinition def = new ClusterDefinition(hosts)
			.withLogLevel(Log.Level.DEBUG)
			.clusterName(args.clusterName)
			.withSystemSettings(SystemSettings.builder()
					.connections(ops -> ops.maximumConnectionsPerNode(200)).build()
					.mergeWith(SystemSettings.DEFAULT));

		if (args.useServicesAlternate) {
			def.usingServicesAlternate();
		}

		// Handle authenticated requests if provided
		if (args.user != null && args.password != null) {
			switch (args.authMode) {
				case INTERNAL:
					def.withNativeCredentials(args.user, args.password);
					break;
				case EXTERNAL:
					def.withExternalCredentials(args.user, args.password);
					break;
				case EXTERNAL_INSECURE:
					def.withExternalInsecureCredentials(args.user, args.password);
					break;
				default:
					break;
			}
		}

		if (args.tlsName != null) {
			String certHome = System.getenv("CERT_HOME");
			if (certHome == null) {
				certHome = "";
			}

			String caFile = resolvePath(certHome, args.caFile);
			String clientCertFile = resolvePath(certHome, args.clientCertFile);
			String clientKeyFile = resolvePath(certHome, args.clientKeyFile);

			def.withTlsConfig(tls -> tls
				.tlsName(args.tlsName)
				.caFile(caFile)
				.clientCertFile(clientCertFile)
				.clientKeyFile(clientKeyFile)
			);
		}

		cluster = def.connect();

		try {
			session = cluster.createSession(Behavior.DEFAULT);
			args.setServerSpecific(cluster);
		}
		catch (RuntimeException re) {
			cluster.close();
			throw re;
		}
	}

	@AfterAll
	public static void shutdownCluster() {
		// Don't close cluster if it was initialized by suite
		// The suite's @AfterSuite will handle cleanup
		if (initializedBySuite) {
			return;
		}

		// Session doesn't need explicit cleanup - it's just a wrapper
		session = null;

		if (cluster != null) {
			cluster.close();
			cluster = null;
		}
	}

	private static String resolvePath(String dir, String path) {
		File file = new File(path);
		if (file.isAbsolute()) {
			return path;
		}
		file = new File(dir, path);
		return file.getAbsolutePath();
	}
}
