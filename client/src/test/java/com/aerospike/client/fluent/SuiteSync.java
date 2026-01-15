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
package com.aerospike.client.fluent;

import java.io.File;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class SuiteSync {
	public static Cluster cluster = null;

	@BeforeAll
	public static void init() {
		Log.setCallback(null);

		System.out.println("Begin AerospikeClient");
		Args args = Args.Instance;

		Host[] hosts = Host.parseHosts(args.host, args.port);

        ClusterDefinition def = new ClusterDefinition(hosts)
        	.withLogLevel(Log.Level.DEBUG)
        	.clusterName(args.clusterName);

        if (args.tlsName != null) {
            String certHome = System.getenv("CERT_HOME");

            if (certHome == null) {
                certHome = "";
            }

            String caFile = resolvePath(certHome, args.caFile);
            String clientCertFile = resolvePath(certHome, args.clientCertFile);
            String clientKeyFile = resolvePath(certHome, args.clientKeyFile);

            def.withTlsConfigOf()
            	.tlsName(args.tlsName)
	            .caFile(caFile)
	            .clientCertFile(clientCertFile)
	            .clientKeyFile(clientKeyFile)
	        	.done();
        }

        cluster = def.connect();

		try {
			args.setServerSpecific(cluster);
		}
		catch (RuntimeException re) {
			cluster.close();
			throw re;
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

    @AfterAll
	public static void destroy() {
		System.out.println("End AerospikeClient");
		if (cluster != null) {
			cluster.close();
		}
	}
}
