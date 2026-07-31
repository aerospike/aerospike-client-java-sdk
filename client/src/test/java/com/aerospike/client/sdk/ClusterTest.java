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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.util.Util;
import com.aerospike.client.sdk.util.Version;

public class ClusterTest {
    public static Args args = Args.Instance;
    public static Cluster cluster;
    public static Session session;
    public static Session sessionWithSendKey;
    static boolean initializedBySuite = false;

    @BeforeAll
    public static void initCluster() {
        if (session != null) {
            return; // Already initialized by suite
        }

        Host[] hosts = Host.parseHosts(args.host, args.port);

        ClusterDefinition def = new ClusterDefinition(hosts)
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
            sessionWithSendKey = cluster.createSession(Behavior.DEFAULT.deriveWithChanges(
                    "sendKey",
                    opt -> opt.on(Selectors.all(), s -> s.sendKey(true)))
            );
            args.setServerSpecific(cluster);
        }
        catch (RuntimeException re) {
            cluster.close();
            throw re;
        }
    }

    /**
     * Brief wait when the tend thread has not yet populated the partition map.
     * Avoids flaky "Partition map empty" errors in long shared-cluster suites.
     */
    @BeforeEach
    public static void waitForPartitionMap() {
        if (cluster == null || args.namespace == null) {
            return;
        }

        for (int attempt = 0; attempt < 40; attempt++) {
            if (!cluster.getPartitionMap().isEmpty()
                    && cluster.getPartitionMap().containsKey(args.namespace)) {
                return;
            }
            Util.sleep(50);
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
        sessionWithSendKey = null;

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

    /** For {@code @EnabledIf} on test classes that require AEL (8.1.3+). */
    public static boolean supportsAel() {
        return cluster != null && cluster.supportsAel();
    }

    /** Skip when the cluster minimum version is below {@link Version#SERVER_VERSION_8_1_3}. */
    protected static void assumeSupportsAel() {
        Assumptions.assumeTrue(supportsAel(),
            "server does not support AEL (requires " + Version.SERVER_VERSION_8_1_3 + "+)");
    }
}
