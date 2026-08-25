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
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.junit.ServerFeature;
import com.aerospike.client.sdk.junit.ServerFeatureSupport;
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
        if (session == null) {
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

        ensurePartitionMapReady();
    }

    /**
     * Brief wait when the tend thread has not yet populated the partition map.
     * Called from {@link #initCluster()} (before subclass {@code @BeforeAll}) and
     * {@link #waitForPartitionMap()} (before each test).
     */
    protected static void ensurePartitionMapReady() {
        if (cluster == null || args.namespace == null) {
            return;
        }

        for (int attempt = 0; attempt < 60; attempt++) {
            if (!cluster.getPartitionMap().isEmpty()
                    && cluster.getPartitionMap().containsKey(args.namespace)) {
                return;
            }
            Util.sleep(50);
        }

        throw new AerospikeException("Partition map not ready for namespace '" + args.namespace + "'");
    }

    /** Re-check before each test in case tend refreshed the map mid-class. */
    @BeforeEach
    public void waitForPartitionMap() {
        ensurePartitionMapReady();
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

    /**
     * For {@code @EnabledIf} on test classes that require AEL (8.1.3+).
     *
     * <p>Prefer {@link com.aerospike.client.sdk.junit.RequiresServerFeature} on the test
     * class or nested class instead: {@code @EnabledIf} is evaluated before {@code @BeforeAll},
     * so with {@code cluster} still null it disables the entire class rather than skipping it.</p>
     */
    public static boolean supportsAel() {
        return cluster != null && cluster.supportsAel();
    }

    /** For {@code @EnabledIf} on test classes that require string operations (8.1.3+). */
    public static boolean supportsStringOperations() {
        return cluster != null && cluster.supportsStringOperations();
    }

    /** Skip when the cluster minimum version is below {@link Version#SERVER_VERSION_8_1_3}. */
    protected static void assumeSupportsAel() {
        ServerFeatureSupport.assume(ServerFeature.AEL);
    }

    /** Skip when the cluster does not support string read/write operations (8.1.3+). */
    protected static void assumeSupportsStringOps() {
        ServerFeatureSupport.assume(ServerFeature.STRING_OPS);
    }

    /** Skip when extended error-detail verbosity is unavailable (8.1.3+). */
    protected static void assumeExtendedErrorDetail() {
        ServerFeatureSupport.assume(ServerFeature.EXTENDED_ERROR_DETAIL);
    }

    /** Delete test keys before seeding so a prior run cannot leave unexpected bins. */
    protected void deleteTestKeys(Key... keys) {
        if (keys.length == 0) {
            return;
        }
        ChainableNoBinsBuilder d = session.delete(Arrays.asList(keys));
        if (args.scMode) {
            d = d.withDurableDelete();
        }
        d.execute();
    }

    /** Return a key with no prior record state in the namespace set. */
    protected Key freshKey(String id) {
        Key key = args.set.id(id);
        deleteTestKeys(key);
        return key;
    }

    /** Count records consumed from a query stream (does not close the stream). */
    protected static int countResults(RecordStream rs) {
        int count = 0;
        while (rs.hasNext()) {
            rs.next();
            count++;
        }
        return count;
    }
}
