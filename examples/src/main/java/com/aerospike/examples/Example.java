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
package com.aerospike.examples;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.util.Util;
import com.aerospike.client.sdk.util.Version;

/**
 * Abstract base class for all examples.
 *
 * <p>Concrete examples should extend this class and implement the {@link #runExample()} method.
 * The base class provides access to runner-managed configuration. Lifecycle logging and the
 * setup/verify/cleanup fixture flow are owned by {@link ExampleRunner}.
 */
public abstract class Example {
    /**
     * Behavior shared by examples and their fixtures.
     *
     * <p>A strong-consistency namespace rejects a non-durable delete with
     * {@code FAIL_FORBIDDEN}, so examples that delete records need durable deletes to run
     * against SC. The selector is scoped to {@code cp()}, which leaves AP namespaces on the
     * default behavior rather than making every example pay for tombstones.</p>
     */
    public static final Behavior EXAMPLE_BEHAVIOR = Behavior.DEFAULT.deriveWithChanges(
        "examples",
        builder -> builder.on(Selectors.writes().cp(), ops -> ops.useDurableDelete(true)));

    protected Console console;
    private ExampleContext context;

    void initialize(ExampleContext context) {
        this.context = context;
        this.console = context.console();
    }

    /**
     * Run one or more examples through the fixture and reporting-aware runner.
     *
     * @param console the console for output
     * @param args configuration parameters
     * @param examples list of example names to run
     * @throws Exception if an example fails
     */
    public static void runExamples(Console console, Args args, List<String> examples) throws Exception {
        ExampleRunResult result = new ExampleRunner(console, args).run(examples);

        if (result.exitCode() != 0) {
            throw new IllegalStateException("One or more examples failed");
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
     * Parse {@link Args} from a {@code main} {@code String[]} using the same options as
     * {@link Main} ({@link Args#addCommonOptions}): {@code -h}/{@code --host}, {@code -p}/{@code --port}
     * (defaults {@code localhost:3000}), {@code -a} services alternate, TLS flags, etc.
     *
     * @param argv arguments passed to {@code main}
     * @return parsed configuration
     * @throws ParseException if the command line is invalid
     */
    public static Args parseStandaloneArgs(String[] argv) throws ParseException {
        Options options = new Options();
        Args.addCommonOptions(options);
        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, argv);
        return new Args(cl);
    }

    /**
     * Build a {@link ClusterDefinition} with the same baseline settings as {@link #runExamples}
     * (connection pool sizing, optional TLS, optional services alternate from {@code -a}).
     * Callers may chain further options (credentials, racks, …) before {@link ClusterDefinition#connect()}.
     *
     * @param args parsed example arguments
     * @return definition (not yet connected)
     */
    public static ClusterDefinition clusterDefinition(Args args) {
        ClusterDefinition def = new ClusterDefinition(args.host, args.port)
                .clusterName(args.clusterName)
                .withSystemSettings(builder -> builder
                        .circuitBreaker(ops -> ops.maximumErrorsInErrorWindow(200))
                        .connections(conn -> conn
                                .minimumConnectionsPerNode(200)
                                .maximumConnectionsPerNode(200)
                        )
                );

        if (args.useServicesAlternate) {
            def = def.usingServicesAlternate();
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

        return def;
    }

    /**
     * Initialize this example with the runner-managed context and run its body.
     *
     * @param context runner-managed example context
     * @throws Exception if the example fails
     */
    public void run(ExampleContext context) throws Exception {
        initialize(context);
        runExample();
    }

    protected Cluster cluster() {
        return context.cluster();
    }

    protected String namespace() {
        return context.args().namespace;
    }

    protected String host() {
        return context.args().host;
    }

    protected int port() {
        return context.args().port;
    }

    protected boolean useServicesAlternate() {
        return context.args().useServicesAlternate;
    }

    protected DataSet dataSet() {
        return context.dataSet();
    }

    protected DataSet dataSet(String set) {
        return context.dataSet(set);
    }

    /**
     * Whether this example requires string AEL (server 8.1.3+). When true and the cluster
     * is older, {@link ExampleRunner} skips the example before {@link #runExample()}.
     */
    protected boolean requiresStringAel() {
        return false;
    }

    /** @return true when the connected cluster supports string AEL (8.1.3+). */
    protected static boolean supportsStringAel(Cluster cluster) {
        return cluster.getRandomNode().getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_3);
    }

    /**
     * Skip with {@link ExampleSkipException} unless the cluster supports string AEL (8.1.3+).
     */
    protected void requireStringAel() throws ExampleSkipException {
        if (!supportsStringAel(cluster())) {
            Version v = cluster().getRandomNode().getVersion();
            throw new ExampleSkipException("server is " + v + "; string AEL requires 8.1.3+");
        }
    }

    /**
     * Wait until the tend thread has populated the partition map for {@code namespace}.
     * Secondary cluster connections (e.g. YAML config demos) need this before writes.
     */
    protected static void ensurePartitionMapReady(Cluster cluster, String namespace) {
        if (cluster == null || namespace == null) {
            return;
        }

        for (int attempt = 0; attempt < 60; attempt++) {
            if (!cluster.getPartitionMap().isEmpty()
                    && cluster.getPartitionMap().containsKey(namespace)) {
                return;
            }
            Util.sleep(50);
        }

        throw new AerospikeException("Partition map not ready for namespace '" + namespace + "'");
    }

    /**
     * Run the example logic. Subclasses must implement this method.
     *
     * @throws Exception if the example fails
     */
    public abstract void runExample() throws Exception;
}

