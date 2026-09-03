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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.ResultCode;

public class ExampleRunner {
    private final Console console;
    private final Args args;

    public ExampleRunner(Console console, Args args) {
        this.console = console;
        this.args = args;
    }

    public ExampleRunResult run(List<String> exampleNames) throws Exception {
        List<ExampleDefinition> definitions = resolveExamples(exampleNames);
        List<ExampleResult> results = new ArrayList<>();

        try (Cluster cluster = createCluster()) {
            Example.ensurePartitionMapReady(cluster, args.namespace);
            ExampleContext context = new ExampleContext(cluster, args, console);

            for (ExampleDefinition definition : definitions) {
                ExampleResult result = runOne(definition, context);
                results.add(result);

                if (args.failFast && result.failed()) {
                    break;
                }
            }
        }

        ExampleRunResult runResult = new ExampleRunResult(results);
        logSummary(runResult);
        writeReport(runResult);
        return runResult;
    }

    private List<ExampleDefinition> resolveExamples(List<String> exampleNames) {
        List<ExampleDefinition> definitions = new ArrayList<>();

        for (String name : exampleNames) {
            ExampleDefinition definition = ExampleRegistry.find(name)
                .orElseThrow(() -> new IllegalArgumentException("Unknown example: " + name));

            if (matchesTags(definition)) {
                definitions.add(definition);
            }
            else {
                console.info("Skipping %s because it does not match the tag filters", definition.name());
            }
        }

        return definitions;
    }

    private boolean matchesTags(ExampleDefinition definition) {
        if (!args.includeTags.isEmpty() && args.includeTags.stream().noneMatch(definition::hasTag)) {
            return false;
        }

        return args.excludeTags.isEmpty() || args.excludeTags.stream().noneMatch(definition::hasTag);
    }

    private ExampleResult runOne(ExampleDefinition definition, ExampleContext context) {
        long start = System.nanoTime();
        ExampleResult result;
        console.info(definition.name() + " Begin");

        try {
            definition.fixture().setup(context);
            Example example = instantiate(definition);
            if (example.requiresStringAel() && !Example.supportsStringAel(context.cluster())) {
                throw new ExampleSkipException(
                    "server is " + context.cluster().getRandomNode().getVersion()
                        + "; string AEL requires 8.1.3+");
            }
            example.run(context);
            definition.fixture().verify(context);
            console.info(definition.name() + " Passed");
            result = ExampleResult.passed(definition.name(), elapsedMillis(start));
        }
        catch (ExampleSkipException e) {
            console.warn("%s Skipped: %s", definition.name(), e.getMessage());
            result = ExampleResult.skipped(definition.name(), elapsedMillis(start), e.getMessage());
        }
        catch (Throwable t) {
            String enterpriseOnly = enterpriseOnlyReason(t);

            if (enterpriseOnly != null) {
                console.warn("%s Skipped: %s", definition.name(), enterpriseOnly);
                result = ExampleResult.skipped(definition.name(), elapsedMillis(start), enterpriseOnly);
            }
            else {
                console.error("%s Failed: %s", definition.name(), t.getMessage());
                result = ExampleResult.failed(definition.name(), elapsedMillis(start), t);
            }
        }
        finally {
            console.info(definition.name() + " End");
        }

        try {
            definition.fixture().cleanup(context);
        }
        catch (Throwable t) {
            console.error("%s Cleanup failed: %s", definition.name(), t.getMessage());

            // Only a passing example is downgraded by a cleanup failure. A skip or an
            // existing failure keeps its original, more meaningful result.
            if (result.status() == ExampleStatus.PASSED) {
                result = ExampleResult.failed(definition.name(), elapsedMillis(start), t);
            }
        }

        return result;
    }

    /**
     * A Community server rejects Enterprise-only features with {@link ResultCode#ENTERPRISE_ONLY}.
     * That is a server capability gate rather than an example defect, so it is reported as a skip,
     * the same as the strong-consistency and string-AEL gates. The exception is often wrapped, so
     * the whole cause chain is searched.
     *
     * @return the skip reason, or null when this failure is not an edition gate
     */
    private static String enterpriseOnlyReason(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof AerospikeException ae
                    && ae.getResultCode() == ResultCode.ENTERPRISE_ONLY) {
                return "server is Community Edition; this example needs an Enterprise feature: "
                    + ae.getBaseMessage();
            }

            if (cause.getCause() == cause) {
                break;
            }
        }

        return null;
    }

    private Example instantiate(ExampleDefinition definition) throws Exception {
        return definition.exampleClass().getDeclaredConstructor().newInstance();
    }

    private Cluster createCluster() {
        ClusterDefinition def = new ClusterDefinition(args.host, args.port)
            .clusterName(args.clusterName)
            .withSystemSettings(builder -> builder
                .circuitBreaker(ops -> ops.maximumErrorsInErrorWindow(200))
                .connections(conn -> conn
                    .minimumConnectionsPerNode(200)
                    .maximumConnectionsPerNode(200)
                )
            );

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

        return def.connect();
    }

    private static String resolvePath(String dir, String path) {
        File file = new File(path);

        if (file.isAbsolute()) {
            return path;
        }

        file = new File(dir, path);
        return file.getAbsolutePath();
    }

    private void logSummary(ExampleRunResult runResult) {
        console.info(
            "Examples complete: %d passed, %d failed, %d skipped",
            runResult.passedCount(),
            runResult.failedCount(),
            runResult.skippedCount());
    }

    private void writeReport(ExampleRunResult runResult) throws Exception {
        if (args.reportPath == null || args.reportPath.isBlank()) {
            return;
        }

        Path reportPath = Path.of(args.reportPath);
        JUnitXmlReportWriter.write(reportPath, runResult);
        console.info("Wrote example report to " + reportPath);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
