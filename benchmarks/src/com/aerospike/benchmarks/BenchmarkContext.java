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
package com.aerospike.benchmarks;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.aerospike.client.sdk.*;
import com.aerospike.client.sdk.policy.*;

import static com.aerospike.benchmarks.Arguments.toArgs;
import static com.aerospike.benchmarks.Constants.MS;
import static com.aerospike.benchmarks.Constants.US;

public final class BenchmarkContext implements AutoCloseable {

    private static final Map<Constants.OP_TYPE, LatencyManager> LATENCY_MANAGER_MAP = new HashMap<>();

    private final Cluster cluster;
    private final Session session;
    private final Behavior behavior;
    private final DataSet dataSet;
    private final Arguments arguments;


    private static Function<String, String> latencyUsageFunc = (latency) ->
         "Latency usage: ycsb[,<warmup count>] | [alt,]<columns>,<range shift increment>[,us|ms] "
                + " Received: "
                + latency;

    private BenchmarkContext(
        Cluster cluster,
        Session session,
        Behavior behavior,
        DataSet dataSet,
        boolean hasTxn, // TODO need to be applied
        Arguments arguments
    ) {
        this.cluster = cluster;
        this.session = session;
        this.behavior = behavior;
        this.dataSet = dataSet;
        this.arguments = arguments;
    }

    public static BenchmarkContext buildContext(ConnectionOptions connOpts,
                                                WorkloadOptions workloadOpts,
                                                BenchmarkOptions benchmarkOpts) throws Exception {
        BenchmarkContext ctx;
        ClusterDefinition def = buildClusterDefinition(connOpts);
        boolean hasTxn = isTXNWorkload(workloadOpts);
        Behavior behavior = toBehaviorFromOpts(workloadOpts);
        Arguments argument = toArgs(workloadOpts, benchmarkOpts);
        if (benchmarkOpts.getLatency() != null) {
            populateLatenciesPools(benchmarkOpts.getLatency());
        }
        Cluster cluster = def.connect();
        Session session = cluster.createSession(behavior);

        DataSet dataSet = DataSet.of(
                argument.getNamespace(),
                argument.getSetName()
        );

        ctx = new BenchmarkContext(cluster, session, behavior, dataSet, hasTxn, argument);

        return ctx;
    }

    private static void populateLatenciesPools(String latencyArg) throws Exception {
        for (Constants.OP_TYPE type : Constants.OP_TYPE.values()) {
            LATENCY_MANAGER_MAP.putIfAbsent(
                    type,
                    latencyManagerFactory(latencyArg, type.opType)
            );
        }
    }

    private static LatencyManager latencyManagerFactory(String latencyString, String opType) throws Exception {
        String[] latencyOpts = latencyString.split(",");
        if (latencyOpts.length < 1) {
            throw new Exception(latencyUsageFunc.apply(latencyString));
        }
        int columns = 0;
        int bitShift = 0;
        String units = MS;

        if ("ycsb".equalsIgnoreCase(latencyOpts[0])) {
            if (latencyOpts.length > 2) {
                throw new Exception(latencyUsageFunc.apply(latencyString));
            }
            int warmupCount = 0;
            if (latencyOpts.length == 2) {
                warmupCount = Integer.parseInt(latencyOpts[1]);
            }
            return new LatencyManagerYcsb(opType, warmupCount);
        }

        // -latency alt,7,1,units[optional]
        if ("alt".equalsIgnoreCase(latencyOpts[0])) {
            if (latencyOpts.length > 4) {
                throw new Exception(latencyUsageFunc.apply(latencyString));
            }
            columns = Integer.parseInt(latencyOpts[1]);
            bitShift = Integer.parseInt(latencyOpts[2]);
            if (latencyOpts.length > 3) {
                units = US.equalsIgnoreCase(latencyOpts[3]) ? US : units;
            }
            return new LatencyManagerAlternate(columns, bitShift, units);
        }

        // -latency 7,1,units[optional]
        if (latencyOpts.length > 3) {
            throw new Exception(latencyUsageFunc.apply(latencyString));
        }
        columns = Integer.parseInt(latencyOpts[0]);
        bitShift = Integer.parseInt(latencyOpts[1]);
        if (latencyOpts.length > 2) {
            units = US.equalsIgnoreCase(latencyOpts[2]) ? US : units;
        }
        return new LatencyManagerAerospike(columns, bitShift, units);
    }


    private static Behavior toBehaviorFromOpts(WorkloadOptions workloadOpts) {
        return Behavior.DEFAULT.deriveWithChanges("benchmark", builder -> {
            builder.on(Behavior.Selectors.all(), ops -> ops.abandonCallAfter(Duration.ofSeconds(1)));
            builder.on(Behavior.Selectors.all(), ops -> {
                ops.abandonCallAfter(Duration.ofSeconds(1));
            });
            applyRetryPolicy(builder, workloadOpts);
            applyReplica(builder, workloadOpts);
            applyReadMode(builder, workloadOpts);
            applyCommitLevel(builder, workloadOpts);
        });
    }


    private static void applyRetryPolicy(Behavior.BehaviorBuilder builder, WorkloadOptions workloadOpts) {
        Integer maxRetries = workloadOpts.getMaxRetries();
        Integer sleepMs = workloadOpts.getSleepBetweenRetries();
        if (maxRetries == null && sleepMs == null) {
            return;
        }
        builder.on(Behavior.Selectors.all(), ops -> {
            if (maxRetries != null) {
                ops.maximumNumberOfCallAttempts(maxRetries + 1);
            }
            if (sleepMs != null) {
                ops.delayBetweenRetries(Duration.ofMillis(sleepMs));
            }
        });
    }

    private static void applyReplica(Behavior.BehaviorBuilder builder, WorkloadOptions workloadOpts) {
        String replica = workloadOpts.getReplica();
        if (replica == null || replica.isEmpty()) {
            return;
        }
        Replica r = asReplica(replica);
        if (r == null) {
            return;
        }
        builder.on(Behavior.Selectors.reads(), ops -> ops.replicaOrder(r));
        builder.on(Behavior.Selectors.writes(), ops -> ops.replicaOrder(r));
    }

    private static void applyReadMode(Behavior.BehaviorBuilder builder, WorkloadOptions workloadOpts) {
        String readModeSc = workloadOpts.getReadModeSc();
        if (readModeSc != null) {
            ReadModeSC sc = toReadModeSc(readModeSc);
            if (sc != null) {
                builder.on(Behavior.Selectors.reads().cp(), ops -> ops.consistency(sc));
            }
        } else {
            String readModeAp = workloadOpts.getReadModeAp();
            if ("all".equalsIgnoreCase(readModeAp)) {
                builder.on(Behavior.Selectors.reads().ap(), ops -> ops.readMode(ReadModeAP.ALL));
            }
        }
    }

    private static void applyCommitLevel(Behavior.BehaviorBuilder builder, WorkloadOptions workloadOpts) {
        String commitLevel = workloadOpts.getCommitLevel();
        if (commitLevel == null || commitLevel.isEmpty()) {
            return;
        }
        CommitLevel level = "master".equalsIgnoreCase(commitLevel.trim())
                ? CommitLevel.COMMIT_MASTER
                : CommitLevel.COMMIT_ALL;
        builder.on(Behavior.Selectors.writes().ap(), ops -> ops.commitLevel(level));
    }

    /*
    private static Behavior applyBenchmarkOptions(Behavior behavior, BenchmarkOptions benchmarkOpts) {
        if (benchmarkOpts == null) {
            return behavior;
        }
        if (benchmarkOpts.isProleDistribution()) {
            return behavior.deriveWithChanges("prole", b ->
                    b.on(Behavior.Selectors.reads(), ops -> ops.replicaOrder(Replica.MASTER_PROLES)));
        }
        return behavior;
    }
    */

    private static ReadModeSC toReadModeSc(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case "session" -> ReadModeSC.SESSION;
            case "linearize" -> ReadModeSC.LINEARIZE;
            case "allow_replica" -> ReadModeSC.ALLOW_REPLICA;
            case "allow_unavailable" -> ReadModeSC.ALLOW_UNAVAILABLE;
            default -> null;
        };
    }

    private static Replica asReplica(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.toLowerCase()) {
            case "master" -> Replica.MASTER;
            case "any" -> Replica.MASTER_PROLES;
            case "sequence" -> Replica.SEQUENCE;
            case "preferrack" -> Replica.PREFER_RACK;
            case "random" -> Replica.RANDOM;
            default -> null;
        };
    }

    private static boolean isTXNWorkload(WorkloadOptions workloadOpts) {
        String w = workloadOpts.getWorkload();
        if (w == null || w.isEmpty()) {
            return false;
        }
        String upper = w.trim().toUpperCase();
        return upper.startsWith("TXN,") || "TXN".equals(upper);
    }

    /**
     * Expected cluster name for logs and validation: {@code -c} / {@code --clusterName}, else
     * {@code AEROSPIKE_CLUSTER_NAME} env (benchmark-only), so the client sees a name before connect.
     */
    private static String resolveClusterNameForBenchmark(ConnectionOptions connOpts) {
        String fromCli = connOpts.getClusterName();
        if (fromCli != null && !fromCli.isBlank()) {
            return fromCli.trim();
        }
        String fromEnv = System.getenv("AEROSPIKE_CLUSTER_NAME");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
        }
        return null;
    }

    private static ClusterDefinition buildClusterDefinition(ConnectionOptions connOpts) {
        Host[] hosts = Host.parseHosts(connOpts.getHosts(), connOpts.getPort());
        ClusterDefinition def = hosts.length == 1
                ? new ClusterDefinition(hosts[0].name, hosts[0].port)
                : new ClusterDefinition(hosts);

        if (connOpts.getUser() != null && !connOpts.getUser().isEmpty()) {
            String pw = connOpts.getPassword() != null ? new String(connOpts.getPassword()) : "";
            def.withNativeCredentials(connOpts.getUser(), pw);
        }
        String clusterName = resolveClusterNameForBenchmark(connOpts);
        if (clusterName != null) {
            def.clusterName(clusterName);
        }
        if (connOpts.isServicesAlternate()) {
           def.usingServicesAlternate();
        }
        if (connOpts.getLoginTimeout() != null) {
            def.loginTimeout(connOpts.getLoginTimeout());
        }
        if (connOpts.getTendTimeout() != null) {
           def.tendTimeout(connOpts.getTendTimeout());
        }
        if (connOpts.getConnPoolsPerNode() != null) {
           def.connPoolsPerNode(connOpts.getConnPoolsPerNode());
        }
        boolean needsSystemSettings = connOpts.getTendInterval() != null
                || connOpts.getMinConnectionsPerNode() != null
                || connOpts.getMaxConnectionsPerNode() != null;
        if (needsSystemSettings) {
            def.withSystemSettings(builder -> {
                        if (connOpts.getTendInterval() != null) {
                            builder.refresh(ops -> ops.tendInterval(Duration.ofMillis(connOpts.getTendInterval())));
                        }
                        if (connOpts.getMinConnectionsPerNode() != null || connOpts.getMaxConnectionsPerNode() != null) {
                            int min = Optional.ofNullable(connOpts.getMinConnectionsPerNode()).orElse(0);
                            int max = Optional.ofNullable(connOpts.getMaxConnectionsPerNode()).orElse(100);
                            builder.connections(ops -> ops.minimumConnectionsPerNode(min).maximumConnectionsPerNode(max));
                        }
                    });
        }
        def.failIfNotConnected(true);
       // def.withLogLevel(Log.Level.DEBUG);
        return def;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    public Behavior getBehavior() {
        return behavior;
    }

    public DataSet getDataSet() {
        return dataSet;
    }

    public Arguments getArguments() {
        return arguments;
    }

    public LatencyManager getLatencyManager(Constants.OP_TYPE opType) {
        return LATENCY_MANAGER_MAP.getOrDefault(opType, null);
    }

    public boolean isLatenciesEnabled() {
        return !LATENCY_MANAGER_MAP.isEmpty();
    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
        }
    }
}
