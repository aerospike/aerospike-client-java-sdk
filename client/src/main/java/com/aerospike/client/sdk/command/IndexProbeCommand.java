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
package com.aerospike.client.sdk.command;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.util.RandomShift;

/**
 * Server query explain (phase 1): field {@code 44} WHERE with {@code EXPLAIN} flag.
 */
public final class IndexProbeCommand extends Command {
    final String set;
    final String ael;
    final String indexNameHint;
    final long taskId;

    public IndexProbeCommand(
        Cluster cluster,
        String namespace,
        String set,
        String ael,
        String indexNameHint,
        ResolvedSettings settings
    ) {
        this(cluster, namespace, set, ael, indexNameHint, new RandomShift().nextLong(), settings);
    }

    public IndexProbeCommand(
        Cluster cluster,
        String namespace,
        String set,
        String ael,
        String indexNameHint,
        long taskId,
        ResolvedSettings settings
    ) {
        super(cluster, namespace, null, null, settings.getReplicaOrder(), settings);
        this.set = set;
        this.ael = ael;
        this.indexNameHint = indexNameHint;
        this.taskId = taskId;
    }

    /**
     * Run explain against the cluster and return the server query plan.
     */
    public QueryPlan execute() {
        IndexProbeExecutor exec = new IndexProbeExecutor(cluster, this);
        exec.execute();
        return exec.getPlan();
    }
}
