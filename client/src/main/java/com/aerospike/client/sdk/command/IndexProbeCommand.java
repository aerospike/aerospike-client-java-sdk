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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.util.RandomShift;

/**
 * Server-side index selection probe ({@code INFO4_QUERY_SELECTION}).
 */
public final class IndexProbeCommand extends Command {
    final String set;
    final String indexNameHint;
    final long taskId;

    public IndexProbeCommand(
        Cluster cluster,
        String namespace,
        String set,
        Expression predicate,
        String indexNameHint,
        ResolvedSettings settings
    ) {
        this(cluster, namespace, set, predicate, indexNameHint, new RandomShift().nextLong(), settings);
    }

    public IndexProbeCommand(
        Cluster cluster,
        String namespace,
        String set,
        Expression predicate,
        String indexNameHint,
        long taskId,
        ResolvedSettings settings
    ) {
        super(cluster, namespace, null, predicate, settings.getReplicaOrder(), settings);
        if (namespace == null || namespace.isEmpty()) {
            throw new AerospikeException("Index probe requires namespace");
        }
        if (predicate == null) {
            throw new AerospikeException("Index probe requires predicate expression");
        }
        this.set = set;
        this.indexNameHint = indexNameHint;
        this.taskId = taskId;
    }

    /**
     * Run the probe against the cluster and return the server query plan.
     */
    public QueryPlan execute() {
        IndexProbeExecutor exec = new IndexProbeExecutor(cluster, this);
        exec.execute();
        return exec.getPlan();
    }
}
