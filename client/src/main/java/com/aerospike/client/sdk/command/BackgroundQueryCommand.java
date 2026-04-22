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

import java.util.List;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.OpType;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.CommitLevel;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.Filter;

public final class BackgroundQueryCommand extends Command {
    final long taskId;
    final String set;
    final Filter filter;
    final OpType type;
    final CommitLevel commitLevel;
    final List<Operation> ops;
    final String packageName;
    final String functionName;
    final Value[] functionArgs;
    final int recordsPerSecond;
    final int ttl;
    final boolean durableDelete;

    public BackgroundQueryCommand(
        Cluster cluster, DataSet set, long taskId, OpType type, List<Operation> ops, int ttl,
        Filter filter, Expression filterExp, ResolvedSettings settings, int recordsPerSecond,
        Boolean durableDeleteOverride
    ) {
        super(cluster, set.getNamespace(), null, filterExp, settings.getReplicaOrder(), settings);
        this.taskId = taskId;
        this.set = set.getSet();
        this.filter = filter;
        this.type = type;
        this.commitLevel = settings.getCommitLevel();
        this.ops = ops;
        this.packageName = null;
        this.functionName = null;
        this.functionArgs = null;
        this.recordsPerSecond = recordsPerSecond;
        this.ttl = ttl;
        this.durableDelete = settings.getUseDurableDelete(durableDeleteOverride);
    }

    public BackgroundQueryCommand(
        Cluster cluster, DataSet set, long taskId, OpType type,  int ttl,
        String packageName, String functionName, Value[] functionArgs,
        Filter filter, Expression filterExp, ResolvedSettings settings, int recordsPerSecond,
        Boolean durableDeleteOverride
    ) {
        super(cluster, set.getNamespace(), null, filterExp, settings.getReplicaOrder(), settings);
        this.taskId = taskId;
        this.set = set.getSet();
        this.filter = filter;
        this.type = type;
        this.commitLevel = settings.getCommitLevel();
        this.packageName = packageName;
        this.functionName = functionName;
        this.functionArgs = functionArgs;
        this.ops = null;
        this.recordsPerSecond = recordsPerSecond;
        this.ttl = ttl;
        this.durableDelete = settings.getUseDurableDelete(durableDeleteOverride);
    }
}
