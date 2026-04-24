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
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.OpType;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.CommitLevel;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partition;
import com.aerospike.client.sdk.tend.Partitions;

public class WriteCommand extends Command {
    final Key key;
    final Partition partition;
    final OpType type;
    final CommitLevel commitLevel;
    final int gen;
    final int ttl;
    final boolean durableDelete;
    final boolean failOnFilteredOut;

    public WriteCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, OpType type, int gen, int ttl,
        Expression where, boolean failOnFilteredOut, ResolvedSettings settings
    ) {
        this(cluster, partitions, txn, key, type, gen, ttl, where, failOnFilteredOut, settings, null, null);
    }

    public WriteCommand(Cluster cluster, Partitions partitions, Key key, ResolvedSettings settings) {
        this(cluster, partitions, null, key, OpType.UPSERT, 0, 0, null, false, settings, null, null);
    }

    public WriteCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, OpType type, int gen, int ttl,
        Expression where, boolean failOnFilteredOut, ResolvedSettings settings,
        Boolean durableDeleteDefault, Boolean durableDeleteOverride
    ) {
        super(cluster, key.namespace, txn, where, settings.getReplicaOrder(), settings);
        this.key = key;
        this.partition = new Partition(partitions, key, replica, null, false);
        this.type = type;
        this.commitLevel = settings.getCommitLevel();
        this.gen = gen;
        this.ttl = ttl;
        this.durableDelete = settings.getUseDurableDelete(durableDeleteDefault, durableDeleteOverride);
        this.failOnFilteredOut = failOnFilteredOut;
    }
}
