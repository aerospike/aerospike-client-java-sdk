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
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Replica;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.tend.Partition;
import com.aerospike.client.sdk.tend.Partitions;

public final class BatchCommand extends Command {
    final List<BatchRecord> records;
    final Partitions partitions;
    final Replica replicaSC;
    final boolean includeMissingKeys;
    final boolean failOnFilteredOut;
    final boolean inlineMemory;
    final boolean inlineSSD;
    final boolean linearize;

    public BatchCommand(
        Cluster cluster, Partitions partitions, Txn txn, String namespace,
        List<BatchRecord> records, Expression where, boolean includeMissingKeys,
        boolean failOnFilteredOut, boolean linearize, Settings settings
    ) {
        super(cluster, namespace, txn, where, settings.getReplicaOrder(), settings);
        this.records = records;
        this.partitions = partitions;
        this.replicaSC = Partition.getReplicaSC(settings.getReplicaOrder(), settings.getReadModeSC());
        this.includeMissingKeys = includeMissingKeys;
        this.failOnFilteredOut = failOnFilteredOut;
        this.linearize = linearize;
        this.inlineMemory = settings.getAllowInlineMemoryAccess();
        this.inlineSSD = settings.getAllowInlineSsdAccess();
    }

    public static boolean inDoubt(boolean isWrite, int commandSentCounter) {
        return isWrite && commandSentCounter > 1;
    }

    public Partitions getPartitions() {
        return partitions;
    }

    public List<BatchRecord> getRecords() {
        return records;
    }
}
