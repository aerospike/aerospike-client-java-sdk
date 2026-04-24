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
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.OpType;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partitions;

public class OperateWriteCommand extends WriteCommand {
    final List<Operation> ops;
    final OperateArgs args;

    public OperateWriteCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, List<Operation> ops,
        OperateArgs args, OpType type, int gen, int ttl, Expression filterExp,
        boolean failOnFilteredOut, ResolvedSettings settings
    ) {
        super(cluster, partitions, txn, key, type, gen, ttl, filterExp, failOnFilteredOut, settings,
            null, null);
        this.ops = ops;
        this.args = args;
    }

    public OperateWriteCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, List<Operation> ops,
        OperateArgs args, OpType type, int gen, int ttl, Expression filterExp,
        boolean failOnFilteredOut, ResolvedSettings settings, Boolean durableDeleteDefault,
        Boolean durableDeleteOverride
    ) {
        super(cluster, partitions, txn, key, type, gen, ttl, filterExp, failOnFilteredOut, settings,
            durableDeleteDefault, durableDeleteOverride);
        this.ops = ops;
        this.args = args;
    }
}
