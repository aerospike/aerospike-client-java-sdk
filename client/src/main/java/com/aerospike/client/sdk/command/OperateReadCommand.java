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
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partitions;

public class OperateReadCommand extends ReadCommand {
    final List<Operation> ops;
    final OperateArgs args;

    public OperateReadCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, List<Operation> ops, OperateArgs args,
        Expression filterExp, boolean failOnFilteredOut, ResolvedSettings settings, ReadAttr attr
    ) {
        super(cluster, partitions, txn, key, null, false, filterExp, failOnFilteredOut, settings, attr);
        this.ops = ops;
        this.args = args;
    }
}
