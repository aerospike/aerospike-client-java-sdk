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
import com.aerospike.client.sdk.OperationSpec;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partitions;

public class UdfCommand extends WriteCommand {
    final String packageName;
    final String functionName;
    final Value[] args;

    public UdfCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, OperationSpec spec,
        int ttl, Expression where, boolean failOnFilteredOut, ResolvedSettings settings,
        Boolean durableDeleteDefault, Boolean durableDeleteOverride
    ) {
        super(cluster, partitions, txn, key, OpType.UDF, 0, ttl, where, failOnFilteredOut, settings,
            durableDeleteDefault, durableDeleteOverride);
        this.packageName = spec.getUdfPackageName();
        this.functionName = spec.getUdfFunctionName();
        this.args = (spec.getUdfArguments() != null)? spec.getUdfArguments() : new Value[0];
    }
}
