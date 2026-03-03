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
package com.aerospike.client.fluent.command;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.OpType;
import com.aerospike.client.fluent.OperationSpec;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.tend.Partitions;

public class UdfCommand extends WriteCommand {
	final String packageName;
	final String functionName;
	final Value[] args;

	public UdfCommand(
		Cluster cluster, Partitions partitions, Txn txn, Key key, OperationSpec spec,
		int ttl, Expression where, boolean failOnFilteredOut, Settings settings
	) {
		super(cluster, partitions, txn, key, OpType.UDF, 0, ttl, where, failOnFilteredOut, settings);
		this.packageName = spec.getUdfPackageName();
		this.functionName = spec.getUdfFunctionName();
		this.args = (spec.getUdfArguments() != null)? spec.getUdfArguments() : new Value[0];
	}
}
