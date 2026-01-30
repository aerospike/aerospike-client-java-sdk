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

import java.util.List;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.tend.Partitions;

public class BatchReadCommand extends BatchCommand {
    final ReadModeAP readModeAP;
    final ReadModeSC readModeSC;
    final int readTouchTtlPercent;
    final boolean linearize;

	public BatchReadCommand(
		Cluster cluster, Partitions partitions, Txn txn, String namespace,
		List<BatchRecord> records, Expression filterExp, boolean respondAllKeys, Settings policy,
		ReadAttr attr
	) {
		super(cluster, partitions, txn, namespace, records, filterExp, attr.replica, respondAllKeys,
			policy);
		this.readModeAP = attr.readModeAP;
		this.readModeSC = attr.readModeSC;
        this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
        this.linearize = attr.linearize;
	}
}
