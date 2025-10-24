/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Settings;

public class BatchCommand extends ReadCommandBase {
	final Partitions partitions;
	final List<BatchRecord> records;
	final boolean respondAllKeys;
    final boolean inlineMemory;
    final boolean inlineSSD;

	public BatchCommand(
		Cluster cluster, Partitions partitions, Txn txn, String namespace,
		List<BatchRecord> records, Expression filterExp, boolean respondAllKeys, Settings policy
	) {
		super(cluster, namespace, partitions, txn, filterExp, policy);
		this.partitions = partitions;
		this.records = records;
		this.respondAllKeys = respondAllKeys;
		this.inlineMemory = policy.getAllowInlineMemoryAccess();
		this.inlineSSD = policy.getAllowInlineSsdAccess();
	}

	public static boolean inDoubt(boolean isWrite, int commandSentCounter) {
		return isWrite && commandSentCounter > 1;
	}
}
