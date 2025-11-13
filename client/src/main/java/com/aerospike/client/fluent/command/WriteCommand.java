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

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.OpType;
import com.aerospike.client.fluent.Partition;
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.CommitLevel;
import com.aerospike.client.fluent.policy.Settings;

public class WriteCommand extends Command {
	final Key key;
	final Partition partition;
	final OpType type;
	final CommitLevel commitLevel;
	final int gen;
	final int ttl;
	final boolean onLockingOnly;
	final boolean durableDelete;
	final boolean failOnFilteredOut;

	public WriteCommand(
		Cluster cluster, Partitions partitions, Txn txn, Key key, OpType type, int gen, int ttl,
		Expression filterExp, boolean failOnFilteredOut, Settings policy
	) {
		super(cluster, key.namespace, txn, filterExp, policy.getReplicaOrder(), policy);
		this.key = key;
		this.partition = new Partition(partitions, key, replica, null, false);
		this.type = type;
		this.commitLevel = CommitLevel.COMMIT_ALL;
		this.gen = gen;
		this.ttl = ttl;
		this.onLockingOnly = false;
		this.durableDelete = policy.getUseDurableDelete();
		this.failOnFilteredOut = failOnFilteredOut;
	}

	public WriteCommand(Cluster cluster, Key key, Settings policy) {
		super(cluster, key.namespace, null, null, policy.getReplicaOrder(), policy);
		this.key = key;
		this.partition = null;
		this.type = OpType.UPSERT;
		this.commitLevel = CommitLevel.COMMIT_ALL;
		this.gen = 0;
		this.ttl = 0;
		this.onLockingOnly = false;
		this.durableDelete = policy.getUseDurableDelete();
		this.failOnFilteredOut = false;
	}
}
