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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.fluent.Bin;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.OpType;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.cdt.ListOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.ListPolicy;
import com.aerospike.client.fluent.cdt.ListWriteFlags;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.tend.Partitions;

public final class TxnMonitor {
	private static final ListPolicy OrderedListPolicy = new ListPolicy(ListOrder.ORDERED,
		ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);

	private static final String BinNameId = "id";
	private static final String BinNameDigests = "keyds";

	public static void addKey(
		Txn txn, Cluster cluster, Partitions partitions, Settings policy, Key cmdKey
	) {
		txn.verifyCommand();

		if (txn.getWrites().contains(cmdKey)) {
			// Transaction monitor already contains this key.
			return;
		}

		List<Operation> ops = getTranOps(txn, cmdKey);
		addWriteKeys(txn, cluster, partitions, policy, ops);
	}

	public static void addKeys(
		Txn txn, Cluster cluster, Partitions partitions, Settings policy, Key[] keys
	) {
		List<Operation> ops = getTranOps(txn, keys);
		addWriteKeys(txn, cluster, partitions, policy, ops);
	}

	public static void addKeys(
		Txn txn, Cluster cluster, Partitions partitions, Settings policy, List<Key> keys
	) {
		List<Operation> ops = getTranOps(txn, keys);
		addWriteKeys(txn, cluster, partitions, policy, ops);
	}

	public static void addKeysBatchWrite(
		Txn txn, Cluster cluster, Partitions partitions, Settings policy, List<BatchRecord> records
	) {
		List<Operation> ops = getTranOpsBatchWrite(txn, records);
		addWriteKeys(txn, cluster, partitions, policy, ops);
	}

	public static void addKeysBatchReadWrite(Txn txn, Session session, List<BatchRecord> records) {
		List<Operation> ops = getTranOpsBatchReadWrite(txn, records);

		if (ops != null) {
			addWriteKeys(txn, session, ops);
		}
	}

	public static List<Operation> getTranOps(Txn txn, Key cmdKey) {
		txn.setNamespace(cmdKey.namespace);

		List<Operation> ops = new ArrayList<>(2);

		if (!txn.monitorExists()) {
			ops.add(Operation.put(new Bin(BinNameId, txn.getId())));
		}
		ops.add(ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest)));
		return ops;
	}

	public static List<Operation> getTranOps(Txn txn, Key[] keys) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(keys.length);

		for (Key key : keys) {
			txn.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}
		return getTranOps(txn, list);
	}

	public static List<Operation> getTranOps(Txn txn, List<Key> keys) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(keys.size());

		for (Key key : keys) {
			txn.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}
		return getTranOps(txn, list);
	}

	private static List<Operation> getTranOpsBatchWrite(Txn txn, List<BatchRecord> records) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(records.size());

		for (BatchRecord br : records) {
			txn.setNamespace(br.key.namespace);
			list.add(Value.get(br.key.digest));
		}
		return getTranOps(txn, list);
	}

	private static List<Operation> getTranOpsBatchReadWrite(Txn txn, List<BatchRecord> records) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(records.size());

		for (BatchRecord br : records) {
			txn.setNamespace(br.key.namespace);

			if (br.hasWrite) {
				list.add(Value.get(br.key.digest));
			}
		}

		if (list.size() == 0) {
			// Readonly batch does not need to add key digests.
			return null;
		}
		return getTranOps(txn, list);
	}

	private static List<Operation> getTranOps(Txn txn, ArrayList<Value> list) {
		List<Operation> ops = new ArrayList<>(2);

		if (!txn.monitorExists()) {
			ops.add(Operation.put(new Bin(BinNameId, txn.getId())));
		}
		ops.add(ListOperation.appendItems(OrderedListPolicy, BinNameDigests, list));
		return ops;
	}

	private static void addWriteKeys(
		Txn txn, Cluster cluster, Partitions partitions, Settings policy, List<Operation> ops
	) {
		Key txnKey = getTxnMonitorKey(txn);

		OperateArgs args = new OperateArgs(ops);
        OperateWriteCommand cmd = new OperateWriteCommand(cluster, partitions, txn, txnKey, ops,
        	args, OpType.UPSERT, 0, txn.getTimeout(), null, false, policy
			);

        SyncTxnAddKeysExecutor exec = new SyncTxnAddKeysExecutor(cluster, cmd);
    	exec.execute();
	}

	private static void addWriteKeys(Txn txn, Session session, List<Operation> ops) {
		Key txnKey = getTxnMonitorKey(txn);

		OperateArgs args = new OperateArgs(ops);
		Cluster cluster = session.getCluster();
        Partitions partitions = cluster.getPartitionMap().get(txn.getNamespace());

        Settings settings = session.getBehavior().getSettings(OpKind.WRITE_NON_RETRYABLE,
        		OpShape.POINT, partitions.scMode);

        OperateWriteCommand cmd = new OperateWriteCommand(cluster, partitions, txn, txnKey, ops,
        	args, OpType.UPSERT, 0, txn.getTimeout(), null, false, settings
			);

        SyncTxnAddKeysExecutor exec = new SyncTxnAddKeysExecutor(cluster, cmd);
    	exec.execute();
	}

	public static Key getTxnMonitorKey(Txn txn) {
		return new Key(txn.getNamespace(), "<ERO~MRT", txn.getId());
	}
}
