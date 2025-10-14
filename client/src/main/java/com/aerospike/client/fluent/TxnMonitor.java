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
package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.cdt.ListOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.ListPolicy;
import com.aerospike.client.fluent.cdt.ListWriteFlags;
import com.aerospike.client.fluent.policy.BatchPolicy;
import com.aerospike.client.fluent.policy.Policy;
import com.aerospike.client.fluent.policy.WritePolicy;

public final class TxnMonitor {
	private static final ListPolicy OrderedListPolicy = new ListPolicy(ListOrder.ORDERED,
		ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);

	private static final String BinNameId = "id";
	private static final String BinNameDigests = "keyds";

	public static void addKey(Txn txn, Cluster cluster, WritePolicy policy, Key cmdKey) {
		txn.verifyCommand();

		if (txn.getWrites().contains(cmdKey)) {
			// Transaction monitor already contains this key.
			return;
		}

		Operation[] ops = getTranOps(txn, cmdKey);
		addWriteKeys(txn, cluster, policy, ops);
	}

	public static void addKeys(Txn txn, Cluster cluster, BatchPolicy policy, Key[] keys) {
		Operation[] ops = getTranOps(txn, keys);
		addWriteKeys(txn, cluster, policy, ops);
	}

	public static void addKeys(Txn txn, Cluster cluster, Policy policy, List<Key> keys) {
		Operation[] ops = getTranOps(txn, keys);
		addWriteKeys(txn, cluster, policy, ops);
	}

	public static void addKeysBatch(Txn txn, Cluster cluster, BatchPolicy policy, List<BatchRecord> records) {
		Operation[] ops = getTranOpsBatch(txn, records);

		if (ops != null) {
			addWriteKeys(txn, cluster, policy, ops);
		}
	}

	public static Operation[] getTranOps(Txn txn, Key cmdKey) {
		txn.setNamespace(cmdKey.namespace);

		if (txn.monitorExists()) {
			return new Operation[] {
				ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest))
			};
		}
		else {
			return new Operation[] {
				Operation.put(new Bin(BinNameId, txn.getId())),
				ListOperation.append(OrderedListPolicy, BinNameDigests, Value.get(cmdKey.digest))
			};
		}
	}

	public static Operation[] getTranOps(Txn txn, Key[] keys) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(keys.length);

		for (Key key : keys) {
			txn.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}
		return getTranOps(txn, list);
	}

	public static Operation[] getTranOps(Txn txn, List<Key> keys) {
		txn.verifyCommand();

		ArrayList<Value> list = new ArrayList<>(keys.size());

		for (Key key : keys) {
			txn.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}
		return getTranOps(txn, list);
	}

	public static Operation[] getTranOpsBatch(Txn txn, List<BatchRecord> records) {
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

	private static Operation[] getTranOps(Txn txn, ArrayList<Value> list) {
		if (txn.monitorExists()) {
			return new Operation[] {
				ListOperation.appendItems(OrderedListPolicy, BinNameDigests, list)
			};
		}
		else {
			return new Operation[] {
				Operation.put(new Bin(BinNameId, txn.getId())),
				ListOperation.appendItems(OrderedListPolicy, BinNameDigests, list)
			};
		}
	}

	private static void addWriteKeys(Txn txn, Cluster cluster, Policy policy, Operation[] ops) {
		/*
		Key txnKey = getTxnMonitorKey(txn);
		WritePolicy wp = copyTimeoutPolicy(policy);
		OperateArgs args = new OperateArgs(ops);
		TxnAddKeys cmd = new TxnAddKeys(cluster, txnKey, args, txn);
		cmd.execute();
		*/
	}

	public static Key getTxnMonitorKey(Txn txn) {
		return new Key(txn.getNamespace(), "<ERO~MRT", txn.getId());
	}

	public static WritePolicy copyTimeoutPolicy(Policy policy) {
		/*
		// Inherit some fields from the original command's policy.
		WritePolicy wp = new WritePolicy();
		wp.connectTimeout = policy.connectTimeout;
		wp.socketTimeout = policy.socketTimeout;
		wp.totalTimeout = policy.totalTimeout;
		wp.timeoutDelay = policy.timeoutDelay;
		wp.maxRetries = policy.maxRetries;
		wp.sleepBetweenRetries = policy.sleepBetweenRetries;
		wp.compress = policy.compress;
		wp.respondAllOps = true;

		// Note that the server only accepts the timeout on transaction monitor record create.
		// The server ignores the transaction timeout field on successive transaction monitor
		// record updates.
		wp.expiration = policy.txn.getTimeout();
		return wp;
		*/
		return null;
	}
}
