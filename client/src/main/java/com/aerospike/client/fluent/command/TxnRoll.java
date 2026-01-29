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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.tend.Partitions;

public final class TxnRoll {
	private final Cluster cluster;
	private final Partitions partitions;
	private final Txn txn;
	private List<BatchRecord> verifyRecords;
	private List<BatchRecord> rollRecords;

	public TxnRoll(Cluster cluster, Txn txn) {
		this.cluster = cluster;
		this.txn = txn;

		HashMap<String,Partitions> partitionMap = cluster.getPartitionMap();

		this.partitions = partitionMap.get(txn.getNamespace());

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(txn.getNamespace(), partitionMap.size());
		}
	}

	public void verify(Settings verifyPolicy, Settings rollPolicy) {
		BatchReadCommand parent = null;

		try {
			// Verify read versions in a batch.
			Set<Map.Entry<Key, Long>> reads = txn.getReads();
			int max = reads.size();

			if (max == 0) {
				return;
			}

	        List<BatchRecord> batchRecords = new ArrayList<BatchRecord>(max);
			Long[] versions = new Long[max];
			int count = 0;

	        for (Map.Entry<Key, Long> entry : reads) {
				Key key = entry.getKey();
				BatchRecord br = new BatchRead(key, null, false);

				batchRecords.add(br);
				versions[count++] = entry.getValue();
	        }

			this.verifyRecords = batchRecords;

			ReadAttr attr = new ReadAttr(partitions, verifyPolicy);

			parent = new BatchReadCommand(cluster, partitions, txn, txn.getNamespace(),
				batchRecords, null, false, verifyPolicy, attr);

			BatchStatus status = new BatchStatus(true);

	        List<BatchNode> bns = BatchNodeList.generate(cluster, partitions,
	        	verifyPolicy.getReplicaOrder(), batchRecords, status);

	        IBatchCommand[] commands = new IBatchCommand[bns.size()];

			count = 0;

			for (BatchNode bn : bns) {
				if (bn.offsetsSize == 1) {
					int i = bn.offsets[0];
					commands[count++] = new BatchSingle.TxnVerify(cluster, parent, status,
						batchRecords.get(i), bn.node, versions[i]);
				}
				else {
					commands[count++] = new Batch.TxnVerify(cluster, parent, bn, status, versions);
				}
			}

			BatchExecutor.execute(cluster, commands, status);

			if (!status.getStatus()) {
				throw new RuntimeException("Failed to verify one or more record versions");
			}
		}
		catch (Throwable t) {
			// Verify failed. Abort.
			txn.setState(Txn.State.ABORTED);

			try {
				roll(rollPolicy, Command.INFO4_TXN_ROLL_BACK);
			}
			catch (Throwable t2) {
				// Throw combination of verify and roll exceptions.
				t.addSuppressed(t2);
				throw createCommitException(parent, CommitError.VERIFY_FAIL_ABORT_ABANDONED, t);
			}

			if (txn.closeMonitor()) {
				try {
					Key txnKey = TxnMonitor.getTxnMonitorKey(txn);
					WriteCommand cmd = new WriteCommand(cluster, partitions, txnKey, rollPolicy);
					close(cmd);
				}
				catch (Throwable t3) {
					// Throw combination of verify and close exceptions.
					t.addSuppressed(t3);
					throw createCommitException(parent, CommitError.VERIFY_FAIL_CLOSE_ABANDONED, t);
				}
			}

			// Throw original exception when abort succeeds.
			throw createCommitException(parent, CommitError.VERIFY_FAIL, t);
		}

		txn.setState(Txn.State.VERIFIED);
	}

	public CommitStatus commit(Settings rollPolicy) {
		Key txnKey = TxnMonitor.getTxnMonitorKey(txn);
		WriteCommand cmd = new WriteCommand(cluster, partitions, txnKey, rollPolicy);

		if (txn.monitorExists()) {
			// Tell transaction monitor that a roll-forward will commence.
			try {
				markRollForward(cmd);
			}
			catch (AerospikeException ae) {
				AerospikeException.Commit aec = createCommitException(cmd,
					CommitError.MARK_ROLL_FORWARD_ABANDONED, ae);

				if (ae.getResultCode() == ResultCode.MRT_ABORTED) {
					aec.setInDoubt(false);
					txn.setInDoubt(false);
					txn.setState(Txn.State.ABORTED);
				}
				else if (txn.getInDoubt()) {
					// The transaction was already inDoubt and just failed again,
					// so the new exception should also be inDoubt.
					aec.setInDoubt(true);
				}
				else if (ae.getInDoubt()){
					// The current exception is inDoubt.
					aec.setInDoubt(true);
					txn.setInDoubt(true);
				}
				throw aec;
			}
			catch (Throwable t) {
				AerospikeException.Commit aec = createCommitException(cmd,
					CommitError.MARK_ROLL_FORWARD_ABANDONED, t);

				if (txn.getInDoubt()) {
					aec.setInDoubt(true);
				}
				throw aec;
			}
		}

		txn.setState(Txn.State.COMMITTED);
		txn.setInDoubt(false);

		// Roll-forward writes in batch.
		try {
			roll(rollPolicy, Command.INFO4_TXN_ROLL_FORWARD);
		}
		catch (Throwable t) {
			return CommitStatus.ROLL_FORWARD_ABANDONED;
		}

		if (txn.closeMonitor()) {
			// Remove transaction monitor.
			try {
				close(cmd);
			}
			catch (Throwable t) {
				return CommitStatus.CLOSE_ABANDONED;
			}
		}
		return CommitStatus.OK;
	}

	private AerospikeException.Commit createCommitException(
		Command cmd, CommitError error, Throwable cause
	) {
		AerospikeException.Commit aec = new AerospikeException.Commit(error, verifyRecords, rollRecords, cause);

		if (cause instanceof AerospikeException) {
			AerospikeException src = (AerospikeException)cause;
			aec.setNode(src.getNode());
			aec.setIteration(src.getIteration());
			aec.setInDoubt(src.getInDoubt());

			if (cmd != null) {
				aec.setCommand(cmd);
			}
		}
		return aec;
	}

	public AbortStatus abort(Settings rollPolicy) {
		txn.setState(Txn.State.ABORTED);

		try {
			roll(rollPolicy, Command.INFO4_TXN_ROLL_BACK);
		}
		catch (Throwable t) {
			return AbortStatus.ROLL_BACK_ABANDONED;
		}

		if (txn.closeMonitor()) {
			try {
				Key txnKey = TxnMonitor.getTxnMonitorKey(txn);
				WriteCommand cmd = new WriteCommand(cluster, partitions, txnKey, rollPolicy);
				close(cmd);
			}
			catch (Throwable t) {
				return AbortStatus.CLOSE_ABANDONED;
			}
		}
		return AbortStatus.OK;
	}

	private void markRollForward(WriteCommand cmd) {
		// Tell transaction monitor that a roll-forward will commence.
		TxnMarkRollForward mark = new TxnMarkRollForward(cluster, cmd);
		mark.execute();
	}

	private void roll(Settings rollPolicy, int txnAttr) {
		Set<Key> keySet = txn.getWrites();
		int max = keySet.size();

		if (max == 0) {
			return;
		}

        List<BatchRecord> batchRecords = new ArrayList<BatchRecord>(max);

        for (Key key : keySet) {
			BatchRecord br = new BatchWrite(key, null);
			batchRecords.add(br);
        }

		this.rollRecords = batchRecords;

		BatchAttr attr = new BatchAttr();
		attr.setTxn(txnAttr);

        BatchCommand parent = new BatchCommand(cluster, partitions, txn, txn.getNamespace(),
            batchRecords, null, rollPolicy.getReplicaOrder(), false, rollPolicy);

        BatchStatus status = new BatchStatus(true);

		List<BatchNode> bns = BatchNodeList.generate(cluster, partitions,
				rollPolicy.getReplicaOrder(), batchRecords, status);

		IBatchCommand[] commands = new IBatchCommand[bns.size()];
		int count = 0;

		for (BatchNode bn : bns) {
			if (bn.offsetsSize == 1) {
				int i = bn.offsets[0];
				commands[count++] = new BatchSingle.TxnRoll(
					cluster, parent, txn, batchRecords.get(i), status, bn.node, txnAttr);
			}
			else {
				commands[count++] = new Batch.TxnRoll(
					cluster, parent, bn, status, batchRecords, attr);
			}
		}

		BatchExecutor.execute(cluster, commands, status);

		if (!status.getStatus()) {
			String rollString = txnAttr == Command.INFO4_TXN_ROLL_FORWARD? "commit" : "abort";
			throw new RuntimeException("Failed to " + rollString + " one or more records");
		}
	}

	private void close(WriteCommand cmd) {
		// Delete transaction monitor on server.
		TxnClose close = new TxnClose(cluster, cmd);
		close.execute();

		// Clear transaction fields on client.
		txn.clear();
	}
}
