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
import java.util.concurrent.ExecutorService;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Replica;

public final class Batch {
	//-------------------------------------------------------
	// ReadList
	//-------------------------------------------------------
/*
	public static final class ReadListCommand extends BatchCommand {
		private final List<BatchRead> records;

		public ReadListCommand(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			List<BatchRead> records,
			BatchStatus status
		) {
			super(cluster, batch, policy, status, true);
			this.records = records;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				setBatchOperate(batchPolicy, null, null, null, records, batch, null);
			}
			else {
				setBatchRead(batchPolicy, records, batch);
			}
		}

		@Override
		protected boolean parseRow() {
			BatchRead record = records.get(batchIndex);

			parseFieldsRead(record.key);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
			}
			else {
				record.setError(resultCode, false);
				status.setRowError();
			}
			return true;
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new ReadListCommand(cluster, batchNode, batchPolicy, records, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, records, sequenceAP, sequenceSC, batch, status);
		}
	}

	//-------------------------------------------------------
	// GetArray
	//-------------------------------------------------------

	public static final class GetArrayCommand extends BatchCommand {
		private final Key[] keys;
		private final String[] binNames;
		private final Operation[] ops;
		private final Record[] records;
		private final int readAttr;

		public GetArrayCommand(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			String[] binNames,
			Operation[] ops,
			Record[] records,
			int readAttr,
			boolean isOperation,
			BatchStatus status
		) {
			super(cluster, batch, policy, status, isOperation);
			this.keys = keys;
			this.binNames = binNames;
			this.ops = ops;
			this.records = records;
			this.readAttr = readAttr;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				BatchAttr attr = new BatchAttr(policy, readAttr, ops);
				setBatchOperate(batchPolicy, keys, batch, binNames, ops, attr);
			}
			else {
				setBatchRead(batchPolicy, keys, batch, binNames, ops, readAttr);
			}
		}

		@Override
		protected boolean parseRow() {
			parseFieldsRead(keys[batchIndex]);

			if (resultCode == 0) {
				records[batchIndex] = parseRecord();
			}
			return true;
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new GetArrayCommand(cluster, batchNode, batchPolicy, keys, binNames, ops, records, readAttr, isOperation, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	//-------------------------------------------------------
	// ExistsArray
	//-------------------------------------------------------

	public static final class ExistsArrayCommand extends BatchCommand {
		private final Key[] keys;
		private final boolean[] existsArray;

		public ExistsArrayCommand(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy policy,
			Key[] keys,
			boolean[] existsArray,
			BatchStatus status
		) {
			super(cluster, batch, policy, status, false);
			this.keys = keys;
			this.existsArray = existsArray;
		}

		@Override
		protected void writeBuffer() {
			if (batch.node.hasBatchAny()) {
				BatchAttr attr = new BatchAttr(policy, Command.INFO1_READ | Command.INFO1_NOBINDATA);
				setBatchOperate(batchPolicy, keys, batch, null, null, attr);
			}
			else {
				setBatchRead(batchPolicy, keys, batch, null, null, Command.INFO1_READ | Command.INFO1_NOBINDATA);
			}
		}

		@Override
		protected boolean parseRow() {
			parseFieldsRead(keys[batchIndex]);
			existsArray[batchIndex] = resultCode == 0;
			return true;
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new ExistsArrayCommand(cluster, batchNode, batchPolicy, keys, existsArray, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, keys, sequenceAP, sequenceSC, batch, false, status);
		}
	}
*/
	//-------------------------------------------------------
	// OperateList
	//-------------------------------------------------------

	public static final class OperateList extends NodeExecutor {
		private final List<BatchRecord> records;

		public OperateList(
			Cluster cluster,
			BatchCommand parent,
			BatchNode batch,
			List<BatchRecord> records,
			BatchStatus status
		) {
			super(cluster, parent, batch, status, true);
			this.records = records;
		}

		@Override
		protected boolean isWrite() {
			// This method is only called to set inDoubt on node level errors.
			// setError() will filter out reads when setting record level inDoubt.
			return true;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setBatchOperate(parent, batch, null, null, null);
			return cb;
		}

		@Override
		protected boolean parseRow() {
			BatchRecord record = records.get(batchIndex);

			parseFields(record);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
				return true;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord();
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = BatchCommand.inDoubt(record.hasWrite, commandSentCounter);
					status.setRowError();
					return true;
				}
			}

			record.setError(resultCode, BatchCommand.inDoubt(record.hasWrite, commandSentCounter));
			status.setRowError();
			return true;
		}

		@Override
		protected void inDoubt() {
			for (int index : batch.offsets) {
				BatchRecord record = records.get(index);

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = record.hasWrite;

					if (record.inDoubt && parent.txn != null) {
						parent.txn.onWriteInDoubt(record.key);
					}
				}
			}
		}

		@Override
		protected NodeExecutor createCommand(BatchNode batchNode) {
			return new OperateList(cluster, parent, batchNode, records, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, parent, records, sequenceAP, sequenceSC, batch, status);
		}
	}
/*
	//-------------------------------------------------------
	// OperateArray
	//-------------------------------------------------------

	public static final class OperateArrayCommand extends BatchCommand {
		private final Key[] keys;
		private final Operation[] ops;
		private final BatchRecord[] records;
		private final BatchAttr attr;

		public OperateArrayCommand(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Operation[] ops,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) {
			super(cluster, batch, batchPolicy, status, ops != null);
 			this.keys = keys;
			this.ops = ops;
			this.records = records;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchOperate(batchPolicy, keys, batch, null, ops, attr);
		}

		@Override
		protected boolean parseRow() {
			BatchRecord record = records[batchIndex];

			parseFields(record);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
				status.setRowError();
			}
			return true;
		}

		@Override
		protected void inDoubt() {
			if (!attr.hasWrite) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = true;

					if (policy.txn != null) {
						policy.txn.onWriteInDoubt(record.key);
					}
				}
			}
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new OperateArrayCommand(cluster, batchNode, batchPolicy, keys, ops, records, attr, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// UDF
	//-------------------------------------------------------

	public static final class UDFCommand extends BatchCommand {
		private final Key[] keys;
		private final String packageName;
		private final String functionName;
		private final byte[] argBytes;
		private final BatchRecord[] records;
		private final BatchAttr attr;

		public UDFCommand(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			String packageName,
			String functionName,
			byte[] argBytes,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) {
			super(cluster, batch, batchPolicy, status, false);
 			this.keys = keys;
 			this.packageName = packageName;
 			this.functionName = functionName;
 			this.argBytes = argBytes;
			this.records = records;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchUDF(batchPolicy, keys, batch, packageName, functionName, argBytes, attr);
		}

		@Override
		protected boolean parseRow() {
			BatchRecord record = records[batchIndex];

			parseFields(record);

			if (resultCode == 0) {
				record.setRecord(parseRecord());
				return true;
			}

			if (resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = parseRecord();
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = resultCode;
					record.inDoubt = Command.batchInDoubt(attr.hasWrite, commandSentCounter);
					status.setRowError();
					return true;
				}
			}

			record.setError(resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
			status.setRowError();
			return true;
		}

		@Override
		protected void inDoubt() {
			if (!attr.hasWrite) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = true;

					if (policy.txn != null) {
						policy.txn.onWriteInDoubt(record.key);
					}
				}
			}
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new UDFCommand(cluster, batchNode, batchPolicy, keys, packageName, functionName, argBytes, records, attr, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public static final class TxnVerify extends BatchCommand {
		private final Key[] keys;
		private final Long[] versions;
		private final BatchRecord[] records;

		public TxnVerify(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Key[] keys,
			Long[] versions,
			BatchRecord[] records,
			BatchStatus status
		) {
			super(cluster, batch, batchPolicy, status, false);
			this.keys = keys;
			this.versions = versions;
			this.records = records;
		}

		@Override
		protected boolean isWrite() {
			return false;
		}

		@Override
		protected void writeBuffer() {
			setBatchTxnVerify(batchPolicy, keys, versions, batch);
		}

		@Override
		protected boolean parseRow() {
			skipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0) {
				record.resultCode = resultCode;
			}
			else {
				record.setError(resultCode, false);
				status.setRowError();
			}
			return true;
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new TxnVerify(cluster, batchNode, batchPolicy, keys, versions, records, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, false, status);
		}
	}

	public static final class TxnRoll extends BatchCommand {
		private final Txn txn;
		private final Key[] keys;
		private final BatchRecord[] records;
		private final BatchAttr attr;

		public TxnRoll(
			Cluster cluster,
			BatchNode batch,
			BatchPolicy batchPolicy,
			Txn txn,
			Key[] keys,
			BatchRecord[] records,
			BatchAttr attr,
			BatchStatus status
		) {
			super(cluster, batch, batchPolicy, status, false);
			this.txn = txn;
			this.keys = keys;
			this.records = records;
			this.attr = attr;
		}

		@Override
		protected boolean isWrite() {
			return attr.hasWrite;
		}

		@Override
		protected void writeBuffer() {
			setBatchTxnRoll(batchPolicy, txn, keys, batch, attr);
		}

		@Override
		protected boolean parseRow() {
			skipKey(fieldCount);

			BatchRecord record = records[batchIndex];

			if (resultCode == 0) {
				record.resultCode = resultCode;
			}
			else {
				record.setError(resultCode, Command.batchInDoubt(attr.hasWrite, commandSentCounter));
				status.setRowError();
			}
			return true;
		}

		@Override
		protected void inDoubt() {
			if (!attr.hasWrite) {
				return;
			}

			for (int index : batch.offsets) {
				BatchRecord record = records[index];

				if (record.resultCode == ResultCode.NO_RESPONSE) {
					record.inDoubt = true;
				}
			}
		}

		@Override
		protected BatchCommand createCommand(BatchNode batchNode) {
			return new TxnRoll(cluster, batchNode, batchPolicy, txn, keys, records, attr, status);
		}

		@Override
		protected List<BatchNode> generateBatchNodes() {
			return BatchNodeList.generate(cluster, batchPolicy, keys, records, sequenceAP, sequenceSC, batch, attr.hasWrite, status);
		}
	}
*/
	//-------------------------------------------------------
	// Batch Base Command
	//-------------------------------------------------------

	public static abstract class NodeExecutor extends MultiExecutor implements IBatchCommand {
		final BatchCommand parent;
		final BatchNode batch;
		final BatchStatus status;
		int sequenceAP;
		int sequenceSC;
		boolean splitRetry;

		public NodeExecutor(
			Cluster cluster,
			BatchCommand parent,
			BatchNode batch,
			BatchStatus status,
			boolean isOperation
		) {
			super(cluster, parent, batch.node, isOperation);
			this.parent = parent;
			this.batch = batch;
			this.status = status;
		}

		@Override
		public void run() {
			try {
				if (!splitRetry) {
					execute();
				}
				else {
					executeCommand();
				}
			}
			catch (AerospikeException ae) {
				if (ae.getInDoubt()) {
					setInDoubt();
				}
				status.setException(ae);
			}
			catch (Throwable e) {
				setInDoubt();
				status.setException(new AerospikeException(e));
			}
		}

		protected final void parseFieldsRead(Key key) {
			if (parent.txn != null) {
				Long version = parseVersion(fieldCount);
				parent.txn.onRead(key, version);
			}
			else {
				skipKey(fieldCount);
			}
		}

		protected final void parseFields(BatchRecord br) {
			if (parent.txn != null) {
				Long version = parseVersion(fieldCount);

				if (br.hasWrite) {
					parent.txn.onWrite(br.key, version, resultCode);
				}
				else {
					parent.txn.onRead(br.key, version);
				}
			}
			else {
				skipKey(fieldCount);
			}
		}

		@Override
		protected void addSubException(AerospikeException ae) {
			status.addSubException(ae);
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.BATCH;
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			if (! (parent.replica == Replica.SEQUENCE || parent.replica == Replica.PREFER_RACK)) {
				// Perform regular retry to same node.
				return true;
			}
			sequenceAP++;

			if (! timeout || parent.readModeSC != ReadModeSC.LINEARIZE) {
				sequenceSC++;
			}
			return false;
		}

		@Override
		protected boolean retryBatch(
			Cluster cluster,
			int socketTimeout,
			int totalTimeout,
			long deadline,
			int iteration,
			int commandSentCounter
		) {
			// Retry requires keys for this node to be split among other nodes.
			// This is both recursive and exponential.
			List<BatchNode> batchNodes = generateBatchNodes();

			if (batchNodes.size() == 1 && batchNodes.get(0).node == batch.node) {
				// Batch node is the same.  Go through normal retry.
				return false;
			}

			splitRetry = true;

			// Run batch retries in parallel using virtual threads.
			try (ExecutorService es = cluster.getExecutorService();) {
				for (BatchNode batchNode : batchNodes) {
					NodeExecutor exec = createCommand(batchNode);
					exec.sequenceAP = sequenceAP;
					exec.sequenceSC = sequenceSC;
					exec.socketTimeout = socketTimeout;
					exec.totalTimeout = totalTimeout;
					exec.iteration = iteration;
					exec.commandSentCounter = commandSentCounter;
					exec.deadline = deadline;

					cluster.addRetry();
					es.execute(exec);
				}
			}
			return true;
		}

		@Override
		public void setInDoubt() {
			// Set error/inDoubt for keys associated this batch command when
			// the command was not retried and split. If a split retry occurred,
			// those new subcommands have already set inDoubt on the affected
			// subset of keys.
			if (! splitRetry) {
				inDoubt();
			}
		}

		protected void inDoubt() {
			// Do nothing by default. Batch writes will override this method.
		}

		abstract NodeExecutor createCommand(BatchNode batchNode);
		abstract List<BatchNode> generateBatchNodes();
	}
}
