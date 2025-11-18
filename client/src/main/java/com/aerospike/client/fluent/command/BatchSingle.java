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

import java.io.IOException;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.AsyncRecordStream;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Connection;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.Partition;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;

public final class BatchSingle {
/*
	public static final class OperateRead extends Read {
		private final Operation[] ops;

		public OperateRead(
			Cluster cluster,
			BatchPolicy policy,
			Key key,
			Operation[] ops,
			Record[] records,
			int index,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, key, null, records, index, status, node, true);
			this.ops = ops;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, ops);
		}
	}

	public static class Read extends BaseCommand {
		protected final Key key;
		private final String[] binNames;
		private final Record[] records;
		private final int index;
		private final boolean isOperation;

		public Read(
			Cluster cluster,
			Policy policy,
			Key key,
			String[] binNames,
			Record[] records,
			int index,
			BatchStatus status,
			Node node,
			boolean isOperation
		) {
			super(cluster, policy, status, key, node, false);
			this.key = key;
			this.binNames = binNames;
			this.records = records;
			this.index = index;
			this.isOperation = isOperation;
		}

		@Override
		protected void writeBuffer() {
			setRead(policy, key, binNames);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);

			if (rp.resultCode == ResultCode.OK) {
				records[index] = rp.parseRecord(isOperation);
			}
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
		}
	}

	public static final class ReadHeader extends BaseCommand {
		private final Key key;
		private final Record[] records;
		private final int index;

		public ReadHeader(
			Cluster cluster,
			Policy policy,
			Key key,
			Record[] records,
			int index,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, key, node, false);
			this.key = key;
			this.records = records;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setReadHeader(policy, key);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);

			if (rp.resultCode == 0) {
				records[index] = new Record(null, rp.generation, rp.expiration);
			}
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
		}
	}
*/
	public static class ReadRecord extends BatchSingleExecutor {
		private final BatchReadCommand cmd;
		private final BatchRead record;

		public ReadRecord(
			Cluster cluster,
			BatchReadCommand cmd,
			BatchRead record,
			BatchStatus status,
			Node node
		) {
			super(cluster, cmd, status, record.key, node, false);
			this.cmd = cmd;
			this.record = record;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setRead(cmd, record);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, record.key, false);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(cmd.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(true));
			}
			else {
				record.setError(rp.resultCode, false);
				status.setRowError();
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			Partition p = new Partition(parent.partitions, key, parent.replica, null, cmd.linearize);
			p.sequence = sequence;
			p.prevNode = node;
			p.prepareRetryRead(timeout);
			node = p.getNodeRead(cluster);
			sequence = p.sequence;
			return true;
		}
	}
/*
	public static final class Exists extends BaseCommand {
		private final Key key;
		private final boolean[] existsArray;
		private final int index;

		public Exists(
			Cluster cluster,
			Policy policy,
			Key key,
			boolean[] existsArray,
			int index,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, key, node, false);
			this.key = key;
			this.existsArray = existsArray;
			this.index = index;
		}

		@Override
		protected void writeBuffer() {
			setExists(policy, key);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, false);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			existsArray[index] = rp.resultCode == 0;
		}
	}
*/
	public static final class OperateRecordAsync extends OperateRecordSync {
		private final AsyncRecordStream stream;
		private final int index;

		public OperateRecordAsync(
			Cluster cluster,
			BatchCommand parent,
			Operation[] ops,
			BatchAttr attr,
			BatchRecord br,
			BatchStatus status,
			Node node,
			AsyncRecordStream stream,
			int index
		) {
			super(cluster, parent, ops, attr, br, status, node);
			this.stream = stream;
			this.index = index;
		}

		@Override
		public void run() {
			super.run();

        	if (parent.respondAllKeys || super.record.record != null) {
        		stream.publish(new RecordResult(super.record, index));
        	}
		}
	}

	public static class OperateRecordSync extends BatchSingleExecutor {
		private final Operation[] ops;
		private final BatchAttr attr;
		private final BatchRecord record;

		public OperateRecordSync(
			Cluster cluster,
			BatchCommand parent,
			Operation[] ops,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, parent, status, record.key, node, attr.hasWrite);
			this.ops = ops;
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setOperate(parent, attr, record.key, ops);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, key, record.hasWrite);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(cmd.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(true));
			}
			else {
				record.setError(rp.resultCode, BatchCommand.inDoubt(attr.hasWrite, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			Partition p = new Partition(parent.partitions, key, parent.replica, null, false);
			p.sequence = sequence;
			p.prevNode = node;
			p.prepareRetryWrite(timeout);
			node = p.getNodeWrite(cluster);
			sequence = p.sequence;
			return true;
		}
	}
/*
	public static final class Delete extends BaseCommand {
		private final BatchAttr attr;
		private final BatchRecord record;

		public Delete(
			Cluster cluster,
			BatchPolicy policy,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, true);
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setDelete(policy, record.key, attr);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, true);

			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(new Record(null, rp.generation, rp.expiration));
			}
			else {
				// A KEY_NOT_FOUND_ERROR on a delete is benign, but still results in an overall
				// batch status of false to be consistent with the original batch code.
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}

	public static final class UDF extends BaseCommand {
		private final String packageName;
		private final String functionName;
		private final Value[] args;
		private final BatchAttr attr;
		private final BatchRecord record;

		public UDF(
			Cluster cluster,
			BatchPolicy policy,
			String packageName,
			String functionName,
			Value[] args,
			BatchAttr attr,
			BatchRecord record,
			BatchStatus status,
			Node node
		) {
			super(cluster, policy, status, record.key, node, true);
			this.packageName = packageName;
			this.functionName = functionName;
			this.args = args;
			this.attr = attr;
			this.record = record;
		}

		@Override
		protected void writeBuffer() {
			setUdf(policy, attr, record.key, packageName, functionName, args);
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			RecordParser rp = new RecordParser(conn, dataBuffer);
			rp.parseFields(policy.txn, key, true);
			if (node.areMetricsEnabled()) {
				node.addBytesIn(namespace, rp.bytesIn);
			}
			if (rp.resultCode == ResultCode.OK) {
				record.setRecord(rp.parseRecord(false));
			}
			else if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = rp.parseRecord(false);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					record.record = r;
					record.resultCode = rp.resultCode;
					record.inDoubt = Command.batchInDoubt(true, commandSentCounter);
					status.setRowError();
				}
			}
			else {
				record.setError(rp.resultCode, Command.batchInDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (record.resultCode == ResultCode.NO_RESPONSE) {
				record.inDoubt = true;
			}
		}
	}
*/
	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public static final class TxnVerify extends BatchSingleExecutor {
		private final BatchReadCommand read;
		private final long version;
		private final BatchRecord br;

		public TxnVerify(
			Cluster cluster,
			BatchReadCommand cmd,
			BatchStatus status,
			BatchRecord br,
			Node node,
			long version
		) {
			super(cluster, cmd, status, br.key, node, false);
			this.read = cmd;
			this.version = version;
			this.br = br;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setTxnVerify(br.key, version, cmd.serverTimeout);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(cmd.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				br.resultCode = rp.resultCode;
			}
			else {
				br.setError(rp.resultCode, false);
				status.setRowError();
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			Partition p = new Partition(read.partitions, key, read.replica, null, read.linearize);
			p.sequence = sequence;
			p.prevNode = node;
			p.prepareRetryRead(timeout);
			node = p.getNodeRead(cluster);
			sequence = p.sequence;
			return true;
		}
	}

	public static final class TxnRoll extends BatchSingleExecutor {
		private final Txn txn;
		private final BatchRecord br;
		private final int attr;

		public TxnRoll(
			Cluster cluster,
			BatchCommand cmd,
			Txn txn,
			BatchRecord br,
			BatchStatus status,
			Node node,
			int attr
		) {
			super(cluster, cmd, status, br.key, node, true);
			this.txn = txn;
			this.br = br;
			this.attr = attr;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setTxnRoll(br.key, txn, attr, cmd.serverTimeout);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(cmd.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				br.resultCode = rp.resultCode;
			}
			else {
				br.setError(rp.resultCode, BatchCommand.inDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (br.resultCode == ResultCode.NO_RESPONSE) {
				br.inDoubt = true;
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			Partition p = new Partition(parent.partitions, key, parent.replica, null, false);
			p.sequence = sequence;
			p.prevNode = node;
			p.prepareRetryWrite(timeout);
			node = p.getNodeWrite(cluster);
			sequence = p.sequence;
			return true;
		}
	}

	public static abstract class BatchSingleExecutor extends SyncExecutor implements IBatchCommand {
		final BatchCommand parent;
		BatchStatus status;
		Key key;
		Node node;
		int sequence;
		boolean hasWrite;
		boolean isSC;

		public BatchSingleExecutor(
			Cluster cluster,
			BatchCommand cmd,
			BatchStatus status,
			Key key,
			Node node,
			boolean hasWrite
		) {
			super(cluster, cmd);
			this.parent = cmd;
			this.status = status;
			this.key = key;
			this.node = node;
			this.hasWrite = hasWrite;
		}

		@Override
		public void run() {
			try {
				execute();
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

		@Override
		protected boolean isWrite() {
			return hasWrite;
		}

		@Override
		protected Node getNode() {
			return node;
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.BATCH;
		}

		@Override
		protected void addSubException(AerospikeException ae) {
			status.addSubException(ae);
		}

		public void setInDoubt() {
		}
	}
}
