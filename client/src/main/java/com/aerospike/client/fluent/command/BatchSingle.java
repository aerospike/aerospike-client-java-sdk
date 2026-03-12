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

import java.io.IOException;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.AsyncRecordStream;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;
import com.aerospike.client.fluent.tend.Partition;

public final class BatchSingle {

	public static class ReadRecordAsync extends ReadRecordSync {
		private final AsyncRecordStream stream;
		private final int index;

		public ReadRecordAsync(
			Cluster cluster,
			BatchCommand cmd,
			BatchRead rec,
			BatchStatus status,
			Node node,
			AsyncRecordStream stream,
			int index
		) {
			super(cluster, cmd, rec, status, node);
			this.stream = stream;
			this.index = index;
		}

		@Override
		public void run() {
			super.run();

        	if (parent.includeMissingKeys || super.rec.record != null) {
        		stream.publish(new RecordResult(super.rec, index));
        	}
		}
	}

	public static class ReadRecordSync extends BatchSingleExecutor {
		private final BatchCommand cmd;
		private final BatchRead rec;

		public ReadRecordSync(
			Cluster cluster,
			BatchCommand cmd,
			BatchRead record,
			BatchStatus status,
			Node node
		) {
			super(cluster, cmd, status, record.key, node, false);
			this.cmd = cmd;
			this.rec = record;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setRead(cmd, rec);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, rec.key, false);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(rec.key.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				rec.setRecord(rp.parseRecord(true));
			}
			else {
				rec.setError(rp.resultCode, false);
				status.setRowError();
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			Partition p = new Partition(parent.partitions, key, parent.replica, null, rec.linearize);
			p.sequence = sequence;
			p.prevNode = node;
			p.prepareRetryRead(timeout);
			node = p.getNodeRead(cluster);
			sequence = p.sequence;
			return true;
		}
	}

	public static final class Exists extends BatchSingleExecutor {
		private final BatchCommand cmd;
		private final BatchRead rec;

		public Exists(
			Cluster cluster,
			BatchCommand cmd,
			BatchRead record,
			BatchStatus status,
			Node node
		) {
			super(cluster, cmd, status, record.key, node, false);
			this.cmd = cmd;
			this.rec = record;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setExists(cmd, rec);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, key, false);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(rec.key.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				rec.setRecord(rp.parseRecord(false));
			}
			else {
				rec.setError(rp.resultCode, false);
				status.setRowError();
			}
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			Partition p = new Partition(parent.partitions, key, parent.replica, null, rec.linearize);
			p.sequence = sequence;
			p.prevNode = node;
			p.prepareRetryRead(timeout);
			node = p.getNodeRead(cluster);
			sequence = p.sequence;
			return true;
		}
	}

	public static final class OperateRecordAsync extends OperateRecordSync {
		private final AsyncRecordStream stream;
		private final int index;

		public OperateRecordAsync(
			Cluster cluster,
			BatchCommand parent,
			BatchWrite rec,
			BatchStatus status,
			Node node,
			AsyncRecordStream stream,
			int index
		) {
			super(cluster, parent, rec, status, node);
			this.stream = stream;
			this.index = index;
		}

		@Override
		public void run() {
			super.run();
			stream.publish(new RecordResult(super.rec, index));
		}
	}

	public static class OperateRecordSync extends BatchSingleExecutor {
		protected final BatchWrite rec;

		public OperateRecordSync(
			Cluster cluster,
			BatchCommand parent,
			BatchWrite rec,
			BatchStatus status,
			Node node
		) {
			super(cluster, parent, status, rec.key, node, rec.hasWrite);
			this.rec = rec;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setOperate(parent, rec);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, key, rec.hasWrite);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(rec.key.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				rec.setRecord(rp.parseRecord(true));
			}
			else {
				rec.setError(rp.resultCode, BatchCommand.inDoubt(rec.hasWrite, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (rec.resultCode == ResultCode.NO_RESPONSE) {
				rec.inDoubt = true;
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

	public static final class Delete extends BatchSingleExecutor {
		private final BatchCommand cmd;
		private final BatchDelete rec;

		public Delete(
			Cluster cluster,
			BatchCommand cmd,
			BatchDelete record,
			BatchStatus status,
			Node node
		) {
			super(cluster, cmd, status, record.key, node, true);
			this.cmd = cmd;
			this.rec = record;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setDelete(cmd, rec);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, key, true);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(rec.key.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				rec.setRecord(new Record(null, rp.generation, rp.expiration));
			}
			else {
				// A KEY_NOT_FOUND_ERROR on a delete is benign, but still results in an overall
				// batch status of false to be consistent with the original batch code.
				rec.setError(rp.resultCode, BatchCommand.inDoubt(true, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (rec.resultCode == ResultCode.NO_RESPONSE) {
				rec.inDoubt = true;
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

	public static final class Udf extends BatchSingleExecutor {
		private final BatchCommand cmd;
		private final BatchUDF rec;

		public Udf(
			Cluster cluster,
			BatchCommand cmd,
			BatchUDF rec,
			BatchStatus status,
			Node node
		) {
			super(cluster, cmd, status, rec.key, node, true);
			this.cmd = cmd;
			this.rec = rec;
		}

		@Override
		protected CommandBuffer getCommandBuffer() {
			CommandBuffer cb = new CommandBuffer();
			cb.setUdf(cmd, rec);
			return cb;
		}

		@Override
		protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
			RecordParser rp = new RecordParser(conn, buffer);
			rp.parseFields(cmd.txn, key, true);

			if (node.isMetricsEnabled()) {
				node.addBytesIn(rec.key.namespace, rp.bytesIn);
			}

			if (rp.resultCode == ResultCode.OK) {
				rec.setRecord(rp.parseRecord(false));
			}
			else if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
				Record r = rp.parseRecord(false);
				String m = r.getString("FAILURE");

				if (m != null) {
					// Need to store record because failure bin contains an error message.
					rec.record = r;
					rec.resultCode = rp.resultCode;
					rec.inDoubt = BatchCommand.inDoubt(true, commandSentCounter);
					status.setRowError();
				}
			}
			else {
				rec.setError(rp.resultCode, BatchCommand.inDoubt(rec.hasWrite, commandSentCounter));
				status.setRowError();
			}
		}

		@Override
		public void setInDoubt() {
			if (rec.resultCode == ResultCode.NO_RESPONSE) {
				rec.inDoubt = true;
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

	//-------------------------------------------------------
	// Transaction
	//-------------------------------------------------------

	public static final class TxnVerify extends BatchSingleExecutor {
		private final BatchCommand read;
		private final long version;
		private final BatchRecord br;

		public TxnVerify(
			Cluster cluster,
			BatchCommand cmd,
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
				node.addBytesIn(br.key.namespace, rp.bytesIn);
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
			Partition p = new Partition(read.partitions, key, read.replica, null, br.linearize);
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
				node.addBytesIn(br.key.namespace, rp.bytesIn);
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
