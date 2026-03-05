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
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;

public class DeleteExecutor extends SyncExecutor {
	private final WriteCommand delete;
	private boolean existed;

	public DeleteExecutor(Cluster cluster, WriteCommand cmd) {
		super(cluster, cmd);
		this.delete = cmd;
		cluster.addCommandCount();
	}

	@Override
	protected final boolean isWrite() {
		return true;
	}

	@Override
	protected final Node getNode() {
		return delete.partition.getNodeWrite(cluster);
	}

	@Override
	protected final LatencyType getLatencyType() {
		return LatencyType.WRITE;
	}

	@Override
	protected CommandBuffer getCommandBuffer() {
		CommandBuffer cb = new CommandBuffer();
		cb.setDelete(delete);
		return cb;
	}

	@Override
	protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
		RecordParser rp = new RecordParser(conn, buffer);
		rp.parseFields(cmd.txn, delete.key, true);

		if (node.isMetricsEnabled()) {
			node.addBytesIn(cmd.namespace, rp.bytesIn);
		}

		if (rp.resultCode == ResultCode.OK) {
			existed = true;
			return;
		}

		if (rp.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			existed = false;
			return;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (delete.failOnFilteredOut) {
				throw AerospikeException.resultCodeToException(rp.resultCode, null);
			}
			existed = true;
			return;
		}

		throw AerospikeException.resultCodeToException(rp.resultCode, null);
	}

	@Override
	protected final boolean prepareRetry(boolean timeout) {
		delete.partition.prepareRetryWrite(timeout);
		return true;
	}

	@Override
	protected void onInDoubt() {
		if (cmd.txn != null) {
			cmd.txn.onWriteInDoubt(delete.key);
		}
	}

	public final boolean existed() {
		return existed;
	}
}
