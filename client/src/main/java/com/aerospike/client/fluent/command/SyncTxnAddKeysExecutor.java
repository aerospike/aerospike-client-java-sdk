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
import com.aerospike.client.fluent.Connection;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.ResultCode;

public final class SyncTxnAddKeysExecutor extends OperateWriteExecutor {
	public SyncTxnAddKeysExecutor(Cluster cluster, OperateWriteCommand cmd) {
		super(cluster, cmd);
	}

	@Override
	protected CommandBuffer getCommandBuffer() {
		CommandBuffer cb = new CommandBuffer();
		cb.setTxnAddKeys(operate);
		return cb;
	}

	@Override
	protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
		RecordParser rp = new RecordParser(conn, buffer);
		rp.parseTranDeadline(cmd.txn);

		if (node.isMetricsEnabled()) {
			node.addBytesIn(cmd.namespace, rp.bytesIn);
		}

		if (rp.resultCode == ResultCode.OK) {
			return;
		}

		throw new AerospikeException(rp.resultCode, "Server error");
	}

	@Override
	protected void onInDoubt() {
	}
}
