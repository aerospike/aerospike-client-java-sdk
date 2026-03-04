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
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;

public class UdfExecutor extends SyncExecutor {
	private final UdfCommand udf;
	private Record rec;

	public UdfExecutor(Cluster cluster, UdfCommand cmd) {
		super(cluster, cmd);
		this.udf = cmd;
		cluster.addCommandCount();
	}

	@Override
	protected final boolean isWrite() {
		return true;
	}

	@Override
	protected final Node getNode() {
		return udf.partition.getNodeWrite(cluster);
	}

	@Override
	protected final LatencyType getLatencyType() {
		return LatencyType.WRITE;
	}

	@Override
	protected CommandBuffer getCommandBuffer() {
		CommandBuffer cb = new CommandBuffer();
		cb.setUdf(udf);
		return cb;
	}

	@Override
	protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
		RecordParser rp = new RecordParser(conn, buffer);
		rp.parseFields(cmd.txn, udf.key, true);

		if (node.isMetricsEnabled()) {
			node.addBytesIn(cmd.namespace, rp.bytesIn);
		}

		if (rp.resultCode == ResultCode.OK) {
			rec = rp.parseRecord(false);
			return;
		}

		if (rp.resultCode == ResultCode.UDF_BAD_RESPONSE) {
			rec = rp.parseRecord(false);
			handleUdfError(rp.resultCode);
			return;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (udf.failOnFilteredOut) {
				throw new AerospikeException(rp.resultCode);
			}
			return;
		}

		throw new AerospikeException(rp.resultCode);
	}

	@Override
	protected final boolean prepareRetry(boolean timeout) {
		udf.partition.prepareRetryWrite(timeout);
		return true;
	}

	@Override
	protected void onInDoubt() {
		if (cmd.txn != null) {
			cmd.txn.onWriteInDoubt(udf.key);
		}
	}

	private void handleUdfError(int resultCode) {
		String ret = (String)rec.bins.get("FAILURE");

		if (ret == null) {
			throw new AerospikeException(resultCode);
		}

		String message;
		int code;

		try {
			String[] list = ret.split(":");
			code = Integer.parseInt(list[2].trim());
			message = list[0] + ':' + list[1] + ' ' + list[3];
		}
		catch (Throwable e) {
			// Use generic exception if parse error occurs.
			throw new AerospikeException(resultCode, ret);
		}

		throw new AerospikeException(code, message);
	}

	public Record getRecord() {
		return rec;
	}
}
