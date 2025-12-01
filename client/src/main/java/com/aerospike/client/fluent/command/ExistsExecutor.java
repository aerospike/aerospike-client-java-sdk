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
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Connection;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;

public final class ExistsExecutor extends SyncExecutor {
	private final ReadCommand read;
	private boolean exists;

	public ExistsExecutor(Cluster cluster, ReadCommand cmd) {
		super(cluster, cmd);
		this.read = cmd;
		cluster.addCommandCount();
	}

	@Override
	protected final Node getNode() {
		return read.partition.getNodeRead(cluster);
	}

	@Override
	protected final LatencyType getLatencyType() {
		return LatencyType.READ;
	}

	@Override
	protected CommandBuffer getCommandBuffer() {
		CommandBuffer cb = new CommandBuffer();
		cb.setExists(read);
		return cb;
	}

	@Override
	protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
		RecordParser rp = new RecordParser(conn, buffer);
		rp.parseFields(cmd.txn, read.key, false);

		if (node.isMetricsEnabled()) {
			node.addBytesIn(cmd.namespace, rp.bytesIn);
		}

		if (rp.resultCode == ResultCode.OK) {
			exists = true;
			return;
		}

		if (rp.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
			exists = false;
			return;
		}

		if (rp.resultCode == ResultCode.FILTERED_OUT) {
			if (read.failOnFilteredOut) {
				throw new AerospikeException(rp.resultCode);
			}
			exists = true;
			return;
		}

		throw new AerospikeException(rp.resultCode);
	}

	@Override
	protected final boolean prepareRetry(boolean timeout) {
		read.partition.prepareRetryRead(timeout);
		return true;
	}

	public boolean exists() {
		return exists;
	}
}
