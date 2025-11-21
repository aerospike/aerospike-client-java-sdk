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

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;

public final class BackgroundQueryNodeExecutor extends NodeExecutor {
	private final BackgroundQueryCommand query;
	private final NodeStatus status;

	public BackgroundQueryNodeExecutor(
		Cluster cluster, BackgroundQueryCommand cmd, Node node, NodeStatus status
	) {
		super(cluster, cmd, node);
		this.query = cmd;
		this.status = status;
	}

	@Override
	protected boolean isWrite() {
		return true;
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.QUERY;
	}

	@Override
	protected CommandBuffer getCommandBuffer() {
		CommandBuffer cb = new CommandBuffer();
		cb.setBackgroundQuery(query);
		return cb;
	}

	@Override
	protected boolean parseRow() {
		skipKey(fieldCount);

		// Server commands (Query/Execute UDF) should only send back a return code.
		if (resultCode != 0) {
			// Background scans (with null query filter) return KEY_NOT_FOUND_ERROR
			// when the set does not exist on the target node.
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
				// Non-fatal error.
				return false;
			}
			throw new AerospikeException(resultCode);
		}

		if (opCount > 0) {
			throw new AerospikeException.Parse("Unexpectedly received bins on background query!");
		}

		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}
		return true;
	}

	@Override
	protected void addSubException(AerospikeException ae) {
		status.addSubException(ae);
	}
}
