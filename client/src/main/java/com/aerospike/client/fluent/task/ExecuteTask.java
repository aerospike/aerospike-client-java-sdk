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
package com.aerospike.client.fluent.task;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Info;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.util.Version;

/**
 * Task used to poll for long-running server execute job completion.
 */
public class ExecuteTask extends Task {
	private final long taskId;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public ExecuteTask(Cluster cluster, long taskId, int timeout) {
		super(cluster, timeout);
		this.taskId = taskId;
	}

	/**
	 * Return task id.
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public int queryStatus() throws AerospikeException {
		// All nodes must respond with complete to be considered done.
		Node[] nodes = cluster.validateNodes();
		String tid = Long.toUnsignedString(taskId);

		for (Node node : nodes) {
			Version serverVersion = node.getVersion();
			String command = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "query-show:id=" + tid : "query-show:trid=" + tid;

			String response = Info.request(node, command, timeout);

			if (response.startsWith("ERROR:2")) {
				// Query not found.
				// Server >= 6.0:  Query has completed.
				// Continue checking other nodes.
				continue;
			}

			if (response.startsWith("ERROR:")) {
				throw new AerospikeException(command + " failed: " + response);
			}

			String find = "status=";
			int index = response.indexOf(find);

			if (index < 0) {
				throw new AerospikeException(command + " failed: " + response);
			}

			int begin = index + find.length();
			int end = response.indexOf(':', begin);
			String status = response.substring(begin, end);

			// Newer servers use "done" while older servers use "DONE"
			if (! (status.startsWith("done") || status.startsWith("DONE"))) {
				return Task.IN_PROGRESS;
			}
		}
		return Task.COMPLETE;
	}
}
