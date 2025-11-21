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
import com.aerospike.client.fluent.AsyncRecordStream;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.command.PartitionTracker.NodePartitions;
import com.aerospike.client.fluent.metrics.LatencyType;
import com.aerospike.client.fluent.query.KeyRecord;

public final class QueryNodeExecutor extends NodeExecutor {
	private final QueryCommand query;
	private final long taskId;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;
	private final AsyncRecordStream stream;

	public QueryNodeExecutor(
		Cluster cluster, QueryCommand cmd, long taskId, PartitionTracker tracker,
		NodePartitions nodePartitions, AsyncRecordStream stream
	) {
		super(cluster, cmd, nodePartitions.node);
		this.query = cmd;
		this.taskId = taskId;
		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
		this.stream = stream;
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.QUERY;
	}

	@Override
	protected CommandBuffer getCommandBuffer() {
		CommandBuffer cb = new CommandBuffer();
		cb.setQuery(query, tracker, nodePartitions, taskId);
		return cb;
	}

	@Override
	protected boolean parseRow() {
		BVal bval = new BVal();
		Key key = parseKey(fieldCount, bval);

		if ((info3 & Command.INFO3_PARTITION_DONE) != 0) {
			// When an error code is received, mark partition as unavailable
			// for the current round. Unavailable partitions will be retried
			// in the next round. Generation is overloaded as partitionId.
			if (resultCode != 0) {
				tracker.partitionUnavailable(nodePartitions, generation);
			}
			return true;
		}

		if (resultCode != 0) {
			throw new AerospikeException(resultCode);
		}

		Record record = parseRecord();

		if (! valid) {
			throw new AerospikeException.QueryTerminated();
		}

		if (tracker.allowRecord(nodePartitions)) {
			stream.publish(new RecordResult(new KeyRecord(key, record), -1));
			/*
			if (! recordSet.put(new KeyRecord(key, record))) {
				stop();
				throw new AerospikeException.QueryTerminated();
			}
			*/
			tracker.setLast(nodePartitions, key, bval.val);
		}
		return true;
	}

	private Key parseKey(int fieldCount, BVal bval) {
		byte[] digest = null;
		String namespace = null;
		String setName = null;
		Value userKey = null;

		for (int i = 0; i < fieldCount; i++) {
			int fieldlen = Buffer.bytesToInt(dataBuffer, dataOffset);
			dataOffset += 4;

			int fieldtype = dataBuffer[dataOffset++];
			int size = fieldlen - 1;

			switch (fieldtype) {
			case FieldType.DIGEST_RIPE:
				digest = new byte[size];
				System.arraycopy(dataBuffer, dataOffset, digest, 0, size);
				break;

			case FieldType.NAMESPACE:
				namespace = Buffer.utf8ToString(dataBuffer, dataOffset, size);
				break;

			case FieldType.TABLE:
				setName = Buffer.utf8ToString(dataBuffer, dataOffset, size);
				break;

			case FieldType.KEY:
				int type = dataBuffer[dataOffset++];
				size--;
				userKey = Buffer.bytesToKeyValue(type, dataBuffer, dataOffset, size);
				break;

			case FieldType.BVAL_ARRAY:
				bval.val = Buffer.littleBytesToLong(dataBuffer, dataOffset);
				break;
			}
			dataOffset += size;
		}
		return new Key(namespace, digest, setName, userKey);
	}
}
