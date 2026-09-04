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
package com.aerospike.client.sdk.command;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.PartitionTracker.NodePartitions;
import com.aerospike.client.sdk.metrics.LatencyType;
import com.aerospike.client.sdk.query.KeyRecord;

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
        Key key = parser.parseFieldsQuery(bval);

        if ((parser.info3 & Command.INFO3_PARTITION_DONE) != 0) {
            // Generation is overloaded as partitionId.
            if (parser.resultCode != 0) {
                // PARTITION_UNAVAILABLE is the only per-partition condition reported this way, and
                // is retried in the next round. Every other code rejects the whole query, so
                // retrying would spend all the rounds and then report the partitions as unavailable
                // instead of the reason the server gave.
                if (parser.resultCode != ResultCode.PARTITION_UNAVAILABLE) {
                    throw parser.toException();
                }
                tracker.partitionUnavailable(nodePartitions, parser.generation);
            }
            return true;
        }

        if (parser.resultCode != 0) {
            throw parser.toException();
        }

        Record record = parser.parseRecord(false);

        if (! valid) {
            throw new AerospikeException.QueryTerminated();
        }

        if (tracker.allowRecord(nodePartitions)) {
            stream.publish(new RecordResult(new KeyRecord(key, record), -1));
            if (stream.cancelled().getAsBoolean()) {
                stop();
                throw new AerospikeException.QueryTerminated();
            }
            tracker.setLast(nodePartitions, key, bval.val);
        }
        return true;
    }
}
