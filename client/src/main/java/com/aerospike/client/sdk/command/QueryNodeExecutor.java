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
            // When an error code is received, mark partition as unavailable
            // for the current round. Unavailable partitions will be retried
            // in the next round. Generation is overloaded as partitionId.
            if (parser.resultCode != 0) {
                tracker.partitionUnavailable(nodePartitions, parser.generation);
            }
            return true;
        }

        if (parser.resultCode != 0) {
            throw parser.toException();
        }

        // A query carrying read operations can return the same bin name more than once,
        // so its results must be merged into a list rather than overwriting each other.
        /*
         TODO: BN: confirm this is correct. Old code was:
         Record record = parser.parseRecord(false);
         However this fails in test CdtBuilderPermutationTest.DatasetQueryTopLevel.mapAndListNavigations with
         org.opentest4j.AssertionFailedError: cdtSMap result count ==> expected: <49> but was: <1>

         Claude's reasoning for the code change was:
         The dataset query result parser was discarding all but the last result when several operations targeted the same bin. RecordParser.parseRecord takes an isOperation flag that controls exactly this:
         if (isOperation) {
             if (bins.containsKey(name)) {
                 // Multiple values returned for the same bin.
                 Object prev = bins.get(name);
                 // ... accumulates into an OpResults list ...
             }
             else {
                 bins.put(name, value);
             }
         }
         else {
             bins.put(name, value);
         }
         Every other path that can return multiple results per bin passes true — OperateReadExecutor, OperateWriteExecutor, ReadExecutor, and the batch operate commands — which is why the update and 
         key-query permutations passed. QueryNodeExecutor was hardcoding false, so the 49 cdtSMap operations collapsed to a single value, and getValue("cdtSMap") returned the last one instead of an OpResults list.

        The fix makes the flag reflect whether the query actually carries operations:
         */
        Record record = parser.parseRecord(query.ops != null && !query.ops.isEmpty());

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
