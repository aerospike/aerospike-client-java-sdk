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

import java.io.IOException;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.metrics.LatencyType;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.util.RandomShift;

public final class IndexProbeExecutor extends SyncExecutor {
    private final IndexProbeCommand probe;
    private final Node[] nodes;
    private int nodeIndex;
    private QueryPlan plan;

    public IndexProbeExecutor(Cluster cluster, IndexProbeCommand cmd) {
        super(cluster, cmd);
        this.probe = cmd;
        this.nodes = cluster.validateNodes();
        this.nodeIndex = new RandomShift().nextInt(nodes.length);
        cluster.addCommandCount();
    }

    @Override
    protected Node getNode() {
        return nodes[nodeIndex];
    }

    @Override
    protected LatencyType getLatencyType() {
        return LatencyType.QUERY;
    }

    @Override
    protected CommandBuffer getCommandBuffer() {
        CommandBuffer cb = new CommandBuffer();
        cb.setIndexProbe(probe);
        return cb;
    }

    @Override
    protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
        RecordParser rp = new RecordParser(conn, buffer);

        if (node.isMetricsEnabled()) {
            node.addBytesIn(cmd.namespace, rp.bytesIn);
        }

        if (rp.resultCode != ResultCode.OK && rp.resultCode != ResultCode.FILTERED_OUT) {
            throw AerospikeException.resultCodeToException(rp.resultCode, null);
        }

        byte[] predicateBytes = probe.where.getBytes();
        plan = QueryPlan.fromProbeResponse(
            rp.resultCode,
            probe.namespace,
            probe.set,
            predicateBytes,
            MsgFieldParser.from(rp)
        );
    }

    @Override
    protected boolean prepareRetry(boolean timeout) {
        if (nodes.length > 1) {
            nodeIndex = (nodeIndex + 1) % nodes.length;
        }
        return true;
    }

    public QueryPlan getPlan() {
        return plan;
    }
}
