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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.metrics.LatencyType;
import com.aerospike.client.sdk.query.plan.IndexRangeWire;
import com.aerospike.client.sdk.query.plan.QueryPlan;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;
import com.aerospike.client.sdk.util.RandomShift;

public final class IndexProbeExecutor extends SyncExecutor {
    private static final Logger log = LoggerFactory.getLogger(IndexProbeExecutor.class);

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
        cb.setQueryExplain(probe);
        return cb;
    }

    @Override
    protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
        RecordParser rp = new RecordParser(conn, buffer);

        if (node.isMetricsEnabled()) {
            node.addBytesIn(cmd.namespace, rp.bytesIn);
        }

        if (rp.resultCode != ResultCode.OK && rp.resultCode != ResultCode.FILTERED_OUT) {
            throw rp.toException();
        }

        byte[] whereBytes = QueryWhereWire.forExplain(probe.whereFlags, probe.ael);
        plan = QueryPlan.fromExplainResponse(
            rp.resultCode,
            probe.namespace,
            probe.set,
            whereBytes,
            MsgFieldParser.from(rp)
        );

        if (log.isDebugEnabled()) {
            logQueryPlan(node, plan);
        }
    }

    private void logQueryPlan(Node node, QueryPlan plan) {
        String range = IndexRangeWire.describeProbeRange(plan.getIndexRangeBytes());
        String indexHint = probe.indexNameHint != null ? probe.indexNameHint : "none";
        String whereFlags = formatWhereFlags(probe.whereFlags);

        if (plan.isSecondaryIndex()) {
            log.debug(
                "query-plan: node={} ns={} set={} selected sindex={} {} indexType={} "
                    + "ael={} indexHint={} whereFlags={}",
                node,
                plan.getNamespace(),
                plan.getSet(),
                plan.getIndexName(),
                range,
                plan.getIndexType(),
                plan.getAel(),
                indexHint,
                whereFlags
            );
            return;
        }

        log.debug(
            "query-plan: node={} ns={} set={} selection={} ael={} indexHint={} whereFlags={}",
            node,
            plan.getNamespace(),
            plan.getSet(),
            plan.getSelection(),
            plan.getAel(),
            indexHint,
            whereFlags
        );
    }

    private static String formatWhereFlags(int flags) {
        int policyFlags = flags & (QueryWhereWire.FLAG_REQUIRE_INDEX | QueryWhereWire.FLAG_HARD_HINT);
        if (policyFlags == 0) {
            return "none";
        }

        StringBuilder sb = new StringBuilder();
        if ((policyFlags & QueryWhereWire.FLAG_REQUIRE_INDEX) != 0) {
            sb.append("REQUIRE_INDEX|");
        }
        if ((policyFlags & QueryWhereWire.FLAG_HARD_HINT) != 0) {
            sb.append("HARD_HINT|");
        }
        return sb.substring(0, sb.length() - 1);
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
