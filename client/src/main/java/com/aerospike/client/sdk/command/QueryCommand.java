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

import java.util.List;

import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.QueryBuilder;
import com.aerospike.client.sdk.query.QueryHint;
import com.aerospike.client.sdk.query.plan.IndexRangeWire;
import com.aerospike.client.sdk.query.plan.QueryPlan;

public final class QueryCommand extends Command {
    final String set;
    final Filter filter;
    final PartitionFilter pf;
    final QueryDuration expectedDuration;
    final long maxRecords;
    final String[] binNames;
    final List<Operation> ops;
    final int maxConcurrentNodes;
    final int recordsPerSecond;
    final int readTouchTtlPercent;
    final boolean withNoBins;
    final boolean planDriven;
    /** Field {@code 44} execute payload when plan-driven; {@code null} on legacy path. */
    final byte[] executeWhereBytes;

    public QueryCommand(
        Cluster cluster, DataSet set, Filter filter, Expression filterExp,
        ResolvedSettings settings, QueryBuilder qb
    ) {
        this(cluster, set, filter, filterExp, settings, qb, null);
    }

    /**
     * Build an execute command from a server {@link QueryPlan} (explain result).
     * Plan pins win over query hints; field {@code 44} is sent without EXPLAIN.
     * Field {@code 22} is normalized for execute ({@code bin_name_len = 0}) when field {@code 21} is sent.
     */
    public static QueryCommand forPlan(
        Cluster cluster,
        DataSet set,
        QueryPlan plan,
        ResolvedSettings settings,
        QueryBuilder qb
    ) {
        if (plan.isFilteredOut()) {
            throw com.aerospike.client.sdk.AerospikeException.toException(
                com.aerospike.client.sdk.ResultCode.FILTERED_OUT,
                "Query plan filtered out by server"
            );
        }

        Filter filter = null;
        if (plan.isSecondaryIndex()) {
            byte[] executeRange = IndexRangeWire.forExecuteWithIndexName(plan.getIndexRangeBytes());
            filter = Filter.fromWireRange(plan.getIndexName(), executeRange, plan.getIndexType());
        }
        return new QueryCommand(cluster, set, filter, null, settings, qb, plan);
    }

    private QueryCommand(
        Cluster cluster, DataSet set, Filter filter, Expression filterExp,
        ResolvedSettings settings, QueryBuilder qb, QueryPlan plan
    ) {
        super(cluster, set.getNamespace(), null, filterExp, settings.getReplicaOrder(), settings);
        this.set = set.getSet();
        this.planDriven = plan != null;
        this.executeWhereBytes = plan != null ? plan.getExecuteWhereBytes() : null;
        this.filter = planDriven ? filter : applyHintToFilter(filter, qb.getQueryHint());

        this.pf = PartitionFilter.range(qb.getStartPartition(),
            qb.getEndPartition() - qb.getStartPartition());

        this.recordsPerSecond = qb.getRecordsPerSecond();
        this.expectedDuration = qb.getEffectiveQueryDuration();
        this.binNames = qb.getBinNames();
        this.ops = qb.getOperations();
        this.maxConcurrentNodes = settings.getMaxConcurrentNodes();
        this.readTouchTtlPercent = settings.getResetTtlOnReadAtPercent();
        this.withNoBins = qb.getWithNoBins();

        if (qb.getChunkSize() > 0) {
            this.maxRecords = qb.getChunkSize();
        }
        else if (qb.getChunkSize() == 0 && qb.getLimit() > 0) {
            this.maxRecords = qb.getLimit();
        }
        else {
            this.maxRecords = 0;
        }
    }

    public boolean isPlanDriven() {
        return planDriven;
    }

    public void execute(AsyncRecordStream stream) {
        Node[] nodes = cluster.validateNodes();

        PartitionTracker tracker = new PartitionTracker(this, nodes, pf);
        QueryExecutor exec = new QueryExecutor(cluster, this, nodes.length, tracker, stream);

        cluster.startVirtualThread(() -> {
            try {
                exec.execute();
            }
            catch (Throwable e) {
                exec.stopThreads(e);
            }
        });
    }

    public boolean isDone() {
        return pf.isDone();
    }

    private static Filter applyHintToFilter(Filter filter, QueryHint.Result hint) {
        if (hint == null || filter == null) {
            return filter;
        }
        String hintIndex = hint.getIndexName();
        String hintBin = hint.getBinName();
        if (hintIndex == null && hintBin == null) {
            return filter;
        }
        if (hintIndex != null) {
            return Filter.withOverrides(filter, null, hintIndex);
        }
        return Filter.withOverrides(filter, hintBin, null);
    }
}
