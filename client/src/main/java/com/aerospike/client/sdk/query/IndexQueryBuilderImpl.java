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
package com.aerospike.client.sdk.query;

import java.util.Objects;

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.AbstractFilterableBuilder;
import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ErrorHandler;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.plan.QueryPlan;

public class IndexQueryBuilderImpl extends QueryImpl {
    private final DataSet dataSet;

    public IndexQueryBuilderImpl(QueryBuilder builder, Session session, DataSet dataSet) {
        super(builder, session);
        this.dataSet = dataSet;
    }

    DataSet getDataSet() {
        return dataSet;
    }

    @Override
    public boolean allowsSecondaryIndexQuery() {
        return true;
    }

    @Override
    public RecordStream execute() {
        return executeInternal();
    }

    @Override
    public RecordStream execute(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeInternal();
    }

    @Override
    public RecordStream execute(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        return AbstractFilterableBuilder.filterStreamErrors(executeInternal(), handler);
    }

    @Override
    public RecordStream executeAsync(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeInternal();
    }

    @Override
    public RecordStream executeAsync(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        RecordStream source = executeInternal();

        Session session = getSession();
        Cluster cluster = session.getCluster();
        ResolvedSettings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        AsyncRecordStream filtered = new AsyncRecordStream(policy.getRecordQueueSize());

        cluster.startVirtualThread(() -> {
            try {
                source.forEach(result -> {
                    if (!result.isOk()) {
                        AerospikeException ex = result.exception() != null
                            ? result.exception()
                            : AerospikeException.resultCodeToException(result.resultCode(), result.message(), result.inDoubt());
                        handler.handle(result.key(), result.index(), ex);
                    } else {
                        filtered.publish(result);
                    }
                });
            } finally {
                filtered.complete();
            }
        });

        return new RecordStream(filtered);
    }

    private RecordStream executeInternal() {
        Session session = getSession();
        Cluster cluster = session.getCluster();
        QueryBuilder qb = getQueryBuilder();

        // Check for operations - not supported on servers < 8.1.2
        if (!cluster.supportsQueryOperations() && qb.getOperations() != null &&
            !qb.getOperations().isEmpty()) {
            throw AerospikeException.resultCodeToException(ResultCode.OP_NOT_APPLICABLE,
                "Index query with read operations requires server version 8.1.2+. Server version is " +
                cluster.getVersion());
        }

        ResolvedSettings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        WhereClauseProcessor where = getQueryBuilder().getAel();
        QueryCommand cmd;

        if (IndexProbePlanner.useServerQuerySelection(cluster, where, qb.getQueryHint())) {
            QueryPlan plan = IndexProbePlanner.plan(session, dataSet, where, qb.getQueryHint());
            cmd = QueryCommand.forPlan(cluster, dataSet, plan, policy, qb);
        }
        else {
            Filter filter = null;
            Expression filterExp = null;

            if (where != null) {
                ParseResult pr = where.process(dataSet.getNamespace(), dataSet.getSet(), session);
                filter = pr.getFilter();
                filterExp = pr.getExpression();
            }

            cmd = new QueryCommand(cluster, dataSet, filter, filterExp, policy, qb);
        }

        AsyncRecordStream stream = new AsyncRecordStream(policy.getRecordQueueSize());
        cmd.execute(stream);

        if (qb.getChunkSize() == 0) {
            // Normal query
            return new RecordStream(stream);

        }
        else {
            // Paginated query
            return new RecordStream(stream, cmd, qb.getLimit(), policy.getRecordQueueSize());
        }
    }
}
