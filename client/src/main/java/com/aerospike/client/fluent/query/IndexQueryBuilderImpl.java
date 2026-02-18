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
package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.AsyncRecordStream;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.QueryCommand;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.dsl.ParseResult;

public class IndexQueryBuilderImpl extends QueryImpl {
    private final DataSet dataSet;

    public IndexQueryBuilderImpl(QueryBuilder builder, Session session, DataSet dataSet) {
        super(builder, session);
        this.dataSet = dataSet;
    }

    @Override
    public boolean allowsSecondaryIndexQuery() {
        return true;
    }
    @Override
    public RecordStream execute() {
        if (getQueryBuilder().getTxnToUse() != null) {
            return executeSync();
        } else {
            return executeAsync();
        }
    }

    @Override
    public RecordStream executeSync() {
        return executeInternal();
    }

    @Override
    public RecordStream executeAsync() {
        if (getQueryBuilder().getTxnToUse() != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using executeSync() or execute() for transactional safety."
            );
        }
        // Index queries stream results; async and sync behave similarly
        return executeInternal();
    }

    private RecordStream executeInternal() {
        Session session = getSession();
        Cluster cluster = session.getCluster();
        QueryBuilder qb = getQueryBuilder();
        
        // Check for operations - not supported on index/scan queries
        if (qb.getOperations() != null && !qb.getOperations().isEmpty()) {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR,
                "CDT read operations and expression operations are not currently supported on " +
                "dataset-based queries (scans and secondary index queries). " +
                "Use key-based queries instead: session.query(dataSet.id(key1, key2, ...))");
        }
        Settings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        WhereClauseProcessor dsl = getQueryBuilder().getDsl();
        Filter filter = null;
        Expression filterExp = null;

        if (dsl != null) {
        	ParseResult pr = dsl.process(dataSet.getNamespace(), getSession());
        	filter = pr.getFilter();
        	filterExp = pr.getExpression();
        }

        AsyncRecordStream stream = new AsyncRecordStream(policy.getRecordQueueSize());
        QueryCommand cmd = new QueryCommand(cluster, dataSet, filter, filterExp, policy, qb);
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
