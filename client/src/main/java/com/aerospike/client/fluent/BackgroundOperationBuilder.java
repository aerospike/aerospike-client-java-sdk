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
package com.aerospike.client.fluent;

import java.util.Random;
import java.util.concurrent.ExecutorService;

import com.aerospike.client.fluent.command.BackgroundQueryCommand;
import com.aerospike.client.fluent.command.BackgroundQueryNodeExecutor;
import com.aerospike.client.fluent.command.NodeStatus;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.Filter;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;
import com.aerospike.client.fluent.task.ExecuteTask;
import com.aerospike.dsl.ParseResult;

/**
 * Builder for server-side background operations that run on entire sets.
 *
 * <p>Background operations execute asynchronously on the server and return
 * an {@link ExecuteTask} for monitoring completion. Unlike regular operations,
 * background operations:</p>
 * <ul>
 *   <li>Operate on entire sets (no specific keys)</li>
 *   <li>Run as server-side scans/queries</li>
 *   <li>Cannot be part of transactions</li>
 *   <li>Do not return record data, only completion status</li>
 *   <li>Support only UPDATE, DELETE, and TOUCH operations</li>
 * </ul>
 *
 * <p>Obtain instances via {@link BackgroundTaskSession}, which is accessed
 * through {@link Session#backgroundTask()}.</p>
 *
 * <p><b>Example:</b></p>
 * <pre>{@code
 * ExecuteTask task = session.backgroundTask()
 *     .update(customerDataSet)
 *     .where("$.age > 30")
 *     .bin("category").setTo("senior")
 *     .expireRecordAfter(Duration.ofDays(90))
 *     .execute();
 *
 * // Monitor completion
 * task.waitTillComplete();
 * System.out.println("Records processed: " + task.getRecordsRead());
 * }</pre>
 *
 * @see BackgroundTaskSession
 * @see Session#backgroundTask()
 */
public class BackgroundOperationBuilder extends AbstractOperationBuilder<BackgroundOperationBuilder> implements FilterableOperation<BackgroundOperationBuilder> {
    private final DataSet dataset;
    private int recordsPerSecond = 0;

    /**
     * Constructs a background operation builder.
     *
     * @param session The session to use for the operation
     * @param dataset The dataset (namespace + set) to operate on
     * @param opType The operation type (must be UPDATE, DELETE, or TOUCH)
     * @throws IllegalArgumentException if operation type is not UPDATE, DELETE, or TOUCH
     * @throws IllegalStateException if called within a transaction
     */
    public BackgroundOperationBuilder(Session session, DataSet dataset, OpType opType) {
        super(session, opType);

        // Validate operation type - only UPDATE, DELETE, TOUCH allowed
        if (opType != OpType.UPDATE && opType != OpType.DELETE && opType != OpType.TOUCH) {
            throw new IllegalArgumentException(
                "Background operations only support UPDATE, DELETE, and TOUCH. Got: " + opType);
        }

        // Validate no active transaction
        if (session.getCurrentTransaction() != null) {
            throw new IllegalStateException(
                "Background operations cannot be used within transactions");
        }

        this.dataset = dataset;
    }

    /**
     * Adds a where clause filter to the background operation using a DSL string.
     * The filter determines which records in the set will be affected.
     *
     * @param dsl The DSL filter expression (e.g., "$.age > 30")
     * @param params The parameters to substitute into the DSL expression
     * @return This builder for method chaining
     */
    @Override
    public BackgroundOperationBuilder where(String dsl, Object... params) {
        setWhereClause(createWhereClauseProcessor(true, dsl, params));
        return this;
    }

    /**
     * Adds a where clause filter to the background operation using a BooleanExpression.
     * The filter determines which records in the set will be affected.
     *
     * @param dsl The BooleanExpression filter
     * @return This builder for method chaining
     */
    @Override
    public BackgroundOperationBuilder where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

    /**
     * Adds a where clause filter to the background operation using a PreparedDsl.
     * The filter determines which records in the set will be affected.
     *
     * @param dsl The PreparedDsl filter
     * @param params Parameters to bind to the prepared DSL
     * @return This builder for method chaining
     */
    @Override
    public BackgroundOperationBuilder where(PreparedDsl dsl, Object... params) {
        setWhereClause(WhereClauseProcessor.from(true, dsl, params));
        return this;
    }

    /**
     * Adds a where clause filter to the background operation using an Exp operation.
     * The filter determines which records in the set will be affected.
     *
     * @param exp The expression to validate the records against
     * @return This builder for method chaining
     */
    @Override
    public BackgroundOperationBuilder where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    /**
     * Adds a where clause filter to the background operation using an Expression operation.
     * The filter determines which records in the set will be affected.
     *
     * @param e The expression to validate the records against
     * @return This builder for method chaining
     */
    @Override
    public BackgroundOperationBuilder where(Expression e) {
        setWhereClause(WhereClauseProcessor.from(e));
        return this;
    }

    /**
     * Rate limit the records per second processed by this background operation.
     *
     * <p>This can be used to limit the server-side load of background operations
     * to prevent them from impacting normal operations.</p>
     *
     * @param recordsPerSecond the maximum records per second to process (0 = unlimited)
     * @return This builder for method chaining
     */
    public BackgroundOperationBuilder recordsPerSecond(int recordsPerSecond) {
        this.recordsPerSecond = recordsPerSecond;
        return this;
    }

    /**
     * Not applicable for background operations since they operate on entire sets, not specific keys.
     *
     * @return This builder for method chaining (no-op)
     * @throws UnsupportedOperationException as this method is not applicable to background operations
     */
    @Override
    public BackgroundOperationBuilder failOnFilteredOut() {
        throw new UnsupportedOperationException(
            "failOnFilteredOut() is not applicable to background operations. " +
            "This method is only for key-based operations.");
    }

    /**
     * Not applicable for background operations since they operate on entire sets, not specific keys.
     *
     * @return This builder for method chaining (no-op)
     * @throws UnsupportedOperationException as this method is not applicable to background operations
     */
    @Override
    public BackgroundOperationBuilder respondAllKeys() {
        throw new UnsupportedOperationException(
            "respondAllKeys() is not applicable to background operations. " +
            "This method is only for key-based operations.");
    }

    /**
     * Execute the background operation on the server.
     *
     * <p>This method submits the operation to the server for asynchronous execution
     * and immediately returns an ExecuteTask that can be used to monitor progress
     * and completion.</p>
     *
     * @return ExecuteTask for monitoring the background operation
     * @throws com.aerospike.client.AerospikeException if the operation fails to start
     */
    public ExecuteTask execute() {
    	Cluster cluster = session.getCluster();
		Node[] nodes = cluster.validateNodes();

		cluster.addCommandCount();

        boolean retryable = areOperationsRetryable(ops);

        Settings settings = session.getBehavior().getSettings(
            retryable ? OpKind.WRITE_RETRYABLE : OpKind.WRITE_NON_RETRYABLE,
            OpShape.QUERY,
            Mode.ANY
        );

        Filter filter = null;
        Expression filterExp = null;

        if (dsl != null) {
        	ParseResult pr = dsl.process(dataset.getNamespace(), session);
        	filter = pr.getFilter();
        	filterExp = pr.getExpression();

        	//Exp exp = pr.getExp();
        	//System.out.println("BACKGROUND FILTEREXP: " + exp.toString());
        }

        // Add filter expression if where clause is present
        int ttl = getExpirationAsInt();
		long taskId = new Random().nextLong();

        BackgroundQueryCommand cmd = new BackgroundQueryCommand(cluster, dataset, taskId, opType,
    		ops, ttl, filter, filterExp, settings, recordsPerSecond);

        final NodeStatus status = new NodeStatus();

        try (ExecutorService es = cluster.getExecutorService()) {
    		for (Node node : nodes) {
                es.submit(() -> {
                    try {
                    	BackgroundQueryNodeExecutor exec = new BackgroundQueryNodeExecutor(cluster, cmd, node, status);
                    	exec.execute();
                    }
                    catch (AerospikeException ae) {
                    	status.setException(ae);
                    }
                    catch (Throwable t) {
                    	status.setException(new AerospikeException(t));
                    }
                });
    		}
        }

        status.checkException();

		return new ExecuteTask(cluster, taskId, cmd.socketTimeout);
    }

    // Note: areOperationsRetryable() is inherited from AbstractOperationBuilder
}

