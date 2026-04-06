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
package com.aerospike.client.sdk;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.command.BackgroundQueryCommand;
import com.aerospike.client.sdk.command.BackgroundQueryNodeExecutor;
import com.aerospike.client.sdk.command.NodeStatus;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.query.WhereClauseProcessor;
import com.aerospike.client.sdk.task.ExecuteTask;

/**
 * Builder for server-side background UDF (User Defined Function) operations.
 *
 * <p>Background UDF operations execute a registered Lua function against every record
 * in a set (optionally filtered by a where clause). They run asynchronously on the
 * server and return an {@link ExecuteTask} for monitoring completion.</p>
 *
 * <p>Unlike foreground {@code executeUdf} which operates on specific keys, background
 * UDFs operate on entire sets via server-side scans/queries.</p>
 *
 * <p><b>Example:</b></p>
 * <pre>{@code
 * ExecuteTask task = session.backgroundTask()
 *     .executeUdf(products)
 *     .function("inventory", "applyDiscount")
 *     .passing(0.20)
 *     .where("$.stock > 250")
 *     .recordsPerSecond(5000)
 *     .execute();
 *
 * task.waitTillComplete();
 * }</pre>
 *
 * @see BackgroundUdfFunctionBuilder
 * @see BackgroundTaskSession#executeUdf(DataSet)
 */
public class BackgroundUdfBuilder extends AbstractSessionOperationBuilder<BackgroundUdfBuilder>
        implements FilterableOperation<BackgroundUdfBuilder> {

    private final DataSet dataset;
    private final String packageName;
    private final String functionName;
    private Value[] functionArgs = new Value[0];
    private int recordsPerSecond = 0;

    /**
     * Package-private constructor.
     *
     * @param session the session to use for execution
     * @param dataset the dataset (namespace + set) to operate on
     * @param packageName the UDF package name
     * @param functionName the UDF function name
     */
    BackgroundUdfBuilder(Session session, DataSet dataset, String packageName, String functionName) {
        super(session, OpType.UDF);

        if (session.getCurrentTransaction() != null) {
            throw new IllegalStateException(
                "Background operations cannot be used within transactions");
        }

        this.dataset = dataset;
        this.packageName = packageName;
        this.functionName = functionName;
    }

    /**
     * Specify arguments to pass to the UDF.
     * Arguments are converted to Aerospike Value objects using {@link Value#get(Object)}.
     *
     * <p>Supported argument types include:
     * <ul>
     *   <li>String</li>
     *   <li>byte, short, int, long, float, double</li>
     *   <li>boolean</li>
     *   <li>byte[]</li>
     *   <li>List</li>
     *   <li>Map</li>
     *   <li>Value (passed through directly)</li>
     * </ul>
     *
     * @param args the arguments to pass to the UDF
     * @return this builder for method chaining
     */
    public BackgroundUdfBuilder passing(Object... args) {
        if (args == null || args.length == 0) {
            this.functionArgs = new Value[0];
        } else {
            Value[] values = new Value[args.length];
            for (int i = 0; i < args.length; i++) {
                values[i] = Value.get(args[i]);
            }
            this.functionArgs = values;
        }
        return this;
    }

    /**
     * Specify arguments to pass to the UDF using explicit Value objects.
     * Use this method when you need more control over type conversion.
     *
     * @param args the Value arguments to pass to the UDF
     * @return this builder for method chaining
     */
    public BackgroundUdfBuilder passingValues(Value... args) {
        this.functionArgs = args == null ? new Value[0] : args;
        return this;
    }

    /**
     * Specify arguments to pass to the UDF from a List.
     *
     * @param args the list of arguments to pass to the UDF
     * @return this builder for method chaining
     */
    public BackgroundUdfBuilder passing(List<?> args) {
        if (args == null || args.isEmpty()) {
            this.functionArgs = new Value[0];
        } else {
            Value[] values = new Value[args.size()];
            for (int i = 0; i < args.size(); i++) {
                values[i] = Value.get(args.get(i));
            }
            this.functionArgs = values;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The predicate is applied on the server so only matching records run the UDF.
     */
    @Override
    public BackgroundUdfBuilder where(String ael, Object... params) {
        setWhereClause(createWhereClauseProcessor(true, ael, params));
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The predicate is applied on the server so only matching records run the UDF.
     */
    @Override
    public BackgroundUdfBuilder where(BooleanExpression ael) {
        setWhereClause(WhereClauseProcessor.from(ael));
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The predicate is applied on the server so only matching records run the UDF.
     */
    @Override
    public BackgroundUdfBuilder where(PreparedAel ael, Object... params) {
        setWhereClause(WhereClauseProcessor.from(true, ael, params));
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The predicate is applied on the server so only matching records run the UDF.
     */
    @Override
    public BackgroundUdfBuilder where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The predicate is applied on the server so only matching records run the UDF.
     */
    @Override
    public BackgroundUdfBuilder where(Expression e) {
        setWhereClause(WhereClauseProcessor.from(e));
        return this;
    }

    /**
     * Rate limit the records per second processed by this background UDF.
     *
     * <p>This can be used to limit the server-side load to prevent the
     * background operation from impacting normal operations.</p>
     *
     * @param recordsPerSecond the maximum records per second to process (0 = unlimited)
     * @return this builder for method chaining
     */
    public BackgroundUdfBuilder recordsPerSecond(int recordsPerSecond) {
        this.recordsPerSecond = recordsPerSecond;
        return this;
    }

    /**
     * Background UDF runs over a set (or filtered subset), not explicit keys, so filtered-out semantics
     * for key-based reads do not apply.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public BackgroundUdfBuilder failOnFilteredOut() {
        throw new UnsupportedOperationException(
            "failOnFilteredOut() is not applicable to background operations. " +
            "This method is only for key-based operations.");
    }

    /**
     * Background UDF does not address individual keys in a result stream.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public BackgroundUdfBuilder includeMissingKeys() {
        throw new UnsupportedOperationException(
            "includeMissingKeys() is not applicable to background operations. " +
            "This method is only for key-based operations.");
    }

    /**
     * Execute the background UDF on the server.
     *
     * <p>This method submits the UDF to the server for asynchronous execution against
     * all records in the set (optionally filtered by a where clause) and immediately
     * returns an {@link ExecuteTask} that can be used to monitor progress and completion.</p>
     *
     * @return ExecuteTask for monitoring the background UDF operation
     * @throws AerospikeException if the operation fails to start
     */
    public ExecuteTask execute() {
        Cluster cluster = session.getCluster();
        Node[] nodes = cluster.validateNodes();

        cluster.addCommandCount();

        Settings settings = session.getBehavior().getSettings(
            OpKind.WRITE_NON_RETRYABLE,
            OpShape.QUERY,
            Mode.ANY
        );

        Filter filter = null;
        Expression filterExp = null;

        if (ael != null) {
            ParseResult pr = ael.process(dataset.getNamespace(), session);
            filter = pr.getFilter();
            filterExp = Exp.build(pr.getExp());
        }

        int ttl = getExpirationAsInt();
        long taskId = new Random().nextLong();

        BackgroundQueryCommand cmd = new BackgroundQueryCommand(cluster, dataset, taskId, opType,
            ttl, packageName, functionName, functionArgs, filter, filterExp, settings, recordsPerSecond);

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
}
