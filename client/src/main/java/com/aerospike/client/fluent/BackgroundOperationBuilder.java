package com.aerospike.client.fluent;

import com.aerospike.client.fluent.command.Statement;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.policy.WritePolicy;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;
import com.aerospike.client.fluent.task.ExecuteTask;

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
        // Build Statement for the background operation
        Statement stmt = new Statement();
        stmt.setNamespace(dataset.getNamespace());
        stmt.setSetName(dataset.getSet());
        
        // Add filter expression if where clause is present
        Expression filterExp = processWhereClause(dataset.getNamespace(), session);
        
        // Get WritePolicy from settings based on operation retryability
        Operation[] operations = ops.toArray(new Operation[0]);
        boolean retryable = areOperationsRetryable(operations);
        Settings settings = session.getBehavior().getSettings(
            retryable ? OpKind.WRITE_RETRYABLE : OpKind.WRITE_NON_RETRYABLE,
            OpShape.QUERY,
            session.isNamespaceSC(dataset.getNamespace())
        );
        
        WritePolicy wp = settingsToWritePolicy(settings, filterExp);
        
        // Execute background operation and return task
        // TODO: BN - CLIENT-3911
//        return session.getClient().execute(wp, stmt, operations);
        return null;
    }
    
    /**
     * Converts settings and filter expression to a WritePolicy.
     * 
     * @param settings The behavior settings to apply
     * @param filterExp The optional filter expression
     * @return WritePolicy configured for the background operation
     */
    // TODO: BN - CLIENT-3911
    private WritePolicy settingsToWritePolicy(Settings settings, Expression filterExp) {
        WritePolicy wp = new WritePolicy(settings.asWritePolicy());
        wp.filterExp = filterExp;
//        wp.recordExistsAction = recordExistsActionFromOpType(opType);
        wp.expiration = getExpirationAsInt();
        return wp;
    }
    
    /**
     * Checks if the given operations are retryable.
     * Delegates to OperationBuilder's implementation.
     * 
     * @param operations The operations to check
     * @return true if all operations are retryable, false otherwise
     */
    private static boolean areOperationsRetryable(Operation[] operations) {
        return OperationBuilder.areOperationsRetryable(operations);
    }
}

