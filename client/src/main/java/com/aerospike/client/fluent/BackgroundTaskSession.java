package com.aerospike.client.fluent;

/**
 * Context for performing server-side background operations on entire sets.
 * Obtained via {@link Session#backgroundTask()}.
 *
 * <p>Background operations run asynchronously on the server and return
 * an {@link com.aerospike.client.fluent.task.ExecuteTask} for monitoring completion.
 * They operate on entire sets and cannot be part of transactions.</p>
 *
 * <p><b>Supported Operations:</b></p>
 * <ul>
 *   <li>UPDATE - Update existing records only</li>
 *   <li>DELETE - Delete existing records</li>
 *   <li>TOUCH - Touch existing records (update TTL)</li>
 *   <li>UDF - Execute a registered Lua function against matching records</li>
 * </ul>
 *
 * <p><b>Limitations:</b></p>
 * <ul>
 *   <li>Cannot be used within transactions</li>
 *   <li>Operates on entire sets (no specific keys)</li>
 *   <li>Does not return record data, only completion status</li>
 * </ul>
 *
 * <p><b>Examples:</b></p>
 * <pre>{@code
 * // Bulk update
 * ExecuteTask task = session.backgroundTask()
 *     .update(customerDataSet)
 *     .where("$.age > 30")
 *     .bin("category").setTo("senior")
 *     .expireRecordAfter(Duration.ofDays(90))
 *     .execute();
 *
 * // Background UDF
 * ExecuteTask udfTask = session.backgroundTask()
 *     .executeUdf(products)
 *     .function("inventory", "applyDiscount")
 *     .passing(0.20)
 *     .where("$.stock > 250")
 *     .execute();
 *
 * // Monitor progress
 * task.waitTillComplete();
 * }</pre>
 *
 * @see Session#backgroundTask()
 * @see BackgroundOperationBuilder
 * @see BackgroundUdfFunctionBuilder
 */
public class BackgroundTaskSession {
    private final Session session;
    
    /**
     * Package-private constructor. Use {@link Session#backgroundTask()} to obtain instances.
     * 
     * @param session The session to use for background operations
     */
    BackgroundTaskSession(Session session) {
        this.session = session;
    }
    
    /**
     * Create background update operation for a dataset.
     * Updates only existing records that match the optional where clause.
     * 
     * <p>The UPDATE operation will only modify records that already exist in the database.
     * Records are selected based on the optional where clause filter. Use this for
     * bulk updates of existing data.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>{@code
     * ExecuteTask task = session.backgroundTask()
     *     .update(customerDataSet)
     *     .where("$.age > 30")
     *     .bin("category").setTo("senior")
     *     .execute();
     * }</pre>
     * 
     * @param dataset The dataset (namespace + set) to operate on
     * @return BackgroundOperationBuilder for building the update operation
     * @throws IllegalStateException if called within a transaction
     */
    public BackgroundOperationBuilder update(DataSet dataset) {
        return new BackgroundOperationBuilder(session, dataset, OpType.UPDATE);
    }
    
    /**
     * Create background delete operation for a dataset.
     * Deletes only existing records that match the optional where clause.
     * 
     * <p>The DELETE operation will remove records that match the optional filter.
     * Use this for bulk cleanup of unwanted or expired data.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>{@code
     * ExecuteTask task = session.backgroundTask()
     *     .delete(customerDataSet)
     *     .where("$.lastLogin < 1609459200000")
     *     .execute();
     * }</pre>
     * 
     * @param dataset The dataset (namespace + set) to operate on
     * @return BackgroundOperationBuilder for building the delete operation
     * @throws IllegalStateException if called within a transaction
     */
    public BackgroundOperationBuilder delete(DataSet dataset) {
        return new BackgroundOperationBuilder(session, dataset, OpType.DELETE);
    }
    
    /**
     * Create background touch operation for a dataset.
     * Touches only existing records that match the optional where clause.
     * 
     * <p>The TOUCH operation updates the record metadata (like TTL) without
     * modifying the record data itself. Use this to extend expiration times
     * for records that are still relevant.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>{@code
     * ExecuteTask task = session.backgroundTask()
     *     .touch(activeUsers)
     *     .where("$.status == 'active'")
     *     .expireRecordAfter(Duration.ofDays(30))
     *     .execute();
     * }</pre>
     * 
     * @param dataset The dataset (namespace + set) to operate on
     * @return BackgroundOperationBuilder for building the touch operation
     * @throws IllegalStateException if called within a transaction
     */
    public BackgroundOperationBuilder touch(DataSet dataset) {
        return new BackgroundOperationBuilder(session, dataset, OpType.TOUCH);
    }

    /**
     * Create a background UDF operation for a dataset.
     * Executes a registered Lua function against all records in the set
     * that match an optional where clause.
     *
     * <p>Background UDFs run server-side and are useful for bulk transformations
     * that need custom logic beyond simple bin updates. The UDF package must be
     * registered on the server before use.</p>
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
     * @param dataset The dataset (namespace + set) to operate on
     * @return BackgroundUdfFunctionBuilder requiring the UDF function to be specified
     * @throws IllegalStateException if called within a transaction
     * @see BackgroundUdfFunctionBuilder#function(String, String)
     * @see BackgroundUdfBuilder
     */
    public BackgroundUdfFunctionBuilder executeUdf(DataSet dataset) {
        return new BackgroundUdfFunctionBuilder(session, dataset);
    }
}

