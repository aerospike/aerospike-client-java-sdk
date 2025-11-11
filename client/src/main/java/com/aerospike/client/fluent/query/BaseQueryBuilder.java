package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.dsl.BooleanExpression;

/**
 * Base interface for all query builders with common methods.
 * Uses self-referencing generics to maintain fluent API.
 */
public interface BaseQueryBuilder<T extends BaseQueryBuilder<T>> {
    
    /**
     * Specifies which bins to read from the records.
     * 
     * <p>This method allows you to optimize query performance by only reading
     * the bins you need. If not specified, all bins will be read.</p>
     * 
     * <p>This method cannot be used together with {@link #withNoBins()}.</p>
     * 
     * @param binNames the names of the bins to read
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if used together with withNoBins()
     */
    T readingOnlyBins(String ... binNames);
    
    /**
     * Specifies that no bins should be read (header-only query).
     * 
     * <p>This method is useful when you only need to check for record existence
     * or get metadata like generation numbers, without reading the actual data.</p>
     * 
     * <p>This method cannot be used together with {@link #readingOnlyBins(String...)}.</p>
     * 
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if used together with readingOnlyBins()
     */
    T withNoBins();
    
    /**
     * Sets the maximum number of records to return.
     * 
     * <p>This method limits the total number of records returned by the query.
     * Once the limit is reached, the query will stop processing.</p>
     * 
     * @param limit the maximum number of records to return (must be > 0)
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if limit is <= 0
     */
    T limit(long limit);
    
    /**
     * Sets the chunk size for server-side streaming.
     * 
     * <p>This method controls how many records are fetched per chunk from the server
     * when using server-side streaming. The chunk size affects memory usage and network
     * round trips. This is distinct from client-side pagination provided by
     * {@link com.aerospike.NavigatableRecordStream}.</p>
     * 
     * <p><b>Use cases:</b></p>
     * <ul>
     *   <li>Smaller chunks (e.g., 100-1000): Lower memory, more network calls</li>
     *   <li>Larger chunks (e.g., 5000-10000): Higher throughput, more memory</li>
     * </ul>
     * 
     * @param chunkSize the number of records per chunk (must be > 0)
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if chunkSize is <= 0
     */
    T chunkSize(int chunkSize);
    
    /**
     * Targets a specific partition for the query.
     * 
     * <p>This method restricts the query to a single partition. This can be useful
     * for load balancing or when you know the data distribution across partitions.</p>
     * 
     * @param partId the partition ID to target (0-4095)
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if partId is out of range
     */
    T onPartition(int partId);
    
    /**
     * Targets a range of partitions for the query.
     * 
     * <p>This method restricts the query to a specific range of partitions. This
     * can be useful for load balancing, parallel processing, or when you know
     * the data distribution across partitions.</p>
     * 
     * <p>The partition range can only be set once per query. Subsequent calls
     * with different ranges will throw an exception.</p>
     * 
     * @param startIncl the start partition (inclusive, 0-4095)
     * @param endExcl the end partition (exclusive, 1-4096)
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if the partition range is invalid or already set
     */
    T onPartitionRange(int startIncl, int endExcl);
    
    /**
     * Adds a filter condition using a DSL string.
     * 
     * <p>This method allows you to specify a filter condition using Aerospike's
     * Domain Specific Language (DSL). The DSL provides a SQL-like syntax for
     * expressing complex filter conditions.</p>
     * 
     * <p>Example DSL expressions:</p>
     * <ul>
     *   <li><code>"$.name == 'Tim'"</code> - exact string match</li>
     *   <li><code>"$.age > 30"</code> - numeric comparison</li>
     *   <li><code>"$.name == 'Tim' and $.age > 30"</code> - logical AND</li>
     *   <li><code>"$.name == 'Tim' or $.name == 'Jane'"</code> - logical OR</li>
     * </ul>
     * 
     * <p>Only one filter condition can be specified per query. Multiple calls
     * to this method or {@link #where(BooleanExpression)} will throw an exception.</p>
     * 
     * @param dsl the DSL filter expression
     * @param params The params used to replace arguments in the DSL string (used by {@code String.format(dsl, params)}
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(String dsl, Object ... params);
    
    /**
     * Adds a filter condition using a BooleanExpression.
     * 
     * <p>This method allows you to specify a filter condition using the programmatic
     * BooleanExpression API. This provides type safety and compile-time checking
     * compared to DSL strings.</p>
     * 
     * <p>Example usage:</p>
     * <pre>{@code
     * BooleanExpression filter = Dsl.stringBin("name").eq("Tim")
     *     .and(Dsl.longBin("age").gt(30));
     * 
     * RecordStream results = session.query(customerDataSet)
     *     .where(filter)
     *     .execute();
     * }</pre>
     * 
     * <p>Only one filter condition can be specified per query. Multiple calls
     * to this method or {@link #where(String)} will throw an exception.</p>
     * 
     * @param dsl the BooleanExpression filter
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(BooleanExpression dsl);
    
    /**
     * Specifies that these operations are not to be included in any transaction, even if a
     * transaction exists on the underlying session.
     * 
     * <p>This method explicitly excludes the query from any active transaction,
     * ensuring it runs as a standalone operation.</p>
     * 
     * @return this QueryBuilder for method chaining
     */
    T notInAnyTransaction();
    
    /**
     * Specify the transaction to use for this call. Note that this should not be commonly used.
     * A better pattern is to use the {@code doInTransaction} method on {@link Session}:
     * <pre>
     * session.doInTransaction(txnSession -> {
     *     Optional<KeyRecord> result = txnSession.query(customerDataSet.id(1)).execute().getFirst();
     *     // Do stuff...
     *     txnSession.insertInto(customerDataSet.id(3));
     *     txnSession.delete(customerDataSet.id(3));
     * });
     * </pre> 
     * 
     * This method should only be used in situations where different parts of a transaction are not all
     * within the same context, for example forming a transaction on callbacks from a file system. 
     * @param txn - the transaction to use
     */
    T inTransaction(Txn txn);
    
    /**
     * Executes the query with default behavior:
     * <ul>
     *   <li>For queries: async execution (unless in transaction)</li>
     *   <li>In transactions: always synchronous</li>
     * </ul>
     * 
     * <p>The RecordStream provides methods for:</p>
     * <ul>
     *   <li>Iterating through results: {@link RecordStream#hasNext()}, {@link RecordStream#next()}</li>
     *   <li>Pagination: {@link RecordStream#hasMorePages()}</li>
     *   <li>Sorting: {@link RecordStream#asSortable()}</li>
     *   <li>Object conversion: {@link RecordStream#toObjectList(RecordMapper)}</li>
     * </ul>
     * 
     * @return a RecordStream containing the query results
     * @see RecordStream
     */
    RecordStream execute();
    
    /**
     * Execute the query synchronously. All operations complete before this method returns.
     * <p>
     * Use this when you need guaranteed completion before proceeding, or when in a transaction.
     * Operations are still parallelized internally using virtual threads, but all threads
     * are joined before returning.
     * 
     * @return RecordStream containing the results
     */
    RecordStream executeSync();
    
    /**
     * Execute the query asynchronously using virtual threads for parallel execution.
     * Results are streamed as they become available.
     * <p>
     * <b>WARNING:</b> Using this in transactions may lead to operations still being in flight
     * when commit() is called, potentially leading to inconsistent state. A warning will be logged.
     * 
     * @return RecordStream that will be populated as results arrive
     */
    RecordStream executeAsync();
}
