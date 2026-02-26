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

import com.aerospike.client.fluent.AbstractFilterableBuilder;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.NavigatableRecordStream;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.QueryDuration;
import com.aerospike.client.fluent.tend.Partition;

/**
 * Builder class for constructing and executing dataset queries (scans and secondary index queries)
 * against Aerospike.
 *
 * <p>This class provides a fluent API for building complex queries with support for
 * filtering, sorting, pagination, and partition targeting.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Query entire dataset with filtering
 * RecordStream results = session.query(customerDataSet)
 *     .where("$.name == 'Tim' and $.age > 30")
 *     .sortReturnedSubsetBy("age", SortDir.SORT_DESC)
 *     .limit(100)
 *     .pageSize(20)
 *     .execute();
 *
 * // Query specific keys
 * RecordStream results = session.query(customerDataSet.ids(1, 2, 3))
 *     .readingOnlyBins("name", "age")
 *     .execute();
 *
 * // Query with partition targeting
 * RecordStream results = session.query(customerDataSet)
 *     .onPartitionRange(0, 2048)
 *     .limit(1000)
 *     .execute();
 * }</pre>
 *
 * @see Session#query(DataSet)
 * @see RecordStream
 * @see SortDir
 * @see SortProperties
 */
public class QueryBuilder extends AbstractFilterableBuilder implements
            KeyBasedQueryBuilderInterface<QueryBuilder>,
            IndexBasedQueryBuilderInterface<QueryBuilder> {
    private final QueryImpl implementation;
    private String[] binNames = null;
    private boolean withNoBins = false;
    private long limit = 0;
    private int chunkSize = 0;
    private int startPartition = 0;
    private int endPartition = 4096;
    private Txn txnToUse;
    private int recordsPerSecond = 0;
    private QueryDuration expectedQueryDuration = null;
    private java.util.List<com.aerospike.client.fluent.Operation> operations = null;

    /**
     * Creates a QueryBuilder for querying an entire dataset.
     *
     * <p>This constructor creates a query that will scan the entire dataset or use
     * secondary indexes if available. The query can be filtered, sorted, and paginated.</p>
     *
     * @param session the session to use for the query
     * @param dataSet the dataset to query
     */
    public QueryBuilder(Session session, DataSet dataSet) {
        this.implementation = new IndexQueryBuilderImpl(this, session, dataSet);
        this.txnToUse = session.getCurrentTransaction();
    }


    /**
     * Checks if a key falls within the current partition range.
     *
     * <p>This method is used internally to filter keys based on the partition
     * range specified in the query. If no partition range is set (default 0-4096),
     * all keys are considered valid.</p>
     *
     * @param key the key to check
     * @return true if the key is in the partition range, false otherwise
     */
    protected boolean isKeyInPartitionRange(Key key) {
        if (startPartition <= 0 && endPartition >= 4096) {
            return true;
        }
        int partId = Partition.getPartitionId(key.digest);
        return partId >= startPartition && partId < endPartition;
    }

    /**
     * Returns a bin builder for read operations on a specific bin.
     * 
     * <p>Use this method to read bin values or compute expression-based values.
     * Unlike {@link #readingOnlyBins(String...)}, this method allows you to add
     * expression operations like {@code selectFrom()}.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("name").get()
     *     .bin("ageIn20Years").selectFrom("$.age + 20")
     *     .execute();
     * }</pre>
     *
     * @param binName the name of the bin
     * @return QueryBuilderBinBuilder for constructing bin operations
     */
    public QueryBuilderBinBuilder bin(String binName) {
        return new QueryBuilderBinBuilder(this, binName);
    }

    /**
     * Package-private method to add an operation.
     * Used by QueryBuilderBinBuilder.
     */
    void addOperation(com.aerospike.client.fluent.Operation op) {
        if (this.operations == null) {
            this.operations = new java.util.ArrayList<>();
        }
        this.operations.add(op);
    }

    /**
     * Get the list of operations (may be null).
     */
    public java.util.List<com.aerospike.client.fluent.Operation> getOperations() {
        return this.operations;
    }

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
    public QueryBuilder readingOnlyBins(String ... binNames) {
        this.binNames = binNames;
        if (this.withNoBins) {
            throw new IllegalArgumentException("Cannot specify both 'withNoBins' and provide a list of bin names");
        }
        return this;
    }

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
    public QueryBuilder withNoBins() {
        this.withNoBins = true;
        if (this.binNames != null) {
            throw new IllegalArgumentException("Cannot specify both 'withNoBins' and provide a list of bin names");
        }
        return this;
    }

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
    public QueryBuilder limit(long limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be > 0, not " + limit);
        }
        this.limit = limit;
        return this;
    }

    /**
     * Sets the chunk size for server-side streaming.
     *
     * <p>This method controls how many records are fetched per chunk from the server
     * when using server-side streaming. The chunk size affects memory usage and network
     * round trips. This is distinct from client-side pagination provided by
     * {@link NavigatableRecordStream}.</p>
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
    public QueryBuilder chunkSize(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be > 0, not " + chunkSize);
        }
        this.chunkSize = chunkSize;
        return this;
    }

    /**
     * Validates partition range parameters.
     *
     * <p>This method performs sanity checks on partition range parameters to ensure
     * they are valid and consistent.</p>
     *
     * @param startIncl the start partition (inclusive)
     * @param endExcl the end partition (exclusive)
     * @throws IllegalArgumentException if the partition range is invalid
     */
    private void sanityCheckPartitionRange(int startIncl, int endExcl) {
        if ((this.startPartition != 0 || this.endPartition != 4096) &&
                (this.startPartition != startIncl || this.endPartition != endExcl)) {

            throw new IllegalArgumentException(String.format(
                    "Partition range can only be defined once for a query. The partition range is currently "
                    + "[%,d->%,d) and is being set to [%,d->%,d)",
                    startPartition, endPartition, startIncl, endExcl));
        }
        if (startIncl < 0 || startIncl >= 4096) {
            throw new IllegalArgumentException("Start partition must in the range of 0 to 4095, not " + startIncl);
        }
        if (endExcl < 1 || startIncl > 4096) {
            throw new IllegalArgumentException("End partition (exclusive) must in the range of 1 to 4096, not " + startIncl);
        }
        if (startIncl >= endExcl) {
            throw new IllegalArgumentException(String.format(
                    "Start partition must be less than the end partition. Specified start partition is %,d and end partition is %,d",
                    startIncl, endExcl));
        }
    }

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
    public QueryBuilder onPartition(int partId) {
        return onPartitionRange(partId, partId+1);
    }

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
    public QueryBuilder onPartitionRange(int startIncl, int endExcl) {
        sanityCheckPartitionRange(startIncl, endExcl);
        this.startPartition = startIncl;
        this.endPartition = endExcl;
        return this;
    }

    /**
     * If the query has a `where` clause and is provided either a single key or a list of keys,
     * any records which are filtered out will appear in the
     * stream against an exception code of {@link ResultCode.FILTERED_OUT} rather than just not
     * appearing in the result stream.
     * @return this QueryBuilder for method chaining
     */
    public QueryBuilder failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    protected boolean isFailOnFilteredOut() {
        return this.failOnFilteredOut;
    }

    /**
     * By default, if a key is provided (or is part of a list of keys) but the key does not map to a record
     * then nothing will be returned in the stream against that key. However, if this flag is specified, {@code null} will be
     * in the stream again that key.
     * @return this QueryBuilder for method chaining
     */
    public QueryBuilder respondAllKeys() {
        this.respondAllKeys = true;
        return this;
    }

    protected boolean isRespondAllKeys() {
        return this.respondAllKeys;
    }

    /**
     * Rate limit the records per second returned from the server. Note that this will force
     * this to be a "long" query, allowing it to be tracked on the server.
     *
     * @return this QueryBuilder for method chaining
     */
    public QueryBuilder recordsPerSecond(int recordsPerSecond) {
        this.recordsPerSecond = recordsPerSecond;
        return this;
    }

    public int getRecordsPerSecond() {
        return this.recordsPerSecond;
    }

    /**
     * Sets the expected query duration. The server optimizes query handling
     * based on this hint.
     * 
     * @param duration the expected duration (LONG, SHORT, or LONG_RELAX_AP)
     * @return this QueryBuilder for method chaining
     */
    public QueryBuilder expectedQueryDuration(QueryDuration duration) {
        this.expectedQueryDuration = duration;
        return this;
    }

    public QueryDuration getExpectedQueryDuration() {
        return this.expectedQueryDuration;
    }

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
    public QueryBuilder where(String dsl, Object ... params) {
        setWhereClause(createWhereClauseProcessor(this.implementation.allowsSecondaryIndexQuery(), dsl, params));
        return this;
    }

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
    public QueryBuilder where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

	/**
	 * Add a filter condition using an Expression.
	 *
	 * <p>Note: If this method is used, no secondary index can be used. </p>
	 *
	 * <p>Only one filter condition can be specified per query. Multiple calls
	 * to this method or {@link #where(String)} will throw an exception.</p>
	 *
	 * @param expression filter expression
	 * @return
	 */
    public QueryBuilder where(Expression expression) {
        setWhereClause(WhereClauseProcessor.from(expression));
        return this;
    }

    /**
     * Add a filter condition using a Exp operation.
     *
     * <p>Note: This method may be deprecated in the future -- use a string version instead.</p>
     * <p>Note: If this method is used, no secondary index can be used. </p>
     *
     * <p>Only one filter condition can be specified per query. Multiple calls
     * to this method or {@link #where(String)} will throw an exception.</p>
     *
     * @param exp - The expression to validate the records against.
     * @return
     */
    public QueryBuilder where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    public QueryBuilder where(PreparedDsl dsl, Object ... params) {
        setWhereClause(WhereClauseProcessor.from(this.implementation.allowsSecondaryIndexQuery(), dsl, params));
        return this;
    }

    /**
     * Gets the bin names to read.
     *
     * @return the array of bin names, or null if not specified
     */
    public String[] getBinNames() {
        return this.binNames;
    }

    /**
     * Checks if the query should read no bins (header-only).
     *
     * @return true if no bins should be read, false otherwise
     */
    public boolean getWithNoBins() {
        return this.withNoBins;
    }

    /**
     * Gets the query limit.
     *
     * @return the maximum number of records to return, or 0 if not set
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Gets the chunk size for server-side streaming.
     *
     * @return the chunk size, or 0 if not set
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Gets the start partition.
     *
     * @return the start partition (inclusive)
     */
    public int getStartPartition() {
        return startPartition;
    }

    /**
     * Gets the end partition.
     *
     * @return the end partition (exclusive)
     */
    public int getEndPartition() {
        return endPartition;
    }

    /**
     * Specifies that these operations are not to be included in any transaction, even if a
     * transaction exists on the underlying session.
     *
     * <p>This method explicitly excludes the query from any active transaction,
     * ensuring it runs as a standalone operation.</p>
     *
     * @return this QueryBuilder for method chaining
     */
    public QueryBuilder notInAnyTransaction() {
        this.txnToUse = null;
        return this;
    }

    /**
     * Specify the transaction to use for this call. Note that this should not be commonly used.
     * A better pattern is to use the {@code doInTransaction} method on {@link Session}:
     * <pre>
     * session.doInTransaction(txnSession -> {
     *     Optional<KeyRecord> result = txnSession.query(customerDataSet.id(1)).execute().getFirst();
     *     // Do stuff...
     *     txnSession.insert(customerDataSet.id(3));
     *     txnSession.delete(customerDataSet.id(3));
     * });
     * </pre>
     *
     * This method should only be used in situations where different parts of a transaction are not all
     * within the same context, for example forming a transaction on callbacks from a file system.
     * @param txn - the transaction to use
     */
    public QueryBuilder inTransaction(Txn txn) {
        this.txnToUse = txn;
        return this;
    }

    /**
     * Gets the transaction to use for this query.
     *
     * @return the transaction, or null if no transaction should be used
     */
    protected Txn getTxnToUse() {
        return this.txnToUse;
    }

    /**
     * Executes the query and returns a RecordStream.
     *
     * <p>This method executes the query with all the configured parameters and
     * returns a RecordStream that can be used to iterate through the results.</p>
     *
     * <p>The RecordStream provides methods for:</p>
     * <ul>
     *   <li>Iterating through results: {@link RecordStream#hasNext()}, {@link RecordStream#next()}</li>
     *   <li>Server-side chunking: {@link RecordStream#hasMoreChunks()}</li>
     *   <li>Client-side sorting/pagination: {@link RecordStream#asNavigatableStream()}</li>
     *   <li>Object conversion: {@link RecordStream#toObjectList(RecordMapper)}</li>
     * </ul>
     *
     * @return a RecordStream containing the query results
     * @see RecordStream
     */
    @Override
    public RecordStream execute() {
        // Default: async unless in transaction
        if (txnToUse != null) {
            return executeSync();
        } else {
            return executeAsync();
        }
    }

    /**
     * Execute the query synchronously. All operations complete before this method returns.
     * <p>
     * Use this when you need guaranteed completion before proceeding, or when in a transaction.
     * Operations are still parallelized internally using virtual threads, but all threads
     * are joined before returning.
     *
     * @return RecordStream containing the results
     */
    @Override
    public RecordStream executeSync() {
        if (Log.debugEnabled()) {
            Log.debug("QueryBuilder.executeSync() called, transaction: " + (txnToUse != null ? "yes" : "no"));
        }
        return this.implementation.executeSync();
    }

    /**
     * Execute the query asynchronously using virtual threads for parallel execution.
     * Results are streamed as they become available.
     * <p>
     * <b>WARNING:</b> Using this in transactions may lead to operations still being in flight
     * when commit() is called, potentially leading to inconsistent state. A warning will be logged.
     *
     * @return RecordStream that will be populated as results arrive
     */
    @Override
    public RecordStream executeAsync() {
        if (Log.debugEnabled()) {
            Log.debug("QueryBuilder.executeAsync() called, transaction: " + (txnToUse != null ? "yes" : "no"));
        }

        if (txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using executeSync() or execute() for transactional safety."
            );
        }
        return this.implementation.executeAsync();
    }

    protected WhereClauseProcessor getDsl() {
        return this.dsl;
    }
}
