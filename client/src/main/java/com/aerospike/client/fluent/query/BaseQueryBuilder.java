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

import com.aerospike.client.fluent.ErrorHandler;
import com.aerospike.client.fluent.ErrorStrategy;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;

/**
 * Base interface for all query builders with common methods.
 * Uses self-referencing generics to maintain fluent API.
 */
public interface BaseQueryBuilder<T extends BaseQueryBuilder<T>> {

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
    QueryBuilderBinBuilder bin(String binName);

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
     * Adds a filter condition using an Expression.
     *
     * <p>Only one filter condition can be specified per query. Multiple calls
     * to this method or {@link #where(String)} will throw an exception.</p>
     *
     * @param dsl filter expression
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(Expression dsl);

    /**
     * Adds a filter condition using an Exp instance.
     *
     * <p>Only one filter condition can be specified per query. Multiple calls
     * to this method or {@link #where(String)} will throw an exception.</p>
     *
     * @param dsl filter expression
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(Exp dsl);

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
     * Execute the query synchronously with errors embedded in the stream.
     *
     * @param strategy the error strategy (must not be null)
     * @return a RecordStream containing the query results (including error results)
     */
    RecordStream execute(ErrorStrategy strategy);

    /**
     * Execute the query synchronously with errors dispatched to the handler.
     * Error results are excluded from the returned stream.
     *
     * @param handler the error handler callback (must not be null)
     * @return RecordStream containing only successful results
     */
    RecordStream execute(ErrorHandler handler);

    /**
     * Execute the query asynchronously with errors embedded in the stream.
     *
     * @param strategy the error strategy (must not be null)
     * @return RecordStream that will be populated as results arrive
     */
    RecordStream executeAsync(ErrorStrategy strategy);

    /**
     * Execute the query asynchronously with errors dispatched to the handler.
     * Error results are excluded from the returned stream.
     *
     * @param handler the error handler callback (must not be null)
     * @return RecordStream containing only successful results
     */
    RecordStream executeAsync(ErrorHandler handler);
}
