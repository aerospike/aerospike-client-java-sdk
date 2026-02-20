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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.command.Info;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.info.InfoCommands;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.query.IndexBasedQueryBuilderInterface;
import com.aerospike.client.fluent.query.IndexCollectionType;
import com.aerospike.client.fluent.query.KeyBasedQueryBuilderInterface;
import com.aerospike.client.fluent.query.QueryBuilder;
import com.aerospike.client.fluent.task.IndexTask;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.client.fluent.util.Crypto;
import com.aerospike.client.fluent.util.Pack;
import com.aerospike.client.fluent.util.Version;

public class Session {
    private final Cluster cluster;
    private final Behavior behavior;

    protected Session(Cluster cluster, Behavior behavior) {
        this.cluster = cluster;
        this.behavior = behavior;
    }

    /**
     * Builder class for constructing filter expressions used in queries and operations.
     *
     * <p>This class provides a way to build filter expressions from either an {@link Exp}
     * expression builder or a pre-built {@link Expression} object. Filter expressions
     * are used to filter records in queries and background operations.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * ExpressionBuilder filter = new ExpressionBuilder(
     *     Exp.build(Exp.eq(Exp.stringBin("status"), Exp.val("active")))
     * );
     * }</pre>
     */
    public class ExpressionBuilder {
        private Expression filterExpression = null;

        /**
         * Creates an ExpressionBuilder from an Exp expression.
         *
         * @param exp the Exp expression to build into a filter expression
         */
        public ExpressionBuilder(Exp exp) {
            this.filterExpression = Exp.build(exp);
        }

        /**
         * Creates an ExpressionBuilder from a pre-built Expression.
         *
         * @param exp the Expression to use as the filter expression
         */
        public ExpressionBuilder(Expression exp) {
            this.filterExpression = exp;
        }

        /**
         * Gets the filter expression built by this builder.
         *
         * @return the filter expression
         */
        public Expression getFilterExpression() {
            return filterExpression;
        }
    }

    /**
     * Gets the behavior configuration for this session.
     *
     * <p>The behavior configuration controls how operations are performed, including
     * timeouts, retry policies, consistency levels, and other operational settings.</p>
     *
     * @return the behavior configuration for this session
     * @see Behavior
     */
    public Behavior getBehavior() {
        return this.behavior;
    }

    /**
     * Gets the cluster associated with this session.
     *
     * <p>The cluster represents the connection to the Aerospike cluster and provides
     * access to cluster-level operations and configuration.</p>
     *
     * @return the cluster associated with this session
     * @see Cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * Remove records in specified namespace/set efficiently.  This method is many orders of magnitude
     * faster than deleting records one at a time.
     * <p>
     * See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
     * <p>
     * This asynchronous server call may return before the truncation is complete.  The user can still
     * write new records after the server call returns because new records will have last update times
     * greater than the truncate cutoff (set at the time of truncate call).
     */
    public void truncate(DataSet set) {
        truncate(set, null);
    }
    /**
     * Remove records in specified namespace/set that were last updated before the specified time. This method is many orders of magnitude
     * faster than deleting records one at a time.
     * <p>
     * See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
     * <p>
     * This asynchronous server call may return before the truncation is complete.  The user can still
     * write new records after the server call returns because new records will have last update times
     * greater than the truncate cutoff (set at the time of truncate call).
     */
    public void truncate(DataSet set, Calendar beforeLastUpdate) {
		// Send truncate command to one node. That node will distribute the command to other nodes.
		StringBuilder sb = new StringBuilder(200);

		if (set.getSet() != null) {
			sb.append("truncate:namespace=");
			sb.append(set.getNamespace());
			sb.append(";set=");
			sb.append(set.getSet());
		}
		else {
			sb.append("truncate-namespace:namespace=");
			sb.append(set.getNamespace());
		}

		if (beforeLastUpdate != null) {
			sb.append(";lut=");
			// Convert to nanoseconds since unix epoch (1970-01-01)
			sb.append(beforeLastUpdate.getTimeInMillis() * 1000000L);
		}

		Node node = cluster.getRandomNode();

		String response = Info.request(node, sb.toString());

		if (! response.equalsIgnoreCase("ok")) {
			throw new AerospikeException("Truncate failed: " + response);
		}
    }

    /**
     * Gets the record mapping factory associated with this session's cluster.
     *
     * <p>The record mapping factory provides mappers that convert between Aerospike
     * records and Java objects. This enables automatic object serialization and
     * deserialization when working with typed datasets.</p>
     *
     * <p>If no factory is set, this method will return null, and object mapping
     * operations will not be available.</p>
     *
     * @return the record mapping factory, or null if none is set
     * @see RecordMappingFactory
     * @see DefaultRecordMappingFactory
     * @see Cluster#setRecordMappingFactory(RecordMappingFactory)
     */
    public RecordMappingFactory getRecordMappingFactory() {
        return this.cluster.getRecordMappingFactory();
    }

    private List<Key> buildKeyList(Key key1, Key key2, Key ...keys) {
        List<Key> keyList = new ArrayList<>();
        keyList.add(key1);
        keyList.add(key2);
        for (Key thisKey : keys) {
            keyList.add(thisKey);
        }
        return keyList;
    }

    // --------------------------------------------
    // Query functionality
    // --------------------------------------------

    /**
     * Creates a query builder for querying an entire dataset.
     *
     * <p>This method creates a query that will scan the entire dataset or use
     * secondary indexes if available. The query can be filtered, sorted, and paginated.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * RecordStream results = session.query(customerDataSet)
     *     .where("$.age > 30")
     *     .sortReturnedSubsetBy("age", SortDir.SORT_DESC)
     *     .limit(100)
     *     .execute();
     * }</pre>
     *
     * @param dataSet the dataset to query
     * @return a query builder for configuring and executing the query
     * @see QueryBuilder
     * @see IndexBasedQueryBuilderInterface
     */
    public IndexBasedQueryBuilderInterface<QueryBuilder> query(DataSet dataSet) {
        return new QueryBuilder(this, dataSet);
    }

    /**
     * Creates a query builder for querying a single key.
     *
     * <p>This method creates a query that will perform a direct key lookup.
     * The query will return at most one record.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * RecordStream results = session.query(users.id("user-123"))
     *     .readingOnlyBins("name", "email")
     *     .execute();
     * }</pre>
     *
     * @param key the key to query
     * @return a query builder for configuring and executing the query
     * @see QueryBuilder
     * @see KeyBasedQueryBuilderInterface
     */
    public KeyBasedQueryBuilderInterface<QueryBuilder> query(Key key) {
        return new QueryBuilder(this, key);
    }

    /**
     * Creates a query builder for querying multiple keys using varargs.
     *
     * <p>This method creates a batch query that will perform lookups for multiple keys.
     * If only one key is provided, it will use single key optimization.</p>
     *
     * <p>This overload is provided to differentiate from the single-key query method
     * when querying with no parameters is valid.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * RecordStream results = session.query(
     *     users.id("user-1"),
     *     users.id("user-2"),
     *     users.id("user-3")
     * ).execute();
     * }</pre>
     *
     * @param key1 the first key to query
     * @param key2 the second key to query
     * @param keys additional keys to query (varargs)
     * @return a query builder for configuring and executing the batch query
     * @see QueryBuilder
     * @see KeyBasedQueryBuilderInterface
     */
    public KeyBasedQueryBuilderInterface<QueryBuilder> query(Key key1, Key key2, Key...keys) {
        return new QueryBuilder(this, buildKeyList(key1, key2, keys));
    }

    /**
     * Creates a query builder for querying multiple keys from a list.
     *
     * <p>This method creates a batch query that will perform lookups for multiple keys.
     * If only one key is provided in the list, it will use single key optimization.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * List<Key> keys = Arrays.asList(
     *     users.id("user-1"),
     *     users.id("user-2"),
     *     users.id("user-3")
     * );
     * RecordStream results = session.query(keys)
     *     .readingOnlyBins("name", "email")
     *     .execute();
     * }</pre>
     *
     * @param keyList the list of keys to query
     * @return a query builder for configuring and executing the batch query
     * @see QueryBuilder
     * @see KeyBasedQueryBuilderInterface
     */
    public KeyBasedQueryBuilderInterface<QueryBuilder> query(List<Key> keyList) {
        return new QueryBuilder(this, keyList);
    }

    // -------------------
    // CUD functionality (chainable batch operations)
    // -------------------

    /**
     * Begin an insert operation. Supports chaining multiple heterogeneous operations.
     *
     * <p>Example:
     * <pre>{@code
     * session.insert(users.id("user-1"))
     *     .bin("name").setTo("Alice")
     *     .update(users.id("user-2"))
     *     .bin("age").add(1)
     *     .execute();
     * }</pre>
     *
     * @param key the key to insert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder insert(Key key) {
        return new ChainableOperationBuilder(this, OpType.INSERT).init(key, OpType.INSERT);
    }

    /**
     * Begin an insert operation on multiple keys.
     */
    public ChainableOperationBuilder insert(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.INSERT).init(keys, OpType.INSERT);
    }

    /**
     * Begin an insert operation on multiple keys.
     */
    public ChainableOperationBuilder insert(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.INSERT).init(buildKeyList(key1, key2, keys), OpType.INSERT);
    }

    /**
     * Begin an update operation.
     */
    public ChainableOperationBuilder update(Key key) {
        return new ChainableOperationBuilder(this, OpType.UPDATE).init(key, OpType.UPDATE);
    }

    /**
     * Begin an update operation on multiple keys.
     */
    public ChainableOperationBuilder update(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.UPDATE).init(keys, OpType.UPDATE);
    }

    /**
     * Begin an update operation on multiple keys.
     */
    public ChainableOperationBuilder update(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.UPDATE).init(buildKeyList(key1, key2, keys), OpType.UPDATE);
    }

    /**
     * Begin an upsert operation.
     */
    public ChainableOperationBuilder upsert(Key key) {
        return new ChainableOperationBuilder(this, OpType.UPSERT).init(key, OpType.UPSERT);
    }

    /**
     * Begin an upsert operation on multiple keys.
     */
    public ChainableOperationBuilder upsert(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.UPSERT).init(keys, OpType.UPSERT);
    }

    /**
     * Begin an upsert operation on multiple keys.
     */
    public ChainableOperationBuilder upsert(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.UPSERT).init(buildKeyList(key1, key2, keys), OpType.UPSERT);
    }

    /**
     * Begin a replace operation.
     */
    public ChainableOperationBuilder replace(Key key) {
        return new ChainableOperationBuilder(this, OpType.REPLACE).init(key, OpType.REPLACE);
    }

    /**
     * Begin a replace operation on multiple keys.
     */
    public ChainableOperationBuilder replace(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE).init(keys, OpType.REPLACE);
    }

    /**
     * Begin a replace operation on multiple keys.
     */
    public ChainableOperationBuilder replace(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE).init(buildKeyList(key1, key2, keys), OpType.REPLACE);
    }

    /**
     * Begin a replaceIfExists operation (replace only if record exists, fail otherwise).
     */
    public ChainableOperationBuilder replaceIfExists(Key key) {
        return new ChainableOperationBuilder(this, OpType.REPLACE_IF_EXISTS).init(key, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Begin a replaceIfExists operation on multiple keys.
     */
    public ChainableOperationBuilder replaceIfExists(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE_IF_EXISTS).init(keys, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Begin a replaceIfExists operation on multiple keys.
     */
    public ChainableOperationBuilder replaceIfExists(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE_IF_EXISTS).init(buildKeyList(key1, key2, keys), OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Begin a touch operation. Chainable with other operations.
     */
    public ChainableNoBinsBuilder touch(Key key) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .touch(key);
    }

    /**
     * Begin a touch operation on multiple keys.
     */
    public ChainableNoBinsBuilder touch(Key key1, Key key2, Key ... keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .touch(buildKeyList(key1, key2, keys));
    }

    /**
     * Begin a touch operation on multiple keys.
     */
    public ChainableNoBinsBuilder touch(List<Key> keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .touch(keys);
    }

    /**
     * Begin an exists operation. Chainable with other operations.
     */
    public ChainableNoBinsBuilder exists(Key key) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .exists(key);
    }

    /**
     * Begin an exists operation on multiple keys.
     */
    public ChainableNoBinsBuilder exists(Key key1, Key key2, Key ... keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .exists(buildKeyList(key1, key2, keys));
    }

    /**
     * Begin an exists operation on multiple keys.
     */
    public ChainableNoBinsBuilder exists(List<Key> keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .exists(keys);
    }

    /**
     * Begin a delete operation. Chainable with other operations.
     */
    public ChainableNoBinsBuilder delete(Key key) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .delete(key);
    }

    /**
     * Begin a delete operation on multiple keys.
     */
    public ChainableNoBinsBuilder delete(Key key1, Key key2, Key ... keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .delete(buildKeyList(key1, key2, keys));
    }

    /**
     * Begin a delete operation on multiple keys.
     */
    public ChainableNoBinsBuilder delete(List<Key> keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, getCurrentTransaction())
                .delete(keys);
    }

    // --------------------------------
    // Object mapping functionality
    // --------------------------------

    /**
     * Begins an insert operation using object mapping for a dataset.
     *
     * <p>This method allows you to insert Java objects into Aerospike using
     * the record mapping factory configured on the cluster. The objects will
     * be automatically serialized to Aerospike records.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * session.insert(customerDataSet)
     *     .object(customer)
     *     .execute();
     * }</pre>
     *
     * @param dataSet the dataset to insert into
     * @return an OperationObjectBuilder for configuring and executing the insert
     * @see OperationObjectBuilder
     * @see RecordMappingFactory
     */
    @SuppressWarnings("rawtypes")
	public OperationObjectBuilder insert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.INSERT);
    }

    /**
     * Begins an insert operation using type-safe object mapping for a typed dataset.
     *
     * <p>This method provides type-safe object insertion using a {@link TypeSafeDataSet}.
     * The type parameter ensures compile-time type safety when working with objects.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * TypeSafeDataSet<Customer> customers = ...;
     * session.insert(customers)
     *     .object(new Customer("John", "Doe"))
     *     .execute();
     * }</pre>
     *
     * @param <T> the type of objects being inserted
     * @param dataSet the typed dataset to insert into
     * @return a type-safe OperationObjectBuilder for configuring and executing the insert
     * @see OperationObjectBuilder
     * @see TypeSafeDataSet
     * @see RecordMappingFactory
     */
    public <T> OperationObjectBuilder<T> insert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.INSERT);
    }

    /**
     * Begins an upsert operation using object mapping for a dataset.
     *
     * <p>This method allows you to insert or update Java objects in Aerospike using
     * the record mapping factory configured on the cluster. If the record exists,
     * it will be updated; otherwise, it will be created.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * session.upsert(customerDataSet)
     *     .object(customer)
     *     .execute();
     * }</pre>
     *
     * @param dataSet the dataset to upsert into
     * @return an OperationObjectBuilder for configuring and executing the upsert
     * @see OperationObjectBuilder
     * @see RecordMappingFactory
     */
    @SuppressWarnings("rawtypes")
    public OperationObjectBuilder upsert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPSERT);
    }

    /**
     * Begins an upsert operation using type-safe object mapping for a typed dataset.
     *
     * <p>This method provides type-safe object upsertion using a {@link TypeSafeDataSet}.
     * The type parameter ensures compile-time type safety when working with objects.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * TypeSafeDataSet<Customer> customers = ...;
     * session.upsert(customers)
     *     .object(new Customer("John", "Doe"))
     *     .execute();
     * }</pre>
     *
     * @param <T> the type of objects being upserted
     * @param dataSet the typed dataset to upsert into
     * @return a type-safe OperationObjectBuilder for configuring and executing the upsert
     * @see OperationObjectBuilder
     * @see TypeSafeDataSet
     * @see RecordMappingFactory
     */
    public <T> OperationObjectBuilder<T> upsert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.UPSERT);
    }

    /**
     * Begins an update operation using object mapping for a dataset.
     *
     * <p>This method allows you to update existing Java objects in Aerospike using
     * the record mapping factory configured on the cluster. The record must exist
     * for the update to succeed.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * session.update(customerDataSet)
     *     .object(customer)
     *     .execute();
     * }</pre>
     *
     * @param dataSet the dataset to update
     * @return an OperationObjectBuilder for configuring and executing the update
     * @see OperationObjectBuilder
     * @see RecordMappingFactory
     */
    @SuppressWarnings("rawtypes")
    public OperationObjectBuilder update(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPDATE);
    }

    /**
     * Begins an update operation using type-safe object mapping for a typed dataset.
     *
     * <p>This method provides type-safe object updates using a {@link TypeSafeDataSet}.
     * The type parameter ensures compile-time type safety when working with objects.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * TypeSafeDataSet<Customer> customers = ...;
     * session.update(customers)
     *     .object(existingCustomer)
     *     .execute();
     * }</pre>
     *
     * @param <T> the type of objects being updated
     * @param dataSet the typed dataset to update
     * @return a type-safe OperationObjectBuilder for configuring and executing the update
     * @see OperationObjectBuilder
     * @see TypeSafeDataSet
     * @see RecordMappingFactory
     */
    public <T> OperationObjectBuilder<T> update(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.UPDATE);
    }

    // ---------------------------
    // Transaction functionality
    // ---------------------------

    /**
     * Return the current transaction, if any.
     * @return
     */
    public Txn getCurrentTransaction() {
        return null;
    }

    // --------------------------------------
    // Transaction helper methods
    // --------------------------------------

    /**
     * Functional interface for transactional operations that return a value.
     *
     * <p>This interface is used with {@link #doInTransactionReturning(Transactional)}
     * to execute operations within a transaction that need to return a result.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * String result = session.doInTransactionReturning(tx -> {
     *     RecordStream results = tx.query(users.id(userId)).execute();
     *     Record record = results.getFirst().get().recordOrThrow();
     *     return record.getString("name");
     * });
     * }</pre>
     *
     * @param <T> the return type of the transactional operation
     * @see #doInTransactionReturning(Transactional)
     * @see TransactionalSession
     */
    @FunctionalInterface
    public interface Transactional<T> {
        /**
         * Executes a transactional operation and returns a result.
         *
         * @param txn the transactional session to use for operations
         * @return the result of the transactional operation
         */
        T execute(TransactionalSession txn);
    }

    /**
     * Functional interface for transactional operations that do not return a value.
     *
     * <p>This interface is used with {@link #doInTransaction(TransactionalVoid)}
     * to execute operations within a transaction that do not need to return a result.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * session.doInTransaction(txn -> {
     *     txn.upsert(accounts.id("acc1"))
     *         .bin("balance").add(-100)
     *         .execute();
     *     txn.upsert(accounts.id("acc2"))
     *         .bin("balance").add(100)
     *         .execute();
     * });
     * }</pre>
     *
     * @see #doInTransaction(TransactionalVoid)
     * @see TransactionalSession
     */
    @FunctionalInterface
    public interface TransactionalVoid {
        /**
         * Executes a transactional operation.
         *
         * @param txn the transactional session to use for operations
         */
        void execute(TransactionalSession txn);
    }

    /**
     * Executes a transactional operation and returns a value.
     *
     * <p>Use this method when your transaction needs to return a result, such as
     * reading data or computing a value based on transactional operations.</p>
     *
     * <p><b>Why the different name?</b> This method is named differently from
     * {@link #doInTransaction(TransactionalVoid)} to avoid Java type inference ambiguity
     * with complex lambda bodies. Without distinct names, the compiler cannot determine
     * which overload to use when the lambda contains control flow statements like
     * {@code while} loops, forcing users to add explicit {@code return null;} statements.</p>
     *
     * <p>The transaction provides automatic retry logic for transient failures and ensures
     * proper cleanup. Operations will be retried automatically for result codes like
     * MRT_BLOCKED, MRT_VERSION_MISMATCH, and TXN_FAILED.</p>
     *
     * <p><b>Example usage:</b>
     * <pre>{@code
     * String userName = session.doInTransactionReturning(tx -> {
     *     RecordStream results = tx.query(users.id(userId)).execute();
     *     Record record = results.getFirst().get().recordOrThrow();
     *     return record.getString("name");
     * });
     * }</pre>
     *
     * @param <T> the return type
     * @param operation the transactional operation to execute
     * @return the value returned by the operation
     * @throws AerospikeException if the operation fails with a non-retryable error
     * @throws RuntimeException if any other exception occurs during execution
     * @see TransactionalSession#doInTransactionReturning(Transactional)
     * @see #doInTransaction(TransactionalVoid)
     */
    public <T> T doInTransactionReturning(Transactional<T> operation) {
        return new TransactionalSession(cluster, behavior).doInTransactionReturning(operation);
    }

    /**
     * Executes a transactional operation that does not return a value.
     *
     * <p>Use this method when your transaction only needs to perform operations
     * without returning a result to the caller. This is the most common case for
     * transactional writes and updates.</p>
     *
     * <p>The transaction provides automatic retry logic for transient failures and ensures
     * proper cleanup. Operations will be retried automatically for result codes like
     * MRT_BLOCKED, MRT_VERSION_MISMATCH, and TXN_FAILED.</p>
     *
     * <p><b>Example usage:</b>
     * <pre>{@code
     * session.doInTransaction(txn -> {
     *     txn.upsert(accounts.id("acc1"))
     *         .bin("balance").add(-100)
     *         .execute();
     *     txn.upsert(accounts.id("acc2"))
     *         .bin("balance").add(100)
     *         .execute();
     * });
     * }</pre>
     *
     * @param operation the transactional operation to execute
     * @throws AerospikeException if the operation fails with a non-retryable error
     * @throws RuntimeException if any other exception occurs during execution
     * @see TransactionalSession#doInTransaction(TransactionalVoid)
     * @see #doInTransactionReturning(Transactional)
     */
    public void doInTransaction(TransactionalVoid operation) {
        new TransactionalSession(cluster, behavior).doInTransaction(txn -> {
            operation.execute(txn);
//            return null; // Hidden from user
        });
    }

    // ------------------------------------
    // Background Operations functionality
    // ------------------------------------
    /**
     * Enter background task mode for performing set-level operations asynchronously
     * on the server side. Background operations run as server-side scans/queries
     * and return an ExecuteTask for monitoring completion.
     *
     * <p><b>Background Operations:</b></p>
     * <ul>
     *   <li>Run on entire sets (not specific keys)</li>
     *   <li>Cannot be part of transactions</li>
     *   <li>Return ExecuteTask (not record data)</li>
     *   <li>Support UPDATE, DELETE, and TOUCH operations only</li>
     * </ul>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     *   <li>Bulk updates based on criteria</li>
     *   <li>Cleaning up old records</li>
     *   <li>Extending TTL for active records</li>
     * </ul>
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * // Update all customers over 30
     * ExecuteTask task = session.backgroundTask()
     *     .update(customerDataSet)
     *     .where("$.age > 30")
     *     .bin("category").setTo("senior")
     *     .execute();
     *
     * task.waitTillComplete();
     *
     * // Delete old inactive records
     * ExecuteTask deleteTask = session.backgroundTask()
     *     .delete(customerDataSet)
     *     .where("$.lastLogin < 1609459200000")
     *     .execute();
     *
     * // Touch active users to extend TTL
     * ExecuteTask touchTask = session.backgroundTask()
     *     .touch(activeUsers)
     *     .where("$.status == 'active'")
     *     .expireRecordAfter(Duration.ofDays(30))
     *     .execute();
     * }</pre>
     *
     * @return BackgroundTaskSession for creating background operations
     * @see BackgroundTaskSession
     * @see BackgroundOperationBuilder
     */
    public BackgroundTaskSession backgroundTask() {
        return new BackgroundTaskSession(this);
    }

    // ---------------------
    // Info functionality
    // ---------------------

    /**
     * Gets an InfoCommands instance for executing Aerospike info commands.
     *
     * <p>InfoCommands provides high-level methods to execute common Aerospike info
     * commands, such as retrieving namespace details, set information, secondary
     * index information, and build information.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * InfoCommands commands = session.info();
     *
     * // Get all namespaces
     * Set<String> namespaces = commands.namespaces();
     *
     * // Get namespace details
     * Optional<NamespaceDetail> nsDetail = commands.namespaceDetails("test");
     *
     * // Get all secondary indexes
     * List<Sindex> indexes = commands.secondaryIndexes();
     * }</pre>
     *
     * @return an InfoCommands instance for executing info commands
     * @see InfoCommands
     */
    public InfoCommands info() {
        return new InfoCommands(this);
    }

    /**
     * Checks if a namespace is configured for strong consistency (SC) mode.
     *
     * <p>Strong consistency namespaces provide stronger consistency guarantees
     * compared to eventually consistent (AP) namespaces. This affects how
     * operations are performed, particularly for transactions and certain read
     * operations.</p>
     *
     * <p>This method is used internally to determine the appropriate operation
     * settings and policies based on the namespace's consistency mode.</p>
     *
     * @param namespace the namespace to check
     * @return true if the namespace is in strong consistency mode, false otherwise
     * @throws IllegalArgumentException if the namespace is unknown
     */
    public boolean isNamespaceSC(String namespace) {
        Partitions partitionMap = cluster.partitionMap.get(namespace);
        if (partitionMap == null) {
            throw new IllegalArgumentException("Unknown namespace " + namespace);
        }
        return partitionMap.scMode;
    }

    //---------------------
    // Index functionality
    //---------------------

    /**
     * Create scalar secondary index.
     * This asynchronous server call will return before command is complete.
     * The user can optionally wait for command completion by using the returned
     * IndexTask instance.
     *
	 * @param set					dataset containing namespace and set information
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param ctx					optional context to index on elements within a CDT
	 * @throws AerospikeException	if index create fails
     */
    public final IndexTask createIndex(
    	DataSet set,
        String indexName,
        String binName,
        IndexType indexType,
		IndexCollectionType indexCollectionType,
		CTX... ctx
    ) {
		Node node = cluster.getRandomNode();
		String command = buildCreateIndexInfoCommand(node, set.getNamespace(), set.getSet(), indexName, binName, indexType, indexCollectionType, ctx, null);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(node, command);

		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, set.getNamespace(), indexName, true, 1000);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Create index failed: " + response);
    }

	/**
	 * Create an expression-based secondary index with the provided index collection type
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 *
	 * @param set					dataset containing namespace and set information
	 * @param indexName				name of secondary index
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param exp					expression on which to build the index
	 * @throws AerospikeException	if index create fails
	 */
	public final IndexTask createIndex(
	    DataSet set,
		String indexName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		Expression exp
	) {
		Node node = this.cluster.getRandomNode();
		String command = buildCreateIndexInfoCommand(node, set.getNamespace(), set.getSet(), indexName, null, indexType, indexCollectionType, null, exp);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(node, command);

		if (response.equalsIgnoreCase("OK")) {
			// Return task that could optionally be polled for completion.
			return new IndexTask(cluster, set.getNamespace(), indexName, true, 1000);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Create index failed: " + response);
	}

	private String buildCreateIndexInfoCommand(
		Node node,
		String namespace,
		String setName,
		String indexName,
		String binName,
		IndexType indexType,
		IndexCollectionType indexCollectionType,
		CTX[] ctx,
		Expression exp
	) {
		StringBuilder sb = new StringBuilder(1024);
		Version currentServerVersion = node.getVersion();
		String createIndexCommand = currentServerVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "sindex-create:namespace=": "sindex-create:ns=";

		sb.append(createIndexCommand);
		sb.append(namespace);

		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}

		sb.append(";indexname=");
		sb.append(indexName);

		if (exp != null) {
			String base64 = exp.getBase64();

			sb.append(";exp=");
			sb.append(base64);

			if (indexCollectionType != IndexCollectionType.DEFAULT) {
				sb.append(";indextype=");
				sb.append(indexCollectionType);
			}

			sb.append(";type=");
			sb.append(indexType);
		} else {
			if (ctx != null && ctx.length > 0) {
				byte[] bytes = Pack.pack(ctx);
				String base64 = Crypto.encodeBase64(bytes);

				sb.append(";context=");
				sb.append(base64);
			}

			if (indexCollectionType != IndexCollectionType.DEFAULT) {
				sb.append(";indextype=");
				sb.append(indexCollectionType);
			}

			if (node.getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1)) {
				sb.append(";bin=");
				sb.append(binName);
				sb.append(";type=");
				sb.append(indexType);
			} else {
				sb.append(";indexdata=");
				sb.append(binName);
				sb.append(',');
				sb.append(indexType);
			}
		}

		return sb.toString();
	}

	/**
	 * Delete secondary index.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 *
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
	 * @param indexName				name of secondary index
	 * @throws AerospikeException	if index drop fails
	 */
	public final IndexTask dropIndex(DataSet set, String indexName) {
		Node node = this.cluster.getRandomNode();
		String command = buildDropIndexInfoCommand(node, set.getNamespace(), set.getSet(), indexName);

		// Send index command to one node. That node will distribute the command to other nodes.
		String response = sendInfoCommand(node, command);

		if (response.equalsIgnoreCase("OK")) {
			return new IndexTask(cluster, set.getNamespace(), indexName, false, 1000);
		}

		int code = parseIndexErrorCode(response);
		throw new AerospikeException(code, "Drop index failed: " + response);
	}

	private String buildDropIndexInfoCommand(Node node, String namespace, String setName, String indexName) {
		StringBuilder sb = new StringBuilder(500);
		Version currentServerVersion = node.getVersion();
		String deleteIndexCommand = currentServerVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "sindex-delete:namespace=": "sindex-delete:ns=";

		sb.append(deleteIndexCommand);
		sb.append(namespace);

		if (setName != null && setName.length() > 0) {
			sb.append(";set=");
			sb.append(setName);
		}
		sb.append(";indexname=");
		sb.append(indexName);
		return sb.toString();
	}

	private String sendInfoCommand(Node node, String command) {
		Connection conn = node.getConnection(1000, 1000);
		Info info;

		try {
			info = new Info(node, conn, command);
			node.putConnection(conn);
		}
		catch (Throwable e) {
			node.closeConnection(conn);
			throw e;
		}
		return info.getValue();
	}

	private static int parseIndexErrorCode(String response) {
		Info.Error error = new Info.Error(response);
		return (error.code == 0)? ResultCode.SERVER_ERROR : error.code;
	}
}
