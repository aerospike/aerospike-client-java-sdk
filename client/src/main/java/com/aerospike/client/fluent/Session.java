/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.info.InfoCommands;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.query.BaseQueryBuilder;
import com.aerospike.client.fluent.query.KeyBasedQueryBuilderInterface;
import com.aerospike.client.fluent.query.QueryBuilder;

public class Session {
    private final Cluster cluster;
    private final Behavior behavior;

    protected Session(Cluster cluster, Behavior behavior) {
        this.cluster = cluster;
        this.behavior = behavior;
    }

    public class ExpressionBuilder {
        private Expression filterExpression = null;
        public ExpressionBuilder(Exp exp) {
            this.filterExpression = Exp.build(exp);
        }

        public ExpressionBuilder(Expression exp) {
            this.filterExpression = exp;
        }

        public Expression getFilterExpression() {
            return filterExpression;
        }
    }

    public Behavior getBehavior() {
        return this.behavior;
    }

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

		String response = Info.request(null, node, sb.toString());

		if (! response.equalsIgnoreCase("ok")) {
			throw new AerospikeException("Truncate failed: " + response);
		}
    }

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

    public BaseQueryBuilder<QueryBuilder> query(DataSet dataSet) {
        return new QueryBuilder(this, dataSet);
    }

    public KeyBasedQueryBuilderInterface<QueryBuilder> query(Key key) {
        return new QueryBuilder(this, key);
    }

    /**
     * Point or batch read with one or more keys. Query with no parameters is valid, so must have (Key, Key...) to differentiate
     * @param key
     * @param keys
     * @return
     */
    public KeyBasedQueryBuilderInterface<QueryBuilder> query(Key key1, Key key2, Key...keys) {
        return new QueryBuilder(this, buildKeyList(key1, key2, keys));
    }

    public KeyBasedQueryBuilderInterface<QueryBuilder> query(List<Key> keyList) {
        return new QueryBuilder(this, keyList);
    }

    // -------------------
    // CUD functionality
    // -------------------

    public OperationBuilder insert(Key key) {
        return new OperationBuilder(this, key, OpType.INSERT);
    }

    public OperationBuilder update(Key key) {
        return new OperationBuilder(this, key, OpType.UPDATE);
    }

    public OperationBuilder upsert(Key key) {
        return new OperationBuilder(this, key, OpType.UPSERT);
    }

    public OperationBuilder replace(Key key) {
        return new OperationBuilder(this, key, OpType.UPSERT);
    }

    public OperationBuilder upsert(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.UPSERT);
    }

    public OperationBuilder upsert(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.UPSERT);
    }

    public OperationBuilder insert(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.INSERT);
    }

    public OperationBuilder insert(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.INSERT);
    }

    public OperationBuilder update(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.UPDATE);
    }

    public OperationBuilder update(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.UPDATE);
    }

    public OperationBuilder replace(List<Key> keys) {
        return new OperationBuilder(this, keys, OpType.REPLACE);
    }

    public OperationBuilder replace(Key key1, Key key2, Key... keys) {
        List<Key> keyList = buildKeyList(key1, key2, keys);
        return new OperationBuilder(this, keyList, OpType.REPLACE);
    }

    public OperationWithNoBinsBuilder touch(Key key) {
        return new OperationWithNoBinsBuilder(this, key, OpType.TOUCH);
    }

    public OperationWithNoBinsBuilder touch(Key key1, Key key2, Key ... keys) {
        return new OperationWithNoBinsBuilder(this, buildKeyList(key1, key2, keys), OpType.TOUCH);
    }

    public OperationWithNoBinsBuilder touch(List<Key> keys) {
        return new OperationWithNoBinsBuilder(this, keys, OpType.TOUCH);
    }

    public OperationWithNoBinsBuilder exists(Key key) {
        return new OperationWithNoBinsBuilder(this, key, OpType.EXISTS);
    }

    public OperationWithNoBinsBuilder exists(Key key1, Key key2, Key ... keys) {
        return new OperationWithNoBinsBuilder(this, buildKeyList(key1, key2, keys), OpType.EXISTS);
    }

    public OperationWithNoBinsBuilder exists(List<Key> keys) {
        return new OperationWithNoBinsBuilder(this, keys, OpType.EXISTS);
    }
    public OperationWithNoBinsBuilder delete(Key key) {
        return new OperationWithNoBinsBuilder(this, key, OpType.DELETE);
    }

    public OperationWithNoBinsBuilder delete(Key key1, Key key2, Key ... keys) {
        return new OperationWithNoBinsBuilder(this, buildKeyList(key1, key2, keys), OpType.DELETE);
    }

    public OperationWithNoBinsBuilder delete(List<Key> keys) {
        return new OperationWithNoBinsBuilder(this, keys, OpType.DELETE);
    }

    // --------------------------------
    // Object mapping functionality
    // --------------------------------

    public OperationObjectBuilder insert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.INSERT);
    }

    public <T> OperationObjectBuilder<T> insert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.INSERT);
    }

    public OperationObjectBuilder upsert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPSERT);
    }

    public <T> OperationObjectBuilder<T> upsert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.UPSERT);
    }

    public OperationObjectBuilder update(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPDATE);
    }

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

    // Functional interface for returning a result
    @FunctionalInterface
    public interface Transactional<T> {
        T execute(TransactionalSession txn);
    }

    // Functional interface for void-returning operations
    @FunctionalInterface
    public interface TransactionalVoid {
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

    // ---------------------
    // Info functionality
    // ---------------------

    public InfoCommands info() {
        return new InfoCommands(this);
    }

    public boolean isNamespaceSC(String namespace) {
        Partitions partitionMap = cluster.partitionMap.get(namespace);
        if (partitionMap == null) {
            throw new IllegalArgumentException("Unknown namespace " + namespace);
        }
        return partitionMap.scMode;
    }
}
