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

import com.aerospike.client.fluent.cdt.CTX;
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

		String response = Info.request(node, sb.toString());

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

    public IndexBasedQueryBuilderInterface<QueryBuilder> query(DataSet dataSet) {
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

    @SuppressWarnings("rawtypes")
	public OperationObjectBuilder insert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.INSERT);
    }

    public <T> OperationObjectBuilder<T> insert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.INSERT);
    }

    @SuppressWarnings("rawtypes")
    public OperationObjectBuilder upsert(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.UPSERT);
    }

    public <T> OperationObjectBuilder<T> upsert(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.UPSERT);
    }

    @SuppressWarnings("rawtypes")
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

    //---------------------
    // Index functionality
    //---------------------

    /**
     * Create scalar secondary index.
     * This asynchronous server call will return before command is complete.
     * The user can optionally wait for command completion by using the returned
     * IndexTask instance.
     *
	 * @param namespace				namespace - equivalent to database name
	 * @param setName				optional set name - equivalent to database table
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
