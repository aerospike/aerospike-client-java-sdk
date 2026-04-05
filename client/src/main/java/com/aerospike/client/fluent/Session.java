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

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.command.Buffer;
import com.aerospike.client.fluent.command.Connection;
import com.aerospike.client.fluent.command.Info;
import com.aerospike.client.fluent.command.RegisterCommand;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.info.InfoCommands;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.query.IndexBasedQueryBuilderInterface;
import com.aerospike.client.fluent.query.IndexCollectionType;
import com.aerospike.client.fluent.query.QueryBuilder;
import com.aerospike.client.fluent.task.IndexTask;
import com.aerospike.client.fluent.task.RegisterTask;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.client.fluent.util.Crypto;
import com.aerospike.client.fluent.util.Pack;
import com.aerospike.client.fluent.util.Util;
import com.aerospike.client.fluent.util.Version;

/**
 * Primary entry point for data and admin operations against an Aerospike cluster in the fluent API.
 *
 * <p>A session is tied to a {@link Cluster} and a {@link Behavior} profile (timeouts, retries,
 * consistency, and related defaults). Obtain instances with {@link Cluster#createSession(Behavior)}.
 * The base session is not transactional; use {@link TransactionalSession} when you need
 * multi-record transaction support.</p>
 *
 * <p>Typical responsibilities include single-key and batch reads and writes, queries and scans,
 * secondary index management, UDF registration, truncate, and cluster info commands.</p>
 *
 * @see Cluster#createSession(Behavior)
 * @see Behavior
 * @see TransactionalSession
 */
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
     * <p>Example:</p>
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
     *
     * @param set namespace/set to truncate (see {@link #truncate(DataSet, Calendar)})
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
     *
     * @param set namespace/set to truncate
     * @param beforeLastUpdate if non-null, only remove records whose last-update time is before this instant
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
     * <p>Example:</p>
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
     * The query will return at most one record. The returned builder supports
     * chaining to write operations (upsert, update, insert, etc.).</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * // Simple query
     * RecordStream results = session.query(dataset.id("user-123"))
     *     .bins("name", "email")
     *     .execute();
     *
     * // Query then write (chainable)
     * session.query(dataset.id("user-1"))
     *     .bin("name").get()
     *     .upsert(dataset.id("user-2"))
     *     .bin("status").setTo("active")
     *     .execute();
     * }</pre>
     *
     * @param key the key to query
     * @return a chainable query builder for configuring and executing the query
     * @see ChainableQueryBuilder
     */
    public ChainableQueryBuilder query(Key key) {
        return new ChainableQueryBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .initQuery(key);
    }

    /**
     * Creates a query builder for querying multiple keys using varargs.
     *
     * <p>This method creates a batch query that will perform lookups for multiple keys.
     * If only one key is provided, it will use single key optimization. The returned
     * builder supports chaining to write operations.</p>
     *
     * <p>This overload is provided to differentiate from the single-key query method
     * when querying with no parameters is valid.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * RecordStream results = session.query(
     *     dataset.id("user-1"),
     *     dataset.id("user-2"),
     *     dataset.id("user-3")
     * ).execute();
     * }</pre>
     *
     * @param key1 the first key to query
     * @param key2 the second key to query
     * @param keys additional keys to query (varargs)
     * @return a chainable query builder for configuring and executing the batch query
     * @see ChainableQueryBuilder
     */
    public ChainableQueryBuilder query(Key key1, Key key2, Key...keys) {
        return new ChainableQueryBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .initQuery(buildKeyList(key1, key2, keys));
    }

    /**
     * Creates a query builder for querying multiple keys from a list.
     *
     * <p>This method creates a batch query that will perform lookups for multiple keys.
     * If only one key is provided in the list, it will use single key optimization.
     * The returned builder supports chaining to write operations.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * List<Key> keys = Arrays.asList(
     *     dataset.id("user-1"),
     *     dataset.id("user-2"),
     *     dataset.id("user-3")
     * );
     * RecordStream results = session.query(keys)
     *     .bins("name", "email")
     *     .execute();
     * }</pre>
     *
     * @param keyList the list of keys to query
     * @return a chainable query builder for configuring and executing the batch query
     * @see ChainableQueryBuilder
     */
    public ChainableQueryBuilder query(List<Key> keyList) {
        return new ChainableQueryBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .initQuery(keyList);
    }

    // -------------------
    // UDF execution (chainable batch operations)
    // -------------------

	/**
	 * Register package located in a file containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 *
	 * <p>Example:
	 * <pre>{@code
	 * client.registerUdf("udf/record_example.lua", "record_example.lua");
	 * }</pre>
	 *
	 * @param clientPath			path of client file containing user defined functions, relative to current directory
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @return task that can be used to wait for registration to finish on the cluster
	 * @throws AerospikeException	if register fails
	 */
	public final RegisterTask registerUdf(String clientPath, String serverPath) {
		File file = new File(clientPath);
		byte[] bytes = Util.readFile(file);
		return RegisterCommand.register(cluster, bytes, serverPath);
	}

	/**
	 * Register package located in a resource containing user defined functions with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 *
	 * <p>Example:
	 * <pre>{@code
	 * client.registerUdf(TestQueryExecute.class.getClassLoader(), "udf/record_example.lua", "record_example.lua");
	 * }</pre>
	 *
	 * @param resourceLoader		class loader where resource is located.  Example: MyClass.class.getClassLoader() or Thread.currentThread().getContextClassLoader() for webapps
	 * @param resourcePath			class path where Lua resource is located
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @return task that can be used to wait for registration to finish on the cluster
	 * @throws AerospikeException	if register fails
	 */
	public final RegisterTask registerUdf(
		ClassLoader resourceLoader, String resourcePath, String serverPath
	) {
		byte[] bytes = Util.readResource(resourceLoader, resourcePath);
		return RegisterCommand.register(cluster, bytes, serverPath);
	}

	/**
	 * Register UDF functions located in a code string with server.
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * RegisterTask instance.
	 *
	 * <p>Example:
	 * <pre>{@code
	 * String code = """
	 * function writeIfGenerationNotChanged(r,name,value,gen)
	 *     if record.gen(r) == gen then
	 *         r[name] = value
	 *         aerospike:update(r)
	 *     end
	 * end
	 * """;
	 *
	 * client.registerUdfString(code, "gen.lua");
	 * }</pre>
	 *
	 * @param code					code string containing user defined functions.
	 * @param serverPath			path to store user defined functions on the server, relative to configured script directory.
	 * @return task that can be used to wait for registration to finish on the cluster
	 * @throws AerospikeException	if register fails
	 */
	public final RegisterTask registerUdfString(String code, String serverPath) {
		byte[] bytes = Buffer.stringToUtf8(code);
		return RegisterCommand.register(cluster, bytes, serverPath);
	}

	/**
	 * Remove user defined function from server nodes.
     *
     * <p>Example:
     * <pre>{@code
     * session.removeUdf("mylua.lua").execute();
     * }</pre>
	 *
	 * @param serverPath			location of UDF on server nodes.  Example: mylua.lua
	 * @throws AerospikeException	if remove fails
	 */
	public final void removeUdf(String serverPath)
		throws AerospikeException {
		// Send UDF command to one node. That node will distribute the UDF command to other nodes.
		String command = "udf-remove:filename=" + serverPath;
		Node node = cluster.getRandomNode();
		String response = Info.request(node, command);

		if (response.equalsIgnoreCase("ok")) {
			return;
		}

		if (response.startsWith("error=file_not_found")) {
			// UDF has already been removed.
			return;
		}
		throw new AerospikeException("Remove UDF failed: " + response);
	}

	/**
     * Execute a UDF (User Defined Function) for the given key.
     * Supports chaining multiple heterogeneous operations.
     *
     * <p>UDFs are server-side Lua functions that can perform custom operations on records.
     * They are registered on the server and referenced by package name and function name.</p>
     *
     * <p>Example:
     * <pre>{@code
     * // Execute a UDF with arguments
     * session.executeUdf(dataset.id("user-1"))
     *     .function("myPackage", "myFunction")
     *     .passing("arg1", 42, true)
     *     .execute();
     *
     * // Execute a UDF with no arguments
     * session.executeUdf(dataset.id("user-1"))
     *     .function("myPackage", "myFunction")
     *     .execute();
     *
     * // Chain with other operations
     * session.executeUdf(dataset.id("user-1"))
     *     .function("myPackage", "myFunction")
     *     .passing("arg1")
     *     .upsert(dataset.id("user-2"))
     *     .bin("name").setTo("Alice")
     *     .execute();
     * }</pre>
     *
     * @param key the key to execute the UDF on
     * @return UdfFunctionBuilder requiring function specification before execution
     * @see UdfFunctionBuilder
     * @see ChainableUdfBuilder
     */
    public UdfFunctionBuilder executeUdf(Key key) {
        return new UdfFunctionBuilder(this, List.of(key), new ArrayList<>(),
                null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction());
    }

    /**
     * Execute a UDF (User Defined Function) on multiple keys from a list.
     * Supports chaining multiple heterogeneous operations.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = Arrays.asList(dataset.id("user-1"), dataset.id("user-2"));
     * session.executeUdf(keys)
     *     .function("myPackage", "myFunction")
     *     .passing("arg1")
     *     .execute();
     * }</pre>
     *
     * @param keyList the list of keys to execute the UDF on
     * @return UdfFunctionBuilder requiring function specification before execution
     * @see UdfFunctionBuilder
     * @see ChainableUdfBuilder
     */
    public UdfFunctionBuilder executeUdf(List<Key> keyList) {
        return new UdfFunctionBuilder(this, keyList, new ArrayList<>(),
                null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction());
    }

    /**
     * Execute a UDF (User Defined Function) on multiple keys using varargs.
     * Supports chaining multiple heterogeneous operations.
     *
     * <p>Example:
     * <pre>{@code
     * session.executeUdf(dataset.id("user-1"), dataset.id("user-2"))
     *     .function("myPackage", "myFunction")
     *     .passing("arg1")
     *     .execute();
     * }</pre>
     *
     * @param key1 the first key to execute the UDF on
     * @param key2 the second key to execute the UDF on
     * @param keys additional keys to execute the UDF on (varargs)
     * @return UdfFunctionBuilder requiring function specification before execution
     * @see UdfFunctionBuilder
     * @see ChainableUdfBuilder
     */
    public UdfFunctionBuilder executeUdf(Key key1, Key key2, Key... keys) {
        return new UdfFunctionBuilder(this, buildKeyList(key1, key2, keys), new ArrayList<>(),
                null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction());
    }

    // -------------------
    // CUD functionality (chainable batch operations)
    // -------------------

    /**
     * Create record for the given key if it does not exist.
     * The command will fail if the key exists.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.insert(key)
     *     .bin("name").setTo("Alice")
     *     .bin("age").add(1)
     *     .execute();
     * }</pre>
     *
     * @param key the key to insert
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder insert(Key key) {
        return new ChainableOperationBuilder(this, OpType.INSERT).init(key, OpType.INSERT);
    }

    /**
     * Create records for the given keys if they do not exist.
     * The command will fail if a key exists.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.insert(keys)
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param keys keys to insert in one batch chain
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder insert(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.INSERT).init(keys, OpType.INSERT);
    }

    /**
     * Create records for the given keys if they do not exist.
     * The command will fail if a key exists.
     *
     * <p>Example:
     * <pre>{@code
     * session.insert(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder insert(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.INSERT).init(buildKeyList(key1, key2, keys), OpType.INSERT);
    }

    /**
     * Update record for the given key if it exists.
     * The command will fail if the key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.update(key)
     *     .bin("name").setTo("Alice")
     *     .bin("age").add(1)
     *     .execute();
     * }</pre>
     *
     * @param key key to update
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder update(Key key) {
        return new ChainableOperationBuilder(this, OpType.UPDATE).init(key, OpType.UPDATE);
    }

    /**
     * Update records for the given keys if they exist.
     * The command will fail if a key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.update(keys)
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param keys keys to update in one batch chain
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder update(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.UPDATE).init(keys, OpType.UPDATE);
    }

    /**
     * Update records for the given keys if they exist.
     * The command will fail if a key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * session.update(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder update(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.UPDATE).init(buildKeyList(key1, key2, keys), OpType.UPDATE);
    }

    /**
     * Write record for the given key.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.upsert(key)
     *     .bin("name").setTo("Alice")
     *     .bin("age").add(1)
     *     .execute();
     * }</pre>
     *
     * @param key key to upsert
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder upsert(Key key) {
        return new ChainableOperationBuilder(this, OpType.UPSERT).init(key, OpType.UPSERT);
    }

    /**
     * Write records for the given keys.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.upsert(keys)
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
      *
     * @param keys keys to upsert in one batch chain
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder upsert(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.UPSERT).init(keys, OpType.UPSERT);
    }

    /**
     * Write records for the given keys.
     *
     * <p>Example:
     * <pre>{@code
     * session.upsert(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder upsert(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.UPSERT).init(buildKeyList(key1, key2, keys), OpType.UPSERT);
    }

    /**
     * Replace record for the given key.
     * Write referenced bins and delete other non-referenced bins.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.replace(key)
     *     .bin("name").setTo("Alice")
     *     .bin("age").add(1)
     *     .execute();
     * }</pre>
     *
     * @param key key whose record is replaced in full
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder replace(Key key) {
        return new ChainableOperationBuilder(this, OpType.REPLACE).init(key, OpType.REPLACE);
    }

    /**
     * Replace records for the given keys.
     * Write referenced bins and delete other non-referenced bins.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.replace(keys)
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param keys keys to replace in one batch chain
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder replace(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE).init(keys, OpType.REPLACE);
    }

    /**
     * Replace records for the given keys.
     * Write referenced bins and delete other non-referenced bins.
     *
     * <p>Example:
     * <pre>{@code
     * session.replace(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder replace(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE).init(buildKeyList(key1, key2, keys), OpType.REPLACE);
    }

    /**
     * Replace record for the given key if it exists.
     * Write referenced bins and delete other non-referenced bins.
     * The command will fail if the key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.replaceIfExists(key)
     *     .bin("name").setTo("Alice")
     *     .bin("age").add(1)
     *     .execute();
     * }</pre>
     *
     * @param key key to replace
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder replaceIfExists(Key key) {
        return new ChainableOperationBuilder(this, OpType.REPLACE_IF_EXISTS).init(key, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Replace records for the given keys if they exist.
     * Write referenced bins and delete other non-referenced bins.
     * The command will fail if a key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.replaceIfExists(keys)
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param keys keys to replace-if-exists in one batch chain
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder replaceIfExists(List<Key> keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE_IF_EXISTS).init(keys, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Replace records for the given keys if they exist.
     * Write referenced bins and delete other non-referenced bins.
     * The command will fail if a key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * session.replaceIfExists(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .bin("status").setTo("new")
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further operations in the same batch
     */
    public ChainableOperationBuilder replaceIfExists(Key key1, Key key2, Key... keys) {
        return new ChainableOperationBuilder(this, OpType.REPLACE_IF_EXISTS).init(buildKeyList(key1, key2, keys), OpType.REPLACE_IF_EXISTS);
    }

    /**
	 * Reset record time to expiration for the given key.
	 * If the record does not exist, it can't be created because the server deletes empty records.
	 * The command will fail if the key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.touch(key)
     *     .expireRecordAfter(Duration.ofSeconds(1000))
     *     .execute();
     * }</pre>
     *
     * @param key key whose record TTL is touched
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder touch(Key key) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .touch(key);
    }

    /**
	 * Reset records time to expiration for the given keys.
	 * If a record does not exist, it can't be created because the server deletes empty records.
	 * The command will fail if a key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.touch(keys)
     *     .expireRecordAfter(Duration.ofSeconds(1000))
     *     .execute();
     * }</pre>
     *
     * @param keys keys to touch in one batch chain
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder touch(List<Key> keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .touch(keys);
    }

    /**
	 * Reset records time to expiration for the given keys.
	 * If a record does not exist, it can't be created because the server deletes empty records.
	 * The command will fail if a key does not exist.
     *
     * <p>Example:
     * <pre>{@code
     * session.touch(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .expireRecordAfter(Duration.ofSeconds(1000))
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder touch(Key key1, Key key2, Key ... keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .touch(buildKeyList(key1, key2, keys));
    }

    /**
	 * Determine if a key exists.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * RecordStream rs = session.exists(key).execute();
	 * Record rec = rs.getFirstRecord();
     * }</pre>
     *
     * @param key key to test for existence
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder exists(Key key) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .exists(key);
    }

    /**
	 * Determine if keys exist.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * RecordStream rs = session.exists(keys).execute();
     * List<Boolean> exists = rs.stream().map(rec -> rec.asBoolean()).toList();
     * }</pre>
     *
     * @param keys keys to test in one batch chain
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder exists(List<Key> keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .exists(keys);
    }

    /**
	 * Determine if keys exist.
     *
     * <p>Example:
     * <pre>{@code
     * RecordStream rs = session.exists(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .execute();
     * List<Boolean> exists = rs.stream().map(rec -> rec.asBoolean()).toList();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder exists(Key key1, Key key2, Key ... keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .exists(buildKeyList(key1, key2, keys));
    }

    /**
     * Delete record for the given key.
     *
     * <p>Example:
     * <pre>{@code
     * Key key = dataset.id("user-1");
     * session.delete(key).execute();
     * }</pre>
     *
     * @param key key to delete
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder delete(Key key) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .delete(key);
    }

    /**
     * Delete records for the given keys.
     *
     * <p>Example:
     * <pre>{@code
     * List<Key> keys = dataset.ids("user-1", "user-2");
     * session.delete(keys).execute();
     * }</pre>
     *
     * @param keys keys to delete in one batch chain
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder delete(List<Key> keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .delete(keys);
    }

    /**
     * Delete records for the given keys.
     *
     * <p>Example:
     * <pre>{@code
     * session.delete(dataset.id("user-1"), dataset.id("user-2"), dataset.id("user-3"))
     *     .execute();
     * }</pre>
     *
     * @param key1 first key
     * @param key2 second key
     * @param keys additional keys
     * @return builder for further no-bin operations in the same batch
     */
    public ChainableNoBinsBuilder delete(Key key1, Key key2, Key ... keys) {
        return new ChainableNoBinsBuilder(this, new ArrayList<>(), null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, getCurrentTransaction())
                .delete(buildKeyList(key1, key2, keys));
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
     * <p>Example:</p>
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
     * <p>Example:</p>
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

    /**
     * Begins a replace operation using object mapping for a dataset.
     *
     * <p>This method allows you to replace entire Aerospike records with Java objects using
     * the record mapping factory configured on the cluster. Unlike upsert, replace will
     * completely overwrite all bins in the record with the values from the mapped object.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.replace(customerDataSet)
     *     .object(customer)
     *     .execute();
     * }</pre>
     *
     * @param dataSet the dataset to replace into
     * @return an OperationObjectBuilder for configuring and executing the replace
     * @see OperationObjectBuilder
     * @see RecordMappingFactory
     */
    @SuppressWarnings("rawtypes")
    public OperationObjectBuilder replace(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.REPLACE);
    }

    /**
     * Begins a replace operation using type-safe object mapping for a typed dataset.
     *
     * <p>This method provides type-safe object replacement using a {@link TypeSafeDataSet}.
     * The type parameter ensures compile-time type safety when working with objects.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * TypeSafeDataSet<Customer> customers = ...;
     * session.replace(customers)
     *     .object(new Customer("John", "Doe"))
     *     .execute();
     * }</pre>
     *
     * @param <T> the type of objects being replaced
     * @param dataSet the typed dataset to replace into
     * @return a type-safe OperationObjectBuilder for configuring and executing the replace
     * @see OperationObjectBuilder
     * @see TypeSafeDataSet
     * @see RecordMappingFactory
     */
    public <T> OperationObjectBuilder<T> replace(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.REPLACE);
    }

    /**
     * Begins a replaceIfExists operation using object mapping for a dataset.
     *
     * <p>This method replaces the entire record only if it already exists in the database.
     * If the record does not exist, the operation will fail.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.replaceIfExists(customerDataSet)
     *     .object(customer)
     *     .execute();
     * }</pre>
     *
     * @param dataSet the dataset to replace into
     * @return an OperationObjectBuilder for configuring and executing the replace
     * @see OperationObjectBuilder
     * @see RecordMappingFactory
     */
    @SuppressWarnings("rawtypes")
    public OperationObjectBuilder replaceIfExists(DataSet dataSet) {
        return new OperationObjectBuilder(this, dataSet, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Begins a replaceIfExists operation using type-safe object mapping for a typed dataset.
     *
     * <p>This method provides type-safe object replacement using a {@link TypeSafeDataSet},
     * only if the record already exists. If the record does not exist, the operation will fail.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * TypeSafeDataSet<Customer> customers = ...;
     * session.replaceIfExists(customers)
     *     .object(existingCustomer)
     *     .execute();
     * }</pre>
     *
     * @param <T> the type of objects being replaced
     * @param dataSet the typed dataset to replace into
     * @return a type-safe OperationObjectBuilder for configuring and executing the replace
     * @see OperationObjectBuilder
     * @see TypeSafeDataSet
     * @see RecordMappingFactory
     */
    public <T> OperationObjectBuilder<T> replaceIfExists(TypeSafeDataSet<T> dataSet) {
        return new OperationObjectBuilder<T>(this, dataSet, OpType.REPLACE_IF_EXISTS);
    }

    // ---------------------------
    // Transaction functionality
    // ---------------------------

    /**
     * Returns the transaction associated with this session, if any.
     *
     * <p>This base {@code Session} implementation always returns {@code null}. Call sites use this
     * hook so the same fluent APIs can participate in transactions when a session implementation
     * supplies a non-null {@link Txn}.</p>
     *
     * @return the active transaction, or {@code null}
     */
    public Txn getCurrentTransaction() {
    	// TODO Tim This needs to be resolved.
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
     * <p>Example:</p>
     * <pre>{@code
     * String result = session.doInTransactionReturning(tx -> {
     *     RecordStream results = tx.query(dataset.id(userId)).execute();
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
     * <p>Example:</p>
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
     * <p><b>Example:</b>
     * <pre>{@code
     * String userName = session.doInTransactionReturning(tx -> {
     *     RecordStream results = tx.query(dataset.id(userId)).execute();
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
     * <p><b>Example:</b>
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
     *   <li>Support UPDATE, DELETE, TOUCH, and UDF operations</li>
     * </ul>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     *   <li>Bulk updates based on criteria</li>
     *   <li>Cleaning up old records</li>
     *   <li>Extending TTL for active records</li>
     *   <li>Executing Lua UDFs against matching records</li>
     * </ul>
     *
     * <p><b>Examples:</b></p>
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
     *
     * // Execute a UDF against all overstocked products
     * ExecuteTask udfTask = session.backgroundTask()
     *     .executeUdf(products)
     *     .function("inventory", "applyDiscount")
     *     .passing(0.20)
     *     .where("$.stock > 250")
     *     .execute();
     * }</pre>
     *
     * @return BackgroundTaskSession for creating background operations
     * @see BackgroundTaskSession
     * @see BackgroundOperationBuilder
     * @see BackgroundUdfFunctionBuilder
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
     * <p>Example:</p>
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
     * <p>Example:</p>
     * <pre>{@code
     * session.isNamespaceSC("test");
     * }</pre>
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
     * <p>Example:</p>
     * <pre>{@code
     * session.createIndex(dataSet, indexName, binName, IndexType.STRING, IndexCollectionType.DEFAULT)
     *     .waitTillComplete();
     * }</pre>
     *
	 * @param set					dataset containing namespace and set information
	 * @param indexName				name of secondary index
	 * @param binName				bin name that data is indexed on
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param ctx					optional context to index on elements within a CDT
	 * @return task that can be polled for index build completion
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
		throw AerospikeException.resultCodeToException(code, "Create index failed: " + response);
    }

	/**
	 * Create an expression-based secondary index with the provided index collection type
	 * This asynchronous server call will return before command is complete.
	 * The user can optionally wait for command completion by using the returned
	 * IndexTask instance.
	 *
     * <p>Example:</p>
     * <pre>{@code
     * // IF (age >= 18 AND country IN ["Australia, "Canada", "USA"])
     * Expression exp = Exp.build(
     *     Exp.cond(
	 *         Exp.and(
	 *             // Is the age 18 or older?
     *             Exp.ge(Exp.intBin("age"), Exp.val(18)),
     *             // Do they live in a target country?
     *             Exp.or(
     *                 Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(0))),
     *                 Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(1))),
     *                 Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(2)))
     *             )
     *         ),
     *         Exp.val(1),
     *         Exp.unknown()
     *     )
     * );
     *
     * session.createIndex(dataSet, indexName, IndexType.INTEGER, IndexCollectionType.DEFAULT, exp)
	 *     .waitTillComplete();
     * }</pre>
	 *
	 * @param set					dataSet containing namespace and set information
	 * @param indexName				name of secondary index
	 * @param indexType				underlying data type of secondary index
	 * @param indexCollectionType	index collection type
	 * @param exp					expression on which to build the index
	 * @return task that can be polled for index build completion
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
		throw AerospikeException.resultCodeToException(code, "Create index failed: " + response);
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

		String createIndexCommand =
			currentServerVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1)?
				"sindex-create:namespace=": "sindex-create:ns=";

		String indexTypeString = (indexType == IndexType.INTEGER &&
			currentServerVersion.isLessThan(Version.SERVER_VERSION_8_1_3))?
				"NUMERIC" : indexType.toString();

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
			sb.append(indexTypeString);
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
				sb.append(indexTypeString);
			} else {
				sb.append(";indexdata=");
				sb.append(binName);
				sb.append(',');
				sb.append(indexTypeString);
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
     * <p>Example:</p>
     * <pre>{@code
     * session.dropIndex(dataSet, indexName);
     * }</pre>
     *
	 * @param set					dataset (namespace and optional set) the index belongs to
	 * @param indexName				name of the secondary index to drop
	 * @return task that can be polled for drop completion
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
		throw AerospikeException.resultCodeToException(code, "Drop index failed: " + response);
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
