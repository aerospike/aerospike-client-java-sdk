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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodes;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.OperateArgs;
import com.aerospike.client.fluent.command.OperateWriteCommand;
import com.aerospike.client.fluent.command.OperateWriteExecutor;
import com.aerospike.client.fluent.command.ReadAttr;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.dsl.ParseResult;

@SuppressWarnings("unused")
public class ObjectBuilder<T> {
    private final OperationObjectBuilder<T> opBuilder;
    private final List<T> elements;
	private RecordMapper<T> recordMapper;
	private int generation = 0;
    private long expirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;
    private long defaultExpirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;
    private Txn txnToUse;
    private Settings settings;

    /**
     * Constructs a new ObjectBuilder for operating on multiple objects.
     *
     * @param opBuilder the operation builder that created this ObjectBuilder
     * @param elements the list of objects to operate on
     */
    public ObjectBuilder(OperationObjectBuilder<T> opBuilder, List<T> elements) {
        this.opBuilder = opBuilder;
        this.elements = elements;
        this.txnToUse = opBuilder.getSession().getCurrentTransaction();
    }

    /**
     * Constructs a new ObjectBuilder for operating on a single object.
     *
     * @param opBuilder the operation builder that created this ObjectBuilder
     * @param element the object to operate on
     */
    public ObjectBuilder(OperationObjectBuilder<T> opBuilder, T element) {
        this.opBuilder = opBuilder;
        this.elements = List.of(element);
        this.txnToUse = opBuilder.getSession().getCurrentTransaction();
    }

    /**
     * Specifies a custom record mapper to use for converting objects to and from Aerospike records.
     * <p>
     * By default, the mapper is obtained from the {@link RecordMappingFactory} configured on the
     * session's cluster. This method allows you to override that default mapper for this specific
     * operation.
     * <p>
     * The mapper is responsible for:
     * <ul>
     *   <li>Converting objects to a map of bin names and values for storage</li>
     *   <li>Extracting the object's ID for key generation</li>
     * </ul>
     *
     * @param recordMapper the record mapper to use for this operation
     * @return This ObjectBuilder for method chaining
     * @throws NullPointerException if recordMapper is null
     * @see RecordMapper
     * @see RecordMappingFactory
     */
    public ObjectBuilder<T> using(RecordMapper<T> recordMapper) {
        if (recordMapper == null) {
            throw new NullPointerException("recordMapper parameter to 'using' call cannot be 'null'");
        }
        this.recordMapper = recordMapper;
        return this;
    }

    /**
     * Ensure the operation only succeeds if the record generation matches.
     * <p>
     * Generation is a version number that Aerospike increments each time a record is modified.
     * By specifying a generation value, you can ensure that the operation only proceeds if the
     * record has not been modified by another client since you last read it. This provides
     * optimistic concurrency control.
     * <p>
     * If the record's current generation does not match the specified value, the operation
     * will fail with a generation error.
     *
     * @param generation the expected generation value
     * @return This ObjectBuilder for method chaining
     * @throws IllegalArgumentException if generation is <= 0
     */
    public ObjectBuilder<T> ensureGenerationIs(int generation) {
        if (generation <= 0) {
            throw new IllegalArgumentException("Generation must be greater than 0");
        }
        this.generation = generation;
        return this;
    }

    /**
     * Set the expiration for records relative to the current time.
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @param duration The duration after which records should expire
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> expireRecordAfter(Duration duration) {
        this.expirationInSeconds = duration.toSeconds();
        return this;
    }

    /**
     * Set the expiration for records relative to the current time.
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @param expirationInSeconds The number of seconds after which records should expire
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> expireRecordAfterSeconds(int expirationInSeconds) {
        this.expirationInSeconds = expirationInSeconds;
        return this;
    }

    private long getExpirationInSecondsAndCheckValue(Date date) {
        long expirationInSeconds = (date.getTime() - new Date().getTime())/ 1000L;
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    /**
     * Set the expiration for records to an absolute date/time.
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @param date The date at which records should expire
     * @return This ObjectBuilder for method chaining
     * @throws IllegalArgumentException if the date is in the past
     */
    public ObjectBuilder<T> expireRecordAt(Date date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    private long getExpirationInSecondsAndCheckValue(LocalDateTime date) {
        LocalDateTime now = LocalDateTime.now();
        long expirationInSeconds = ChronoUnit.SECONDS.between(now, date);
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    /**
     * Resolve the TTL from individual and default expiration settings.
     * Resolution order: individual > default > server default (0).
     */
    private static long resolveTtl(long expirationInSeconds, long defaultExpirationInSeconds) {
        if (expirationInSeconds != AbstractOperationBuilder.NOT_EXPLICITLY_SET) {
            return expirationInSeconds;
        }
        if (defaultExpirationInSeconds != AbstractOperationBuilder.NOT_EXPLICITLY_SET) {
            return defaultExpirationInSeconds;
        }
        return AbstractOperationBuilder.TTL_SERVER_DEFAULT;
    }

    /**
     * Set the expiration for records to an absolute date/time.
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @param date The date/time at which records should expire
     * @return This ObjectBuilder for method chaining
     * @throws IllegalArgumentException if the date is in the past
     */
    public ObjectBuilder<T> expireRecordAt(LocalDateTime date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Do not change the expiration of records (TTL = -2).
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> withNoChangeInExpiration() {
        this.expirationInSeconds = AbstractOperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set records to never expire (TTL = -1).
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> neverExpire() {
        this.expirationInSeconds = AbstractOperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Use the server's default expiration for records (TTL = 0).
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> expiryFromServerDefault() {
        this.expirationInSeconds = AbstractOperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    /**
     * Set the expiration for all objects in this operation relative to the current time.
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @param duration The duration after which all records should expire
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     */
    public ObjectBuilder<T> defaultExpireRecordAfter(Duration duration) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultExpireRecordAfter() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = duration.getSeconds();
        return this;
    }

    /**
     * Set the expiration for all objects in this operation relative to the current time.
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @param seconds The number of seconds after which all records should expire
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     */
    public ObjectBuilder<T> defaultExpireRecordAfterSeconds(long seconds) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultExpireRecordAfterSeconds() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = seconds;
        return this;
    }

    /**
     * Set the expiration for all objects in this operation to an absolute date/time.
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @param dateTime The date/time at which all records should expire
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public ObjectBuilder<T> defaultExpireRecordAt(LocalDateTime dateTime) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultExpireRecordAt() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the expiration for all objects in this operation to an absolute date/time.
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @param date The date at which all records should expire
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public ObjectBuilder<T> defaultExpireRecordAt(Date date) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultExpireRecordAt() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set all records to never expire (TTL = -1).
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     */
    public ObjectBuilder<T> defaultNeverExpire() {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultNeverExpire() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Do not change the expiration of any records (TTL = -2).
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     */
    public ObjectBuilder<T> defaultNoChangeInExpiration() {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultNoChangeInExpiration() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Use the server's default expiration for all records (TTL = 0).
     * This applies to all objects unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple objects are specified.
     *
     * @return This ObjectBuilder for method chaining
     * @throws IllegalStateException if called when only a single object is specified
     */
    public ObjectBuilder<T> defaultExpiryFromServerDefault() {
        if (elements.size() <= 1) {
            throw new IllegalStateException("defaultExpiryFromServerDefault() is only available when multiple objects are specified");
        }
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    /**
     * Specify that these operations are not to be included in any transaction, even if a
     * transaction exists on the underlying session.
     *
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> notInAnyTransaction() {
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
     *     txnSession.update(customerDataSet).objects(customer1, customer2).execute();
     * });
     * </pre>
     *
     * This method should only be used in situations where different parts of a transaction are not all
     * within the same context, for example forming a transaction on callbacks from a file system.
     *
     * @param txn The transaction to use
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> inTransaction(Txn txn) {
        this.txnToUse = txn;
        return this;
    }

    private int getExpirationAsInt(long expirationInSeconds) {
        if (expirationInSeconds > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        else {
            return (int) expirationInSeconds;
        }
    }

    /**
     * Get the record mapper for the given element. This could be from either an
     * explicitly set mapper or from the mapper factory on the connection.
     * @param element
     * @return
     */
    private RecordMapper<T> getMapper(T element) {
        if (this.recordMapper != null) {
            return this.recordMapper;
        }
        else {
            RecordMappingFactory factory = opBuilder.getSession().getRecordMappingFactory();
            if (factory != null) {
                @SuppressWarnings("unchecked")
				RecordMapper<T> mapper = (RecordMapper<T>)factory.getMapper(element.getClass());
                if (mapper != null) {
                    return mapper;
                }
            }
        }
        throw new UnsupportedOperationException(String.format(
                "Could not find a mapper to convert objects of type %s. Did you specify a RcordMappingFactory on the connection?",
                element.getClass().getName()));
    }

    private List<Operation> operationsForElement(RecordMapper<T> mapper, T element) {
        Map<String, Value> map = mapper.toMap(element);
        List<Operation> ops = new ArrayList<>(map.size());

        for (String key : map.keySet()) {
            Value binData = map.get(key);
            ops.add(Operation.put(new Bin(key, binData)));
        }
        return ops;
    }

    private Key getKeyForElement(RecordMapper<T> mapper, T element) {
        Object id = mapper.id(element);
        return this.opBuilder.getDataSet().idForObject(id);
    }

    // ========================================
    // Chaining methods — bridge to key/bin-based builders
    // ========================================

    /**
     * Materialize the current object operation(s) into {@link OperationSpec} entries
     * so they can be combined with key/bin-based operations in a single batch.
     * Each object becomes its own OperationSpec because each maps to a distinct key
     * with distinct bin values.
     */
    private List<OperationSpec> materializeToSpecs() {
        long effectiveExpiration = resolveTtl(expirationInSeconds, defaultExpirationInSeconds);
        List<OperationSpec> specs = new ArrayList<>(elements.size());
        for (T element : elements) {
            RecordMapper<T> mapper = getMapper(element);
            Key key = getKeyForElement(mapper, element);
            List<Operation> ops = operationsForElement(mapper, element);
            OperationSpec spec = new OperationSpec(List.of(key), opBuilder.getOpType());
            spec.getOperations().addAll(ops);
            if (generation > 0) {
                spec.setGeneration(generation);
            }
            if (effectiveExpiration != AbstractOperationBuilder.NOT_EXPLICITLY_SET) {
                spec.setExpirationInSeconds(effectiveExpiration);
            }
            specs.add(spec);
        }
        return specs;
    }

    /**
     * Chain an insert operation on a single key after this object operation.
     *
     * @param key the key to insert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder insert(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.INSERT, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(key, OpType.INSERT);
    }

    /**
     * Chain an insert operation on multiple keys after this object operation.
     *
     * @param keys the keys to insert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder insert(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.INSERT, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(keys, OpType.INSERT);
    }

    /**
     * Chain an update operation on a single key after this object operation.
     *
     * @param key the key to update
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder update(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.UPDATE, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(key, OpType.UPDATE);
    }

    /**
     * Chain an update operation on multiple keys after this object operation.
     *
     * @param keys the keys to update
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder update(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.UPDATE, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(keys, OpType.UPDATE);
    }

    /**
     * Chain an upsert operation on a single key after this object operation.
     *
     * @param key the key to upsert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder upsert(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.UPSERT, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(key, OpType.UPSERT);
    }

    /**
     * Chain an upsert operation on multiple keys after this object operation.
     *
     * @param keys the keys to upsert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder upsert(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.UPSERT, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(keys, OpType.UPSERT);
    }

    /**
     * Chain a replace operation on a single key after this object operation.
     *
     * @param key the key to replace
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replace(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.REPLACE, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(key, OpType.REPLACE);
    }

    /**
     * Chain a replace operation on multiple keys after this object operation.
     *
     * @param keys the keys to replace
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replace(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableOperationBuilder(opBuilder.getSession(), OpType.REPLACE, specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .init(keys, OpType.REPLACE);
    }

    /**
     * Chain a delete operation on a single key after this object operation.
     *
     * @param key the key to delete
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder delete(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableNoBinsBuilder(opBuilder.getSession(), specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .initDelete(key);
    }

    /**
     * Chain a delete operation on multiple keys after this object operation.
     *
     * @param keys the keys to delete
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder delete(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableNoBinsBuilder(opBuilder.getSession(), specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .initDelete(keys);
    }

    /**
     * Chain an exists check on a single key after this object operation.
     *
     * @param key the key to check
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder exists(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableNoBinsBuilder(opBuilder.getSession(), specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .initExists(key);
    }

    /**
     * Chain an exists check on multiple keys after this object operation.
     *
     * @param keys the keys to check
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder exists(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableNoBinsBuilder(opBuilder.getSession(), specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .initExists(keys);
    }

    /**
     * Chain a query (read) operation on a single key after this object operation.
     *
     * @param key the key to query
     * @return ChainableQueryBuilder for method chaining
     */
    public ChainableQueryBuilder query(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableQueryBuilder(opBuilder.getSession(), specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .initQuery(key);
    }

    /**
     * Chain a query (read) operation on multiple keys after this object operation.
     *
     * @param keys the keys to query
     * @return ChainableQueryBuilder for method chaining
     */
    public ChainableQueryBuilder query(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new ChainableQueryBuilder(opBuilder.getSession(), specs, null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse)
                .initQuery(keys);
    }

    /**
     * Chain a UDF execution on a single key after this object operation.
     *
     * @param key the key to execute the UDF on
     * @return UdfFunctionBuilder requiring function specification
     */
    public UdfFunctionBuilder executeUdf(Key key) {
        List<OperationSpec> specs = materializeToSpecs();
        return new UdfFunctionBuilder(opBuilder.getSession(), List.of(key), specs,
                null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse);
    }

    /**
     * Chain a UDF execution on multiple keys after this object operation.
     *
     * @param keys the keys to execute the UDF on
     * @return UdfFunctionBuilder requiring function specification
     */
    public UdfFunctionBuilder executeUdf(List<Key> keys) {
        List<OperationSpec> specs = materializeToSpecs();
        return new UdfFunctionBuilder(opBuilder.getSession(), keys, specs,
                null, AbstractOperationBuilder.NOT_EXPLICITLY_SET, txnToUse);
    }

    // ========================================
    // Execution
    // ========================================

    /**
     * Execute operations with default behavior (synchronous).
     * All operations complete before this method returns, making it safe for transactions.
     *
     * @return RecordStream containing the results
     */
    public RecordStream execute() {
        return executeSync();
    }

    /**
     * Execute operations synchronously. All operations complete before this method returns.
     * <p>
     * Operations are parallelized using virtual threads, but all threads are joined before
     * returning. This ensures transaction safety and deterministic behavior.
     *
     * @return RecordStream containing the results
     */
    public RecordStream executeSync() {
        if (Log.debugEnabled()) {
            Log.debug("ObjectBuilder.executeSync() called for " + elements.size() + " element(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        if (elements.size() == 0) {
            return new RecordStream();
        }

        if (elements.size() == 1) {
            return executeSingleSync(elements.get(0));
        }

        if (elements.size() < AbstractOperationBuilder.getBatchOperationThreshold()) {
            return executeIndividualSync();
        }

        return executeBatchSync();
    }

    /**
     * Execute operations asynchronously using virtual threads for parallel execution.
     * Method returns immediately; results are consumed via the RecordStream.
     * <p>
     * <b>WARNING:</b> Using this in transactions may lead to operations still being in flight
     * when commit() is called, potentially leading to inconsistent state. A warning will be logged.
     *
     * @return RecordStream that will be populated as results arrive
     */
    public RecordStream executeAsync() {
        if (Log.debugEnabled()) {
            Log.debug("ObjectBuilder.executeAsync() called for " + elements.size() + " element(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        if (this.txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using executeSync() or execute() for transactional safety."
            );
        }

        if (elements.size() == 0) {
            return new RecordStream();
        }

        if (elements.size() == 1) {
            return executeSingleAsync(elements.get(0));
        }

        if (elements.size() < AbstractOperationBuilder.getBatchOperationThreshold()) {
            return executeIndividualAsync();
        }

        return executeBatchAsync();
    }

    /**
     * Execute operations using batch operations (10+ objects).
     */
    private RecordStream executeBatchSync() {
    	BatchCommand parent = prepareBatch();
        List<BatchRecord> records = parent.getRecords();
        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        BatchStatus status = new BatchStatus();

        List<BatchNode> bns = BatchNodes.generate(cluster, parent, records, status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord rec = records.get(i);
                BatchWrite bw = (BatchWrite)rec;

                commands[count++] = new BatchSingle.OperateRecordSync(cluster, parent, bw,
                	status, bn.node);
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, records, status);
            }
        }

        if (txnToUse != null) {
            TxnMonitor.addKeysBatchWrite(txnToUse, session, records);
        }

        BatchExecutor.execute(cluster, commands, status);

        AsyncRecordStream recordStream = new AsyncRecordStream(records.size());

        try {
            for (int i = 0; i < records.size(); i++) {
                BatchRecord br = records.get(i);
                if (opBuilder.shouldIncludeResult(br.resultCode)) {
                    recordStream.publish(AbstractFilterableBuilder.createRecordResultFromBatchRecord(br, settings, i));
                }
            }
            return new RecordStream(recordStream);
        }
        finally {
            recordStream.complete();
        }
    }

    /**
     * Execute operations using async batch operations (10+ objects).
     */
    private RecordStream executeBatchAsync() {
    	BatchCommand parent = prepareBatch();
        List<BatchRecord> records = parent.getRecords();
        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        BatchStatus status = new BatchStatus();

        List<BatchNode> bns = BatchNodes.generate(cluster, parent, records, status);

        AsyncRecordStream stream = new AsyncRecordStream(elements.size());
        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord rec = records.get(i);
                BatchWrite bw = (BatchWrite)rec;

                commands[count++] = new BatchSingle.OperateRecordAsync(cluster, parent, bw,
                	status, bn.node, stream, i);
            }
            else {
                commands[count++] = new Batch.OperateListAsync(cluster, parent, bn, records,
                	stream, status);
            }
        }

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKeysBatchWrite(txnToUse, session, records);
                operateBatchAsync(cluster, commands, status, stream);
            });
        }
        else {
            operateBatchAsync(cluster, commands, status, stream);
        }

        return new RecordStream(stream);
    }

    private BatchCommand prepareBatch() {
        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();

        String namespace = opBuilder.getDataSet().getNamespace();
        Partitions partitions = getPartitions(cluster, namespace);

        settings = session.getBehavior()
        	.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, partitions.scMode);

        long effectiveExpiration = resolveTtl(expirationInSeconds, defaultExpirationInSeconds);
        int ttl = getExpirationAsInt(effectiveExpiration);

        BatchAttr attr = new BatchAttr();
        attr.setWrite(settings, opBuilder.getOpType());

        List<BatchRecord> records = new ArrayList<>(elements.size());

        for (T element : elements) {
            RecordMapper<T> recordMapper = getMapper(element);
            Key key = getKeyForElement(recordMapper, element);
            List<Operation> ops = operationsForElement(recordMapper, element);
            records.add(new BatchWrite(key, null, attr, opBuilder.getOpType(), ops, generation, ttl));
        }

        final Expression filterExp = getFilterExp(namespace);

        return new BatchCommand(cluster, partitions, txnToUse, namespace,
            records, filterExp, opBuilder.respondAllKeys, false, settings);
    }

    private void operateBatchAsync(
    	Cluster cluster, IBatchCommand[] commands, BatchStatus status, AsyncRecordStream stream
    ) {
        AtomicInteger pending = new AtomicInteger(commands.length);

		for (IBatchCommand command : commands) {
            cluster.startVirtualThread(() -> {
            	try {
    				command.run();
            	}
            	finally {
                    if (pending.decrementAndGet() == 0) {
                        stream.complete();
                    }
            	}
	        });
		}
    }

    /**
     * Execute operations synchronously for individual objects (< batch threshold).
     * All virtual threads are joined before returning.
     */
    private RecordStream executeIndividualSync() {
        List<Key> keys = new ArrayList<>(elements.size());

        for (T element : elements) {
            RecordMapper<T> recordMapper = getMapper(element);
            Key key = getKeyForElement(recordMapper, element);
            keys.add(key);
        }

        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        Key firstKey = keys.get(0);
        Partitions partitions = getPartitions(cluster, firstKey.namespace);

    	// Assume all operations are puts (WRITE_RETRYABLE).
        Settings settings = session.getBehavior()
        	.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);

        // Apply where clause if present
        final Expression filterExp = getFilterExp(firstKey.namespace);
        int ttl = (int) resolveTtl(expirationInSeconds, defaultExpirationInSeconds);
        boolean stackTraceOnException = settings.getStackTraceOnException();

        if (txnToUse != null) {
        	// Assume all operations are write operations.
            TxnMonitor.addKeys(txnToUse, session, keys);
        }

		AsyncRecordStream stream = new AsyncRecordStream(elements.size());

        try (ExecutorService es = cluster.getExecutorService()) {
            for (int i = 0; i < keys.size(); i++) {
                final Key key = keys.get(i);
                final T element = elements.get(i);
                final int idx = i;

                es.submit(() -> {
                    try {
                        Record rec = operate(cluster, partitions, settings, filterExp, key, element, ttl);

                        if (opBuilder.respondAllKeys || rec != null) {
                            stream.publish(new RecordResult(key, rec, idx));
                        }
                    } catch (AerospikeException ae) {
                        if (shouldPublish(ae, opBuilder)) {
                            stream.publish(new RecordResult(key, ae, idx));
                        }
                    }
                });
            }
        }
        stream.complete();
        return new RecordStream(stream);
    }

    /**
     * Execute operations asynchronously for individual objects (< batch threshold).
     * Returns immediately; virtual threads complete in background.
     */
    private RecordStream executeIndividualAsync() {
        List<Key> keys = new ArrayList<>(elements.size());

        for (T element : elements) {
            RecordMapper<T> recordMapper = getMapper(element);
            Key key = getKeyForElement(recordMapper, element);
            keys.add(key);
        }

        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        Key firstKey = keys.get(0);
        Partitions partitions = getPartitions(cluster, firstKey.namespace);

    	// Assume all operations are puts (WRITE_RETRYABLE).
        Settings settings = session.getBehavior()
        	.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);

        // Apply where clause if present
        final Expression filterExp = getFilterExp(firstKey.namespace);
        int ttl = (int) resolveTtl(expirationInSeconds, defaultExpirationInSeconds);
        boolean stackTraceOnException = settings.getStackTraceOnException();

        AsyncRecordStream stream = new AsyncRecordStream(elements.size());
        AtomicInteger pendingOps = new AtomicInteger(elements.size());

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKeys(txnToUse, session, keys);
                operateKeysAsync(cluster, partitions, settings, filterExp, ttl, stream, keys);
            });
        }
        else {
            operateKeysAsync(cluster, partitions, settings, filterExp, ttl, stream, keys);
        }

        return new RecordStream(stream);
    }

    private RecordStream executeSingleSync(T element) {
        RecordMapper<T> recordMapper = getMapper(element);
        Key key = getKeyForElement(recordMapper, element);

        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, key.namespace);

    	// Assume all operations are puts (WRITE_RETRYABLE).
        Settings settings = session.getBehavior()
        	.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);

        // Apply where clause if present
        final Expression filterExp = getFilterExp(key.namespace);

        if (txnToUse != null) {
        	// Assume all operations are write operations.
            TxnMonitor.addKey(txnToUse, session, key);
        }

        int ttl = (int) resolveTtl(expirationInSeconds, defaultExpirationInSeconds);
        boolean stackTraceOnException = settings.getStackTraceOnException();

        try {
            Record rec = operate(cluster, partitions, settings, filterExp, key, element, ttl);

            if (opBuilder.respondAllKeys || rec != null) {
                return new RecordStream(key, rec);
            }
        }
        catch (AerospikeException ae) {
            if (shouldPublish(ae, opBuilder)) {
                return new RecordStream(new RecordResult(key, ae, 0));
            }
        }
        return new RecordStream();
    }

    private RecordStream executeSingleAsync(T element) {
        RecordMapper<T> recordMapper = getMapper(element);
        Key key = getKeyForElement(recordMapper, element);

        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        Partitions partitions = getPartitions(cluster, key.namespace);

    	// Assume all operations are puts (WRITE_RETRYABLE).
        Settings settings = session.getBehavior()
        	.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);

        // Apply where clause if present
        final Expression filterExp = getFilterExp(key.namespace);

        int ttl = (int) resolveTtl(expirationInSeconds, defaultExpirationInSeconds);
        boolean stackTraceOnException = settings.getStackTraceOnException();
        AsyncRecordStream stream = new AsyncRecordStream(1);
        AtomicInteger pendingOps = new AtomicInteger(1);

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKey(txnToUse, session, key);
                operateAsync(cluster, partitions, settings, filterExp, key, element, ttl, stream, 0, pendingOps);
            });
        }
        else {
            operateAsync(cluster, partitions, settings, filterExp, key, element, ttl, stream, 0, pendingOps);
        }

        return new RecordStream(stream);
    }

    private void operateKeysAsync(
    	Cluster cluster, Partitions partitions, Settings settings, Expression filterExp, int ttl,
    	AsyncRecordStream stream, List<Key> keys
    ) {
        AtomicInteger pendingOps = new AtomicInteger(elements.size());

        for (int i = 0; i < elements.size(); i++) {
        	T element = elements.get(i);
            Key key = keys.get(i);
            operateAsync(cluster, partitions, settings, filterExp, key, element, ttl, stream, i, pendingOps);
        }
    }

    private void operateAsync(
    	Cluster cluster, Partitions partitions, Settings settings, Expression filterExp, Key key,
    	T element, int ttl, AsyncRecordStream stream, int index, AtomicInteger pendingOps
    ) {
        cluster.startVirtualThread(() -> {
            try {
                Record rec = operate(cluster, partitions, settings, filterExp, key, element, ttl);

                if (opBuilder.respondAllKeys || rec != null) {
                    stream.publish(new RecordResult(key, rec, index));
                }
            }
            catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                    if (opBuilder.failOnFilteredOut || opBuilder.respondAllKeys) {
                        stream.publish(new RecordResult(key, ae, index));
                    }
                    // Otherwise skip this record
                } else {
                    stream.publish(new RecordResult(key, ae, index));
                }
            }
            finally {
                if (pendingOps.decrementAndGet() == 0) {
                    stream.complete();
                }
            }
        });
    }

    private Record operate(
    	Cluster cluster, Partitions partitions, Settings settings, Expression filterExp, Key key,
    	T element, int ttl
    ) {
        RecordMapper<T> recordMapper = getMapper(element);
        List<Operation> ops = operationsForElement(recordMapper, element);

		OperateArgs args = new OperateArgs(ops);
        OperateWriteCommand cmd = new OperateWriteCommand(cluster, partitions, txnToUse, key, ops,
        	args, opBuilder.getOpType(), generation, ttl, filterExp,
        	opBuilder.failOnFilteredOut, settings);

        OperateWriteExecutor exec = new OperateWriteExecutor(cluster, cmd);
        exec.execute();
        return exec.getRecord();
    }

    private Partitions getPartitions(Cluster cluster, String namespace) {
        HashMap<String, Partitions> partitionMap = cluster.getPartitionMap();
        Partitions partitions = partitionMap.get(namespace);

        if (partitions == null) {
            throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
        }
        return partitions;
    }

    private Expression getFilterExp(String namespace) {
        if (opBuilder.getDsl() != null && !elements.isEmpty()) {
            ParseResult parseResult = opBuilder.getDsl().process(namespace, opBuilder.getSession());
            return Exp.build(parseResult.getExp());
        }
        return null;
    }

    private boolean shouldPublish(AerospikeException ae, OperationObjectBuilder<T> opBuilder) {
        return switch (ae.getResultCode()) {
            case ResultCode.FILTERED_OUT ->
                opBuilder.isFailOnFilteredOut() || opBuilder.isRespondAllKeys();
            default -> true;
        };
    }
}
