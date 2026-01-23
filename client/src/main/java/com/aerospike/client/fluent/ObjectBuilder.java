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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.OperateArgs;
import com.aerospike.client.fluent.command.OperateWriteCommand;
import com.aerospike.client.fluent.command.OperateWriteExecutor;
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
    private long expirationInSeconds = 0;
    private long expirationInSecondsForAll = 0;
    private Txn txnToUse;


    public ObjectBuilder(OperationObjectBuilder<T> opBuilder, List<T> elements) {
        this.opBuilder = opBuilder;
        this.elements = elements;
        this.txnToUse = opBuilder.getSession().getCurrentTransaction();
    }

    public ObjectBuilder(OperationObjectBuilder<T> opBuilder, T element) {
        this.opBuilder = opBuilder;
        this.elements = List.of(element);
        this.txnToUse = opBuilder.getSession().getCurrentTransaction();
    }

    public ObjectBuilder<T> using(RecordMapper<T> recordMapper) {
        if (recordMapper == null) {
            throw new NullPointerException("recordMapper parameter to 'using' call cannot be 'null'");
        }
        this.recordMapper = recordMapper;
        return this;
    }

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
        this.expirationInSeconds = OperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set records to never expire (TTL = -1).
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> neverExpire() {
        this.expirationInSeconds = OperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Use the server's default expiration for records (TTL = 0).
     * This applies to all objects unless overridden by "ForAll" expiration settings.
     *
     * @return This ObjectBuilder for method chaining
     */
    public ObjectBuilder<T> expiryFromServerDefault() {
        this.expirationInSeconds = OperationBuilder.TTL_SERVER_DEFAULT;
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
    public ObjectBuilder<T> expireAllRecordsAfter(Duration duration) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("expireAllRecordsAfter() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = duration.getSeconds();
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
    public ObjectBuilder<T> expireAllRecordsAfterSeconds(long seconds) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("expireAllRecordsAfterSeconds() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = seconds;
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
    public ObjectBuilder<T> expireAllRecordsAt(LocalDateTime dateTime) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = getExpirationInSecondsAndCheckValue(dateTime);
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
    public ObjectBuilder<T> expireAllRecordsAt(Date date) {
        if (elements.size() <= 1) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = getExpirationInSecondsAndCheckValue(date);
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
    public ObjectBuilder<T> neverExpireAllRecords() {
        if (elements.size() <= 1) {
            throw new IllegalStateException("neverExpireAllRecords() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NEVER_EXPIRE;
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
    public ObjectBuilder<T> withNoChangeInExpirationForAllRecords() {
        if (elements.size() <= 1) {
            throw new IllegalStateException("withNoChangeInExpirationForAllRecords() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NO_CHANGE;
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
    public ObjectBuilder<T> expiryFromServerDefaultForAllRecords() {
        if (elements.size() <= 1) {
            throw new IllegalStateException("expiryFromServerDefaultForAllRecords() is only available when multiple objects are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_SERVER_DEFAULT;
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
            return executeSingle(elements.get(0));
        }

        if (elements.size() < OperationBuilder.getBatchOperationThreshold()) {
            return executeIndividualSync();
        }

        return executeBatch();
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

        if (elements.size() < OperationBuilder.getBatchOperationThreshold()) {
            return executeIndividualAsync();
        }

        return executeBatch();
    }

    /**
     * Execute operations using batch operations (10+ objects).
     */
    private RecordStream executeBatch() {
    	/*
        List<BatchRecord> batchWrites = new ArrayList<>();
        BatchPolicy batchPolicy = this.opBuilder.getSession().getBehavior().getMutablePolicy(CommandType.BATCH_WRITE);

        // Apply where clause if present
        Expression whereExp = processWhereClauseForElements();
        batchPolicy.failOnFilteredOut = opBuilder.isFailOnFilteredOut();
        batchPolicy.filterExp = whereExp;

        // Apply expiration: use individual expiration if set, otherwise use "ForAll" expiration
        long effectiveExpiration = (expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll;
        int expirationAsInt = getExpirationAsInt(effectiveExpiration);

        AsyncRecordStream recordStream = new AsyncRecordStream(elements.size());
        try {
	        for (T element : elements) {
	            RecordMapper<T> recordMapper = getMapper(element);
	            Key key = getKeyForElement(recordMapper, element);
	            List<Operation> operations = operationsForElement(recordMapper, element);

	            BatchWritePolicy bwp = new BatchWritePolicy();
	            bwp.sendKey = batchPolicy.sendKey;
	            if (generation > 0) {
	                bwp.generation = generation;
	                bwp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
	            }
	            bwp.expiration = expirationAsInt;

	            batchWrites.add(new BatchWrite(bwp, key, operations));
	        }

	        batchPolicy.setTxn(this.txnToUse);

	        this.opBuilder.getSession().getClient().operate(
	                batchPolicy,
	                batchWrites);

            // Convert BatchRecord to RecordResult with proper stack trace handling
            Settings settings = this.opBuilder.getSession().getBehavior()
                    .getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.ANY);

            for (int i = 0; i < batchWrites.size(); i++) {
                BatchRecord br = batchWrites.get(i);
                if (opBuilder.shouldIncludeResult(br.resultCode)) {
                    recordStream.publish(opBuilder.createRecordResultFromBatchRecord(br, settings, i));
                }
            }

            return new RecordStream(recordStream);
        }
        finally {
            recordStream.complete();
        }
        */
    	return null;
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
        int ttl = (int)((expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll);
        boolean stackTraceOnException = settings.getStackTraceOnException();

        if (txnToUse != null) {
        	// Assume all operations are write operations.
            TxnMonitor.addKeys(txnToUse, cluster, partitions, settings, keys);
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
        int ttl = (int)((expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll);
        boolean stackTraceOnException = settings.getStackTraceOnException();

        AsyncRecordStream stream = new AsyncRecordStream(elements.size());
        AtomicInteger pendingOps = new AtomicInteger(elements.size());

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKeys(txnToUse, cluster, partitions, settings, keys);
                operateKeysAsync(cluster, partitions, settings, filterExp, ttl, stream, keys);
            });
        }
        else {
            operateKeysAsync(cluster, partitions, settings, filterExp, ttl, stream, keys);
        }

        return new RecordStream(stream);
    }

    private RecordStream executeSingle(T element) {
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
            TxnMonitor.addKey(txnToUse, cluster, partitions, settings, key);
        }

        int ttl = (int)((expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll);
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

        int ttl = (int)((expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll);
        boolean stackTraceOnException = settings.getStackTraceOnException();
        AsyncRecordStream stream = new AsyncRecordStream(1);
        AtomicInteger pendingOps = new AtomicInteger(1);

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKey(txnToUse, cluster, partitions, settings, key);
                operateAsync(cluster, partitions, settings, filterExp, key, element, ttl, stream, 0, pendingOps);
            });
        }
        else {
            operateAsync(cluster, partitions, settings, filterExp, key, element, ttl, stream, 0, pendingOps);
        }

        return new RecordStream(stream);
    }

/*
    private void operateAsync(
    	Cluster cluster, Partitions partitions, OperateArgs args, Settings settings,
    	Expression filterExp, AsyncRecordStream asyncStream
    ) {
        AtomicInteger pendingOps = new AtomicInteger(elements.size());

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            final int index = i;
            cluster.startVirtualThread(() -> {
                try {
                    Record rec = operate(cluster, partitions, args, settings, filterExp, key);

                    if (respondAllKeys || rec != null) {
                        asyncStream.publish(new RecordResult(key, rec, index));
                    }
                } catch (AerospikeException ae) {
                    if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                        if (failOnFilteredOut || respondAllKeys) {
                            asyncStream.publish(new RecordResult(key, ae, index));
                        }
                        // Otherwise skip this record
                    } else {
                        asyncStream.publish(new RecordResult(key, ae, index));
                    }
                } finally {
                    if (pendingOps.decrementAndGet() == 0) {
                        asyncStream.complete();
                    }
                }
            });
        }
    }
*/

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
