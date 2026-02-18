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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodeList;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.OperateArgs;
import com.aerospike.client.fluent.command.OperateReadCommand;
import com.aerospike.client.fluent.command.OperateReadExecutor;
import com.aerospike.client.fluent.command.OperateWriteCommand;
import com.aerospike.client.fluent.command.OperateWriteExecutor;
import com.aerospike.client.fluent.command.ReadAttr;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;
import com.aerospike.client.fluent.tend.Partitions;

public class OperationBuilder extends AbstractOperationBuilder<OperationBuilder> implements FilterableOperation<OperationBuilder>, BinsValuesOperations {
    private final List<Key> keys;
    protected int generation = 0;
    protected long expirationInSecondsForAll = 0;
    protected Txn txnToUse;
    private String namespace;
    private Partitions partitions;
    private Settings settings;

    /**
     * The threshold for determining when to use batch operations vs individual operations.
     * Operations with item counts >= this threshold will use batch mode.
     * Operations with item counts < this threshold will use individual parallel execution.
     */
    public static final int BATCH_OPERATION_THRESHOLD = 10;

    /**
     * TTL constant: Record never expires (TTL = -1)
     */
    public static final int TTL_NEVER_EXPIRE = -1;

    /**
     * TTL constant: Do not change the current TTL of the record (TTL = -2)
     */
    public static final int TTL_NO_CHANGE = -2;

    /**
     * TTL constant: Use the server's default TTL for the namespace (TTL = 0)
     */
    public static final int TTL_SERVER_DEFAULT = 0;

    /**
     * Returns the threshold for determining when to use batch operations vs individual operations.
     * Operations with item counts >= this threshold will use batch mode.
     * Operations with item counts < this threshold will use individual parallel execution.
     *
     * @return the batch operation threshold
     */
    public static int getBatchOperationThreshold() {
        return BATCH_OPERATION_THRESHOLD;
    }

    public static boolean areOperationsRetryable(List<Operation> ops) {
        for (Operation op : ops) {
            switch (op.type) {
            case ADD:
            case APPEND:
            case PREPEND:
                // Definitely not retryable
                return false;

            case BIT_MODIFY:
            case CDT_MODIFY:
            case EXP_MODIFY:
            case HLL_MODIFY:
            case MAP_MODIFY:
                // These are questionable. For example, MAP_MODIFY could be CLEAR (retryable) or DECREMENT (non-retryable)
                // For now return as not retryable, but will need further information in API v2
                // TODO: add the need for determination into new spec
                return false;

            case BIT_READ:
            case CDT_READ:
            case DELETE:
            case EXP_READ:
            case HLL_READ:
            case MAP_READ:
            case READ:
            case READ_HEADER:
            case TOUCH:
            case WRITE:
//            default:
                // definitely retryable
            }
        }
        return true;
    }

    public OperationBuilder(Session session, Key key, OpType type) {
        super(session, type);
        this.keys = List.of(key);
        this.txnToUse = session.getCurrentTransaction();
    }

    public OperationBuilder(Session session, List<Key> keys, OpType type) {
        super(session, type);
        this.keys = keys;
        this.txnToUse = session.getCurrentTransaction();
    }

    // Covariant return type overrides for method chaining
    @Override
    public OperationBuilder expireRecordAfter(Duration duration) {
        super.expireRecordAfter(duration);
        return this;
    }

    @Override
    public OperationBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        super.expireRecordAfterSeconds(expirationInSeconds);
        return this;
    }

    @Override
    public OperationBuilder expireRecordAt(Date date) {
        super.expireRecordAt(date);
        return this;
    }

    @Override
    public OperationBuilder expireRecordAt(LocalDateTime date) {
        super.expireRecordAt(date);
        return this;
    }

    @Override
    public OperationBuilder withNoChangeInExpiration() {
        super.withNoChangeInExpiration();
        return this;
    }

    @Override
    public OperationBuilder neverExpire() {
        super.neverExpire();
        return this;
    }

    @Override
    public OperationBuilder expiryFromServerDefault() {
        super.expiryFromServerDefault();
        return this;
    }

    public BinsValuesBuilder bins(String binName, String... binNames) {
        return new BinsValuesBuilder(this, keys, binName, binNames);
    }

    public OperationBuilder ensureGenerationIs(int generation) {
        if (generation <= 0) {
            throw new IllegalArgumentException("Generation must be greater than 0");
        }
        this.generation = generation;
        return this;
    }

    /**
     * Set the expiration for all records in this operation relative to the current time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param duration The duration after which all records should expire
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationBuilder expireAllRecordsAfter(Duration duration) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAfter() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = duration.getSeconds();
        return this;
    }

    /**
     * Set the expiration for all records in this operation relative to the current time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param seconds The number of seconds after which all records should expire
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationBuilder expireAllRecordsAfterSeconds(long seconds) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAfterSeconds() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = seconds;
        return this;
    }

    /**
     * Set the expiration for all records in this operation to an absolute date/time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param dateTime The date/time at which all records should expire
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public OperationBuilder expireAllRecordsAt(LocalDateTime dateTime) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the expiration for all records in this operation to an absolute date/time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param date The date at which all records should expire
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public OperationBuilder expireAllRecordsAt(Date date) {
        if (!isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set all records to never expire (TTL = -1).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationBuilder neverExpireAllRecords() {
        if (!isMultiKey()) {
            throw new IllegalStateException("neverExpireAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Do not change the expiration of any records (TTL = -2).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationBuilder withNoChangeInExpirationForAllRecords() {
        if (!isMultiKey()) {
            throw new IllegalStateException("withNoChangeInExpirationForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = TTL_NO_CHANGE;
        return this;
    }

    /**
     * Use the server's default expiration for all records (TTL = 0).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This OperationBuilder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public OperationBuilder expiryFromServerDefaultForAllRecords() {
        if (!isMultiKey()) {
            throw new IllegalStateException("expiryFromServerDefaultForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = TTL_SERVER_DEFAULT;
        return this;
    }

    public Txn getTxnToUse() {
        return this.txnToUse;
    }

    /**
     * Specify that these operations are not to be included in any transaction, even if a
     * transaction exists on the underlying session
     */
    public OperationBuilder notInAnyTransaction() {
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
     *     txnSession.insertInto(customerDataSet.id(3));
     *     txnSession.delete(customerDataSet.id(3));
     * });
     * </pre>
     *
     * This method should only be used in situations where different parts of a transaction are not all
     * within the same context, for example forming a transaction on callbacks from a file system.
     * @param txn - the transaction to use
     */
    public OperationBuilder inTransaction(Txn txn) {
        this.txnToUse = txn;
        return this;
    }

    public int getNumKeys() {
        return keys.size();
    }

    public boolean isMultiKey() {
        return keys.size() > 1;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the effective respondAllKeys value. UPDATE and REPLACE_IF_EXISTS
     * operations always return true because they must report KEY_NOT_FOUND_ERROR
     * when the record doesn't exist.</p>
     */
    @Override
    public boolean isRespondAllKeys() {
        return isEffectiveRespondAllKeys();
    }

    public int getExpirationAsInt(long expirationInSeconds) {
        if (expirationInSeconds > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        else {
            return (int) expirationInSeconds;
        }
    }

    @Override
    protected int getExpirationAsInt() {
        long effectiveExpiration = (expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll;
        return super.getExpirationAsInt(effectiveExpiration);
    }

    @Override
    public OperationBuilder where(String dsl, Object ... params) {
        setWhereClause(createWhereClauseProcessor(false, dsl, params));
        return this;
    }

    @Override
    public OperationBuilder where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

    @Override
    public OperationBuilder where(PreparedDsl dsl, Object ... params) {
        setWhereClause(WhereClauseProcessor.from(false, dsl, params));
        return this;
    }

    @Override
    public OperationBuilder where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    @Override
    public OperationBuilder where(Expression e) {
        setWhereClause(WhereClauseProcessor.from(e));
        return this;
    }

    @Override
    public OperationBuilder failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    @Override
    public boolean isFailOnFilteredOut() {
        return this.failOnFilteredOut;
    }

    @Override
    public OperationBuilder respondAllKeys() {
        this.respondAllKeys = true;
        return this;
    }

    public Session getSession() {
        return this.session;
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
            Log.debug("OperationBuilder.executeSync() called for " + keys.size() + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        if (keys.size() == 0) {
            return new RecordStream();
        }

    	OperateArgs args = new OperateArgs(ops);

        // Use batch operations if 10 or more keys
        if (keys.size() >= getBatchOperationThreshold()) {
            return executeBatchSync(args);
        }
        else {
            return executeIndividualSync(args);
        }
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
            Log.debug("OperationBuilder.executeAsync() called for " + keys.size() + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        if (keys.size() == 0) {
            return new RecordStream();
        }

        if (txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using executeSync() or execute() for transactional safety."
            );
        }

    	OperateArgs args = new OperateArgs(ops);

        // Use batch operations if 10 or more keys
        if (keys.size() >= getBatchOperationThreshold()) {
            return executeBatchAsync(args);
        }
        else {
            return executeIndividualAsync(args);
        }
    }

    private RecordStream executeBatchSync(OperateArgs args) {
    	BatchCommand parent = prepareBatch(args);
        Cluster cluster = session.getCluster();
        BatchStatus status = new BatchStatus(true);
        List<BatchRecord> records = parent.getRecords();
        List<BatchNode> bns = BatchNodeList.generate(cluster, partitions,
        	settings.getReplicaOrder(), records, status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord rec = records.get(i);

                if (args.hasWrite) {
	                BatchWrite bw = (BatchWrite)rec;

	                commands[count++] = new BatchSingle.OperateRecordSync(cluster, parent, bw,
	                	status, bn.node);
                }
                else {
	                BatchRead br = (BatchRead)rec;

					commands[count++] = new BatchSingle.ReadRecordSync(cluster, parent, br, status,
						bn.node);
                }
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, records, status);
            }
        }

	    if (txnToUse != null) {
	    	prepareBatchTransaction(args);
	    }

        BatchExecutor.execute(cluster, commands, status);

        // Convert BatchRecord to RecordResult with proper filtering and stack trace handling
        AsyncRecordStream recordStream = new AsyncRecordStream(records.size());

        try {
            for (int i = 0; i < records.size(); i++) {
                BatchRecord br = records.get(i);

                if (shouldIncludeResult(br.resultCode)) {
                    recordStream.publish(createRecordResultFromBatchRecord(br, settings, i));
                }
            }
            return new RecordStream(recordStream);
        }
        finally {
            recordStream.complete();
        }
    }

    private RecordStream executeBatchAsync(OperateArgs args) {
    	BatchCommand parent = prepareBatch(args);
        Cluster cluster = session.getCluster();
        BatchStatus status = new BatchStatus(true);
        List<BatchRecord> records = parent.getRecords();
        List<BatchNode> bns = BatchNodeList.generate(cluster, partitions,
        	settings.getReplicaOrder(), records, status);

        AsyncRecordStream stream = new AsyncRecordStream(keys.size());
        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord rec = records.get(i);

                if (args.hasWrite) {
	                BatchWrite bw = (BatchWrite)rec;

	                commands[count++] = new BatchSingle.OperateRecordAsync(cluster, parent, bw,
	                	status, bn.node, stream, i);
                }
                else {
	                BatchRead br = (BatchRead)rec;

					commands[count++] = new BatchSingle.ReadRecordAsync(cluster, parent, br, status,
						bn.node, stream, i);
                }
            }
            else {
                commands[count++] = new Batch.OperateListAsync(cluster, parent, bn, records, stream,
                	status);
            }
        }

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                prepareBatchTransaction(args);
                operateBatchAsync(cluster, commands, status, stream);
            });
        }
        else {
            operateBatchAsync(cluster, commands, status, stream);
        }

        return new RecordStream(stream);
    }

    private BatchCommand prepareBatch(OperateArgs args) {
    	Cluster cluster = session.getCluster();

		// Assume all keys have the same namespace.
		Key firstKey = keys.get(0);

		namespace = firstKey.namespace;
		partitions = getPartitions(cluster, namespace);
		settings = getSettings(partitions, args, OpShape.BATCH);

		final Expression filterExp = processWhereClause(firstKey.namespace, session);

		BatchAttr attr = new BatchAttr();
		int ttl = getExpirationAsInt();
		List<BatchRecord> batchRecords = new ArrayList<>(keys.size());
		BatchCommand parent;

		if (args.hasWrite) {
		    attr.setWrite(settings, opType);

		    for (Key key : keys) {
		        batchRecords.add(new BatchWrite(key, null, attr, opType, ops, generation, ttl));
		    }

		    // Use effective value: UPDATE/REPLACE_IF_EXISTS must always get server responses for non-existent keys
	        parent = new BatchCommand(cluster, partitions, txnToUse, namespace,
	            batchRecords, filterExp, isEffectiveRespondAllKeys(), false, settings);
	    }
	    else {
	        attr.setRead(settings, partitions.scMode);

	        for (Key key : keys) {
	            batchRecords.add(new BatchRead(key, null, attr, ttl, ops));
	        }

	        // Use effective value: UPDATE/REPLACE_IF_EXISTS must always get server responses for non-existent keys
	        parent = new BatchCommand(cluster, partitions, txnToUse, namespace,
	        	batchRecords, filterExp, isEffectiveRespondAllKeys(), attr.linearize, settings);
	    }

        return parent;
    }

    private void prepareBatchTransaction(OperateArgs args) {
        if (args.hasWrite) {
        	TxnMonitor.addKeys(txnToUse, session.getCluster(), partitions, settings, keys);
        }
        else {
        	txnToUse.prepareRead(namespace);
        }
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
     * Execute operations synchronously for individual keys (< batch threshold). All
     * virtual threads are joined before returning.
     */
    private RecordStream executeIndividualSync(OperateArgs args) {
        Cluster cluster = session.getCluster();

        // Assume all keys have the same namespace.
        Key firstKey = keys.get(0);
        Partitions partitions = getPartitions(cluster, firstKey.namespace);
        Settings settings = getSettings(partitions, args, OpShape.POINT);
        final Expression filterExp = processWhereClause(firstKey.namespace, session);
    	int ttl = getExpirationAsInt();

        if (txnToUse != null) {
            if (args.hasWrite) {
            	TxnMonitor.addKeys(txnToUse, cluster, partitions, settings, keys);
            }
            else {
            	txnToUse.prepareRead(firstKey.namespace);
            }
        }

        if (keys.size() == 1) {
            try {
                Record rec = operate(cluster, partitions, args, settings, filterExp, firstKey);
                // Use effective value: UPDATE/REPLACE_IF_EXISTS must report null records
                if (isEffectiveRespondAllKeys() || rec != null) {
                    return new RecordStream(firstKey, rec);
                }
            }
            catch (AerospikeException ae) {
                if (shouldPublishException(ae)) {
                    if (ae.getResultCode() != ResultCode.FILTERED_OUT) {
                        showWarningsOnException(ae, txnToUse, firstKey, ttl);
                    }
                    return new RecordStream(new RecordResult(firstKey, ae, 0));
                }
            }
            return new RecordStream();

        } else {
            // Run multiple keys in parallel and join.
        	@SuppressWarnings("resource")
			AsyncRecordStream stream = new AsyncRecordStream(keys.size());
            try (ExecutorService es = cluster.getExecutorService()) {
                for (int i = 0; i < keys.size(); i++) {
                    final Key key = keys.get(i);
                    final int idx = i;

                    es.submit(() -> {
                        try {
                            Record record = operate(cluster, partitions, args, settings, filterExp, key);
                            // Use effective value: UPDATE/REPLACE_IF_EXISTS must report null records
                            if (isEffectiveRespondAllKeys() || record != null) {
                                stream.publish(new RecordResult(key, record, idx));
                            }
                        } catch (AerospikeException ae) {
                            if (shouldPublishException(ae)) {
                                if (ae.getResultCode() != ResultCode.FILTERED_OUT) {
                                    showWarningsOnException(ae, txnToUse, key, ttl);
                                }
                                stream.publish(new RecordResult(key, ae, idx));
                            }
                        }

                    });
                }
            }
            return new RecordStream(stream.complete());
        }
    }

    /**
     * Execute operations asynchronously for individual keys (< batch threshold).
     * Returns immediately; virtual threads complete in background.
     */
    private RecordStream executeIndividualAsync(OperateArgs args) {
        Cluster cluster = session.getCluster();

        // Assume all keys have the same namespace.
        Key firstKey = keys.get(0);
        Partitions partitions = getPartitions(cluster, firstKey.namespace);
        Settings settings = getSettings(partitions, args, OpShape.POINT);
        final Expression filterExp = processWhereClause(firstKey.namespace, session);

        AsyncRecordStream asyncStream = new AsyncRecordStream(keys.size());

        if (txnToUse != null) {
            if (args.hasWrite) {
                cluster.startVirtualThread(() -> {
                    TxnMonitor.addKeys(txnToUse, cluster, partitions, settings, keys);
                    operateKeysAsync(cluster, partitions, args, settings, filterExp,
                    	asyncStream);
                });
            }
            else {
            	txnToUse.prepareRead(firstKey.namespace);
                operateKeysAsync(cluster, partitions, args, settings, filterExp, asyncStream);
            }
        }
        else {
            operateKeysAsync(cluster, partitions, args, settings, filterExp, asyncStream);
        }

        return new RecordStream(asyncStream);
    }

    private void operateKeysAsync(
    	Cluster cluster, Partitions partitions, OperateArgs args, Settings settings,
    	Expression filterExp, AsyncRecordStream asyncStream
    ) {
        AtomicInteger pendingOps = new AtomicInteger(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            final int index = i;
            cluster.startVirtualThread(() -> {
                try {
                    Record rec = operate(cluster, partitions, args, settings, filterExp, key);

                    // Use effective value: UPDATE/REPLACE_IF_EXISTS must report null records
                    if (isEffectiveRespondAllKeys() || rec != null) {
                        asyncStream.publish(new RecordResult(key, rec, index));
                    }
                } catch (AerospikeException ae) {
                    if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                        // Use effective value: UPDATE/REPLACE_IF_EXISTS must report filtered-out records
                        if (failOnFilteredOut || isEffectiveRespondAllKeys()) {
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

    private Record operate(
    	Cluster cluster, Partitions partitions, OperateArgs args, Settings settings,
    	Expression filterExp, Key key
    ) {
    	if (args.hasWrite) {
        	int ttl = getExpirationAsInt();

            OperateWriteCommand cmd = new OperateWriteCommand(cluster, partitions, txnToUse, key,
            	ops, args, opType, generation, ttl, filterExp,
            	failOnFilteredOut, settings);

            try {
                OperateWriteExecutor exec = new OperateWriteExecutor(cluster, cmd);
                exec.execute();
                return exec.getRecord();
            }
            catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                    throw ae;
                }
                else {
                    showWarningsOnExceptionAndThrow(ae, txnToUse, key, ttl);
                    return null;
                }
            }
    	}
    	else {
    		ReadAttr attr = new ReadAttr(partitions, settings);
            OperateReadCommand cmd = new OperateReadCommand(cluster, partitions, txnToUse, key,
            	ops, args, filterExp, failOnFilteredOut, settings, attr);

            try {
                OperateReadExecutor exec = new OperateReadExecutor(cluster, cmd);
                exec.execute();
                return exec.getRecord();
            }
            catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                    throw ae;
                }
                else {
                    showWarningsOnExceptionAndThrow(ae, txnToUse, key, 0);
                    return null;
                }
            }
    	}
    }

    private Partitions getPartitions(Cluster cluster, String namespace) {
        HashMap<String, Partitions> partitionMap = cluster.getPartitionMap();
        Partitions partitions = partitionMap.get(namespace);

        if (partitions == null) {
            throw new AerospikeException.InvalidNamespace(namespace, partitionMap.size());
        }
        return partitions;
    }

    private Settings getSettings(
    	Partitions partitions, OperateArgs args, OpShape shape
    ) {
    	OpKind kind;

    	if (args.hasWrite) {
    		kind = OperationBuilder.areOperationsRetryable(ops)?
    			OpKind.WRITE_RETRYABLE : OpKind.WRITE_NON_RETRYABLE;
    	}
    	else {
    		kind = OpKind.READ;
    	}

        return session.getBehavior().getSettings(kind, shape, partitions.scMode);
    }

    public void showWarningsOnExceptionAndThrow(AerospikeException ae, Txn txn, Key key, int expiration) {
        showWarningsOnException(ae, txn, key, expiration);
        throw ae;
    }

    @Override
    public OpType getOpType() {
        return this.opType;
    }

    public void showWarningsOnException(AerospikeException ae, Txn txn, Key key, int expiration) {
        if (Log.warnEnabled()) {
            if (ae.getResultCode() == ResultCode.FAIL_FORBIDDEN && expiration > 0) {
                Log.warn("Operation failed on server with FAIL_FORBIDDEN (22) and the record had "
                        + "an expiry set in the operation. This is possibly caused by nsup being disabled. "
                        + "See https://aerospike.com/docs/database/reference/error-codes for more information");
            }
            if (ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                if (txn != null && !session.isNamespaceSC(key.namespace)) {
                    Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                            + "This will throw an Unsupported Server Feature exception", key.namespace));
                }
            }
        }
    }
}
