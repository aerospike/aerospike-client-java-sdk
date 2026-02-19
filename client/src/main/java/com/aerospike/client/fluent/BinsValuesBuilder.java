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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodes;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.OperateArgs;
import com.aerospike.client.fluent.command.OperateWriteCommand;
import com.aerospike.client.fluent.command.OperateWriteExecutor;
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
import com.aerospike.dsl.ParseResult;

/**
 * Builder for the bins+values pattern in OperationBuilder. This allows setting
 * multiple bin names and then providing values for each record.
 */
public class BinsValuesBuilder extends AbstractFilterableBuilder implements FilterableOperation<BinsValuesBuilder> {
    private static class ValueData {
        private Object[] values;
        private int generation = 0;
        private long expirationInSeconds = Long.MIN_VALUE;

        public ValueData(Object[] values) {
            this.values = values;
        }
    }

    private long expirationInSecondsForAll = 0;
    private final BinsValuesOperations opBuilder;
    private final String[] binNames;
    private final Map<Key, ValueData> valueSets = new HashMap<>();
    private final List<Key> keys;
    private ValueData current = null;
    private Txn txnToUse;
    private Settings settings;

    /**
     * Constructs a new BinsValuesBuilder for setting multiple bin names and values.
     *
     * @param opBuilder The parent OperationBuilder
     * @param keys The list of keys for which values will be set
     * @param binName The first bin name
     * @param binNames Additional bin names
     */
    public BinsValuesBuilder(BinsValuesOperations opBuilder, List<Key> keys, String binName, String... binNames) {
        this.opBuilder = opBuilder;
        this.binNames = new String[1 + binNames.length];
        this.binNames[0] = binName;
        System.arraycopy(binNames, 0, this.binNames, 1, binNames.length);
        this.keys = keys;
        if (opBuilder instanceof AbstractSessionOperationBuilder) {
            this.txnToUse = ((AbstractSessionOperationBuilder<?>) opBuilder).getTxnToUse();
        }
    }
    /**
     * Add a set of values for one record. The number of values must match the
     * number of bins. Multiple calls to this method can be chained together.
     *
     * @param values The values for this record
     * @return This builder for chaining
     */
    public BinsValuesBuilder values(Object... values) {
        if (values.length != binNames.length) {
            throw new IllegalArgumentException(
                    String.format("When calling '.values(...)' to specify the values for multiple bins,"
                            + " the number of values must match the number of bins specified in the '.bins(...)' call."
                            + " This call specified %d bins, but supplied %d values.", binNames.length, values.length));
        }

        checkRoomToAddAnotherValue();
        current = new ValueData(values);
        valueSets.put(keys.get(valueSets.size()), current);
        return this;
    }

    private void checkValuesExist(String name) {
        if (valueSets.size() == 0) {
            throw new IllegalArgumentException(
                    String.format("%s was called when no values were defined (by calling '.values'). This method"
                            + " sets parameters on the values for that record, so call '.values' before"
                            + " calling this method", name));
        }
    }

    private void checkRoomToAddAnotherValue() {
        if (valueSets.size() >= keys.size()) {
            throw new IllegalArgumentException(String.format(
                    "The number of '.values(...)' must match the number of specified keys (%d), but there are too many .values(...) calls",
                    keys.size()));
        }
    }

    /**
     * Ensure the operation only succeeds if the record generation matches.
     * This applies to the current record (the one most recently set via values()).
     *
     * @param generation the expected generation value
     * @return this builder for method chaining
     * @throws IllegalArgumentException if generation is <= 0 or if values() has not been called
     */
    public BinsValuesBuilder ensureGenerationIs(int generation) {
        if (generation <= 0) {
            throw new IllegalArgumentException("Generation must be greater than 0");
        }
        checkValuesExist("ensureGenerationIs");
        current.generation = generation;
        return this;
    }

    /**
     * Set the expiration for the current record relative to the current time.
     * This applies to the record most recently set via values().
     *
     * @param duration The duration after which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if values() has not been called
     */
    public BinsValuesBuilder expireRecordAfter(Duration duration) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = duration.toSeconds();
        return this;
    }

    /**
     * Set the expiration for the current record relative to the current time.
     * This applies to the record most recently set via values().
     *
     * @param expirationInSeconds The number of seconds after which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if values() has not been called
     */
    public BinsValuesBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = expirationInSeconds;
        return this;
    }

    /**
     * Set the expiration for the current record to an absolute date/time.
     * This applies to the record most recently set via values().
     *
     * @param date The date at which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if the date is in the past or if values() has not been called
     */
    public BinsValuesBuilder expireRecordAt(Date date) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = opBuilder.getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set the expiration for the current record to an absolute date/time.
     * This applies to the record most recently set via values().
     *
     * @param date The LocalDateTime at which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if the date is in the past or if values() has not been called
     */
    public BinsValuesBuilder expireRecordAt(LocalDateTime date) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = opBuilder.getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Do not change the expiration of the current record (TTL = -2).
     * This applies to the record most recently set via values().
     *
     * @return This builder for method chaining
     * @throws IllegalArgumentException if values() has not been called
     */
    public BinsValuesBuilder withNoChangeInExpiration() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = OperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set the current record to never expire (TTL = -1).
     * This applies to the record most recently set via values().
     *
     * @return This builder for method chaining
     * @throws IllegalArgumentException if values() has not been called
     */
    public BinsValuesBuilder neverExpire() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = OperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Use the server's default expiration for the current record (TTL = 0).
     * This applies to the record most recently set via values().
     *
     * @return This builder for method chaining
     * @throws IllegalArgumentException if values() has not been called
     */
    public BinsValuesBuilder expiryFromServerDefault() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = OperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    /**
     * Specify that these operations are not to be included in any transaction, even if a
     * transaction exists on the underlying session.
     *
     * @return This builder for method chaining
     */
    public BinsValuesBuilder notInAnyTransaction() {
        this.txnToUse = null;
        return this;
    }

    /**
     * Specify the transaction to use for this call. Note that this should not be commonly used.
     * A better pattern is to use the {@code doInTransaction} method on {@link Session}:
     * <pre>
     * session.doInTransaction(txnSession -> {
     *     txnSession.insertInto(customerDataSet.id(1))
     *         .bins("name", "age")
     *         .values("John", 30)
     *         .execute();
     * });
     * </pre>
     *
     * This method should only be used in situations where different parts of a transaction are not all
     * within the same context, for example forming a transaction on callbacks from a file system.
     *
     * @param txn The transaction to use
     * @return This builder for method chaining
     */
    public BinsValuesBuilder inTransaction(Txn txn) {
        this.txnToUse = txn;
        return this;
    }

    // Multi-key expiry methods (only available for multiple keys)
    /**
     * Set the expiration for all records in this operation relative to the current time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param duration The duration after which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public BinsValuesBuilder expireAllRecordsAfter(Duration duration) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException(
                    "expireAllRecordsAfter() is only available when multiple keys are specified");
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
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public BinsValuesBuilder expireAllRecordsAfterSeconds(long seconds) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException(
                    "expireAllRecordsAfterSeconds() is only available when multiple keys are specified");
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
     * @param dateTime The LocalDateTime at which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public BinsValuesBuilder expireAllRecordsAt(LocalDateTime dateTime) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = opBuilder.getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the expiration for all records in this operation to an absolute date/time.
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @param date The date at which all records should expire
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     * @throws IllegalArgumentException if the date is in the past
     */
    public BinsValuesBuilder expireAllRecordsAt(Date date) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = opBuilder.getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set all records in this operation to never expire (TTL = -1).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public BinsValuesBuilder neverExpireAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException(
                    "neverExpireAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Do not change the expiration of all records in this operation (TTL = -2).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public BinsValuesBuilder withNoChangeInExpirationForAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException(
                    "withNoChangeInExpirationForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Use the server's default expiration for all records in this operation (TTL = 0).
     * This applies to all keys unless overridden by individual record expiration settings.
     * <p>
     * Note: This method is only available when multiple keys are specified.
     *
     * @return This builder for method chaining
     * @throws IllegalStateException if called when only a single key is specified
     */
    public BinsValuesBuilder expiryFromServerDefaultForAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException(
                    "expiryFromServerDefaultForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    /**
     * Apply a where clause filter to these operations. Only records matching the
     * filter will be affected.
     * <p>
     * The DSL string can contain parameters which are replaced with the passed
     * arguments. For example:
     *
     * <pre>
     * builder.where("$.age > %d", 21)
     * </pre>
     *
     * @param dsl    The DSL string defining the filter condition
     * @param params Optional parameters to be substituted into the DSL string
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(String dsl, Object... params) {
        setWhereClause(createWhereClauseProcessor(false, dsl, params));
        return this;
    }

    /**
     * Apply a where clause filter to these operations using a boolean expression.
     * Only records matching the filter will be affected.
     *
     * @param dsl The boolean expression defining the filter condition
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

    /**
     * Apply a where clause filter to these operations using a prepared DSL. Only
     * records matching the filter will be affected.
     *
     * @param dsl    The prepared DSL defining the filter condition
     * @param params Parameters to be substituted into the prepared DSL
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(PreparedDsl dsl, Object... params) {
        setWhereClause(WhereClauseProcessor.from(false, dsl, params));
        return this;
    }

    /**
     * Apply a where clause filter to these operations using an Aerospike
     * expression. Only records matching the filter will be affected.
     *
     * @param exp The Aerospike expression defining the filter condition
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    /**
     * Adds a where clause filter to the background operation using an Expression operation.
     * The filter determines which records in the set will be affected.
     *
     * @param e The expression to validate the records against
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(Expression e) {
        setWhereClause(WhereClauseProcessor.from(e));
        return this;
    }

    /**
     * If a where clause is specified and a record is filtered out, it will appear
     * in the result stream with an exception code of
     * {@link ResultCode#FILTERED_OUT} rather than being silently omitted from the
     * results.
     *
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    /**
     * By default, if a key does not map to a record (or is filtered out), nothing
     * will be returned in the stream for that key. If this flag is specified, a
     * result will be included in the stream for every key, even if the record
     * doesn't exist or was filtered out.
     *
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder respondAllKeys() {
        this.respondAllKeys = true;
        return this;
    }

    /**
     * Execute operations with default behavior (synchronous). All operations
     * complete before this method returns, making it safe for transactions.
     *
     * @return RecordStream containing the results
     */
    public RecordStream execute() {
        return executeSync();
    }

    /**
     * Execute operations synchronously. All operations complete before this method
     * returns.
     * <p>
     * Operations are parallelized using virtual threads, but all threads are joined
     * before returning. This ensures transaction safety and deterministic behavior.
     *
     * @return RecordStream containing the results
     */
    public RecordStream executeSync() {
        if (Log.debugEnabled()) {
            Log.debug("BinsValuesBuilder.executeSync() called for " + keys.size() + " key(s), transaction: "
                    + (txnToUse != null ? "yes" : "no"));
        }

        if (keys.size() == 0) {
            return new RecordStream();
        }

        if (valueSets.size() != opBuilder.getNumKeys()) {
            throw new IllegalArgumentException(String.format(
                    "The number of '.values(...)' calls (%d) must match the number of specified keys (%d)",
                    valueSets.size(), opBuilder.getNumKeys()));
        }

        if (keys.size() >= OperationBuilder.getBatchOperationThreshold()) {
            return executeBatchSync();
        } else {
            return executeIndividualSync();
        }
    }

    /**
     * Execute operations asynchronously using virtual threads for parallel
     * execution. Method returns immediately; results are consumed via the
     * RecordStream.
     * <p>
     * <b>WARNING:</b> Using this in transactions may lead to operations still being
     * in flight when commit() is called, potentially leading to inconsistent state.
     * A warning will be logged.
     *
     * @return RecordStream that will be populated as results arrive
     */
    public RecordStream executeAsync() {
        if (Log.debugEnabled()) {
            Log.debug("BinsValuesBuilder.executeAsync() called for " + keys.size() + " key(s), transaction: "
                    + (txnToUse != null ? "yes" : "no"));
        }

        if (keys.size() == 0) {
            return new RecordStream();
        }

        if (this.txnToUse != null && Log.warnEnabled()) {
            Log.warn("executeAsync() called within a transaction. "
                    + "Async operations may still be in flight when commit() is called, "
                    + "which could lead to inconsistent state. "
                    + "Consider using executeSync() or execute() for transactional safety.");
        }

        if (valueSets.size() != opBuilder.getNumKeys()) {
            throw new IllegalArgumentException(String.format(
                    "The number of '.values(...)' calls (%d) must match the number of specified keys (%d)",
                    valueSets.size(), opBuilder.getNumKeys()));
        }

        if (keys.size() >= OperationBuilder.getBatchOperationThreshold()) {
            return executeBatchAsync();
        } else {
            return executeIndividualAsync();
        }
    }

    private RecordStream executeBatchSync() {
    	BatchCommand parent = prepareBatch();
        List<BatchRecord> records = parent.getRecords();
        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();

        BatchStatus status = new BatchStatus(true);
        List<BatchNode> bns = BatchNodes.generate(cluster, parent, records, status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord record = records.get(i);
                BatchWrite bw = (BatchWrite)record;

                commands[count++] = new BatchSingle.OperateRecordSync(cluster, parent, bw, status,
                	bn.node);
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, records, status);
            }
        }

        Txn txn = opBuilder.getTxnToUse();

        if (txn != null) {
            TxnMonitor.addKeys(txnToUse, session, keys);
        }

        BatchExecutor.execute(cluster, commands, status);

        List<RecordResult> results = new ArrayList<>();
        // UPDATE and REPLACE_IF_EXISTS must always report KEY_NOT_FOUND_ERROR because
        // these operations are semantically expected to fail on non-existent records.
        OpType opType = opBuilder.getOpType();
        boolean effectiveRespondAllKeys = respondAllKeys ||
            opType == OpType.UPDATE || opType == OpType.REPLACE_IF_EXISTS;

        for (int i = 0; i < keys.size(); i++) {
            BatchRecord br = records.get(i);
            // Handle respondAllKeys and filterExp behavior
            if (switch (br.resultCode) {
            case ResultCode.KEY_NOT_FOUND_ERROR -> effectiveRespondAllKeys;
            case ResultCode.FILTERED_OUT -> failOnFilteredOut || effectiveRespondAllKeys;
            default -> true;
            }) {
                if (settings.getStackTraceOnException() && br.resultCode != ResultCode.OK) {
                    results.add(new RecordResult(br,
                            AerospikeException.resultCodeToException(br.resultCode, null, br.inDoubt), i));
                } else {
                    results.add(new RecordResult(br, i));
                }
            }

            // Handle respondAllKeys and filterExp behavior
        }
        return new RecordStream(results, 0);
    }

    private RecordStream executeBatchAsync() {
    	BatchCommand parent = prepareBatch();
        List<BatchRecord> records = parent.getRecords();
        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();

        BatchStatus status = new BatchStatus(true);
        List<BatchNode> bns = BatchNodes.generate(cluster, parent, records, status);

        AsyncRecordStream stream = new AsyncRecordStream(keys.size());
        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord record = records.get(i);
                BatchWrite bw = (BatchWrite)record;

                commands[count++] = new BatchSingle.OperateRecordAsync(cluster, parent, bw, status,
                	bn.node, stream, i);
            }
            else {
                commands[count++] = new Batch.OperateListAsync(cluster, parent, bn, records, stream,
                	status);
            }
        }

        Txn txn = opBuilder.getTxnToUse();

        if (txn != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKeys(txnToUse, session, keys);
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

        // Assume all keys have the same namespace.
        String namespace = keys.get(0).namespace;
        Partitions partitions = getPartitions(cluster, namespace);
        settings = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH,
        	partitions.scMode);
        final Expression filterExp = getFilterExp(session, namespace);

        BatchAttr attr = new BatchAttr();
	    attr.setWrite(settings, opBuilder.getOpType());

		List<BatchRecord> batchRecords = new ArrayList<>(keys.size());

        for (Key key : keys) {
            ValueData valueSet = valueSets.get(key);
            List<Operation> ops = getOperationsForValueData(valueSet);
            int ttl = getExpiration(valueSet);

            batchRecords.add(new BatchWrite(key, null, attr, opBuilder.getOpType(), ops,
            	valueSet.generation, ttl));
        }

        return new BatchCommand(cluster, partitions, opBuilder.getTxnToUse(), namespace,
        	batchRecords, filterExp, opBuilder.isRespondAllKeys(), false, settings);
    }

    private void operateBatchAsync(Cluster cluster, IBatchCommand[] commands, BatchStatus status, AsyncRecordStream stream) {
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
    private RecordStream executeIndividualSync() {
        if (keys.size() == 0) {
            return new RecordStream();
        }

        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();
        Key firstKey = keys.get(0);

        // Assume all keys have the same namespace.
        Partitions partitions = getPartitions(cluster, firstKey.namespace);
        Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);
        final Expression filterExp = processWhereClause(keys.get(0).namespace, opBuilder.getSession());

        if (txnToUse != null) {
            TxnMonitor.addKeys(txnToUse, session, keys);
        }

        if (keys.size() == 1) {
            try {
                Record rec = operate(cluster, partitions, policy, filterExp, firstKey);
                if (respondAllKeys || rec != null) {
                    return new RecordStream(firstKey, rec);
                }
            }
            catch (AerospikeException ae) {
                if (shouldPublishException(ae)) {
                    if (ae.getResultCode() != ResultCode.FILTERED_OUT) {
                        opBuilder.showWarningsOnException(ae, txnToUse, firstKey, getExpiration(valueSets.get(firstKey)));
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
                            Record record = operate(cluster, partitions, policy, filterExp, key);
                            if (respondAllKeys || record != null) {
                                stream.publish(new RecordResult(key, record, idx));
                            }
                        } catch (AerospikeException ae) {
                            if (shouldPublishException(ae)) {
                                if (ae.getResultCode() != ResultCode.FILTERED_OUT) {
                                    opBuilder.showWarningsOnException(ae, txnToUse, key, getExpiration(valueSets.get(key)));
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
    private RecordStream executeIndividualAsync() {
        if (keys.size() == 0) {
            return new RecordStream();
        }

        Session session = opBuilder.getSession();
        Cluster cluster = session.getCluster();

        // Assume all keys have the same namespace.
        String namespace = keys.get(0).namespace;
        Partitions partitions = getPartitions(cluster, namespace);
        Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, partitions.scMode);
        final Expression filterExp = getFilterExp(session, namespace);
        AsyncRecordStream asyncStream = new AsyncRecordStream(keys.size());

        if (txnToUse != null) {
            cluster.startVirtualThread(() -> {
                TxnMonitor.addKeys(txnToUse, session, keys);
                operateKeysAsync(cluster, partitions, policy, filterExp, asyncStream);
            });
        } else {
            operateKeysAsync(cluster, partitions, policy, filterExp, asyncStream);
        }

        return new RecordStream(asyncStream);
    }

    private void operateKeysAsync(Cluster cluster, Partitions partitions, Settings policy, Expression filterExp,
            AsyncRecordStream asyncStream) {
        AtomicInteger pendingOps = new AtomicInteger(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            final int index = i;
            cluster.startVirtualThread(() -> {
                try {
                    Record rec = operate(cluster, partitions, policy, filterExp, key);

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

    private Record operate(Cluster cluster, Partitions partitions, Settings policy, Expression filterExp, Key key) {
        ValueData valueSet = valueSets.get(key);
        List<Operation> ops = getOperationsForValueData(valueSet);
        int ttl = getExpiration(valueSet);

		OperateArgs args = new OperateArgs(ops);
        OperateWriteCommand cmd = new OperateWriteCommand(cluster, partitions, txnToUse, key, ops,
        	args, opBuilder.getOpType(), valueSet.generation, ttl, filterExp,
        	opBuilder.isFailOnFilteredOut(), policy);

        try {
            OperateWriteExecutor exec = new OperateWriteExecutor(cluster, cmd);
            exec.execute();
            return exec.getRecord();
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                throw ae;
            } else {
                opBuilder.showWarningsOnException(ae, txnToUse, key, ttl);
                throw ae;
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

    private Expression getFilterExp(Session session, String namespace) {
        if (dsl != null) {
            // Apply filter expression clause.
            ParseResult parseResult = dsl.process(namespace, session);
            return Exp.build(parseResult.getExp());
        } else {
            return null;
        }
    }

    private List<Operation> getOperationsForValueData(ValueData valueData) {
        Object[] values = valueData.values;
        List<Operation> ops = new ArrayList<>(binNames.length);
        for (int i = 0; i < binNames.length; i++) {
        	ops.add(Operation.put(new Bin(binNames[i], Value.get(values[i]))));
        }
        return ops;
    }

    private int getExpiration(ValueData valueData) {
        if (valueData.expirationInSeconds != Long.MIN_VALUE) {
            // Per-record expiration set via expireRecordAfterSeconds() after values()
            return opBuilder.getExpirationAsInt(valueData.expirationInSeconds);
        } else if (expirationInSecondsForAll != 0) {
            // Batch expiration set via expireAllRecordsAfterSeconds()
            return opBuilder.getExpirationAsInt(expirationInSecondsForAll);
        } else {
            return 0;
        }
    }
}
