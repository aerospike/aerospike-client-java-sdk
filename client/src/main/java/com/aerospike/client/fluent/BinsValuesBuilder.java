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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.command.OperateCommand;
import com.aerospike.client.fluent.command.SyncOperateExecutor;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;

/**
 * Builder for the bins+values pattern in OperationBuilder.
 * This allows setting multiple bin names and then providing values for each record.
 */
public class BinsValuesBuilder implements FilterableOperation<BinsValuesBuilder> {
    private static class ValueData {
        private Object[] values;
        private int generation = 0;
        private long expirationInSeconds = Long.MIN_VALUE;

        public ValueData(Object[] values) {
            this.values = values;
        }
    }

    private long expirationInSecondsForAll = 0;
    private final OperationBuilder opBuilder;
    private final String[] binNames;
    private final Map<Key, ValueData> valueSets = new HashMap<>();
    private final List<Key> keys;
    private ValueData current = null;
    private Txn txnToUse;
    protected WhereClauseProcessor dsl = null;
    protected boolean respondAllKeys = false;
    protected boolean failOnFilteredOut = false;

    public BinsValuesBuilder(OperationBuilder opBuilder, List<Key> keys, String binName, String... binNames) {
        this.opBuilder = opBuilder;
        this.binNames = new String[1 + binNames.length];
        this.binNames[0] = binName;
        System.arraycopy(binNames, 0, this.binNames, 1, binNames.length);
        this.keys = keys;
        this.txnToUse = opBuilder.getTxnToUse();
    }

    /**
     * Add a set of values for one record. The number of values must match the number of bins.
     * Multiple calls to this method can be chained together.
     *
     * @param values The values for this record
     * @return This builder for chaining
     */
    public BinsValuesBuilder values(Object... values) {
        if (values.length != binNames.length) {
            throw new IllegalArgumentException(String.format(
                "When calling '.values(...)' to specify the values for multiple bins,"
                + " the number of values must match the number of bins specified in the '.bins(...)' call."
                + " This call specified %d bins, but supplied %d values.",
                binNames.length, values.length));
        }

        checkRoomToAddAnotherValue();
        current = new ValueData(values);
        valueSets.put(keys.get(valueSets.size()),current);
        return this;
    }

    private void checkValuesExist(String name) {
        if (valueSets.size() == 0) {
            throw new IllegalArgumentException(String.format(
                    "%s was called when no values were defined (by calling '.values'). This method"
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

    public BinsValuesBuilder ensureGenerationIs(int generation) {
        checkValuesExist("ensureGenerationIs");
        current.generation = generation;
        return this;
    }

    public BinsValuesBuilder expireRecordAfter(Duration duration) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = duration.toSeconds();
        return this;
    }

    public BinsValuesBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = expirationInSeconds;
        return this;
    }

    public BinsValuesBuilder expireRecordAt(Date date) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = opBuilder.getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    public BinsValuesBuilder expireRecordAt(LocalDateTime date) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = opBuilder.getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    public BinsValuesBuilder withNoChangeInExpiration() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = OperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    public BinsValuesBuilder neverExpire() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = OperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    public BinsValuesBuilder expiryFromServerDefault() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = OperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    public BinsValuesBuilder notInAnyTransaction() {
        this.txnToUse = null;
        return this;
    }

    public BinsValuesBuilder inTransaction(Txn txn) {
        this.txnToUse = txn;
        return this;
    }

    // Multi-key expiry methods (only available for multiple keys)
    public BinsValuesBuilder expireAllRecordsAfter(Duration duration) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAfter() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = duration.getSeconds();
        return this;
    }

    public BinsValuesBuilder expireAllRecordsAfterSeconds(long seconds) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAfterSeconds() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = seconds;
        return this;
    }

    public BinsValuesBuilder expireAllRecordsAt(LocalDateTime dateTime) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = opBuilder.getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    public BinsValuesBuilder expireAllRecordsAt(Date date) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = opBuilder.getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    public BinsValuesBuilder neverExpireAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("neverExpireAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    public BinsValuesBuilder withNoChangeInExpirationForAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("withNoChangeInExpirationForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    public BinsValuesBuilder expiryFromServerDefaultForAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expiryFromServerDefaultForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = OperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    private void setWhereClause(WhereClauseProcessor clause) {
        if (this.dsl == null) {
            this.dsl = clause;
        }
        else {
            throw new IllegalArgumentException("Only one 'where' clause can be specified. There is already one of '%s' and another is being set to '%s'"
                    .formatted(this.dsl, clause));
        }
    }

    /**
     * Apply a where clause filter to these operations. Only records matching the filter will be affected.
     * <p>
     * The DSL string can contain parameters which are replaced with the passed arguments. For example:
     * <pre>
     * builder.where("$.age > %d", 21)
     * </pre>
     *
     * @param dsl The DSL string defining the filter condition
     * @param params Optional parameters to be substituted into the DSL string
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(String dsl, Object ... params) {
        WhereClauseProcessor impl;
        if (dsl == null || dsl.isEmpty()) {
            impl = null;
        }
        else if (params.length == 0) {
            impl = WhereClauseProcessor.from(false, dsl);
        }
        else {
            impl = WhereClauseProcessor.from(false, String.format(dsl, params));
        }
        setWhereClause(impl);
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
     * Apply a where clause filter to these operations using a prepared DSL.
     * Only records matching the filter will be affected.
     *
     * @param dsl The prepared DSL defining the filter condition
     * @param params Parameters to be substituted into the prepared DSL
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder where(PreparedDsl dsl, Object ... params) {
        setWhereClause(WhereClauseProcessor.from(false, dsl, params));
        return this;
    }

    /**
     * Apply a where clause filter to these operations using an Aerospike expression.
     * Only records matching the filter will be affected.
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
     * If a where clause is specified and a record is filtered out, it will appear in the
     * result stream with an exception code of {@link ResultCode#FILTERED_OUT} rather than
     * being silently omitted from the results.
     *
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    /**
     * By default, if a key does not map to a record (or is filtered out), nothing will be
     * returned in the stream for that key. If this flag is specified, a result will be
     * included in the stream for every key, even if the record doesn't exist or was filtered out.
     *
     * @return This builder for method chaining
     */
    @Override
    public BinsValuesBuilder respondAllKeys() {
        this.respondAllKeys = true;
        return this;
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
            Log.debug("BinsValuesBuilder.executeSync() called for " + keys.size() + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        if (valueSets.size() != opBuilder.getNumKeys()) {
            throw new IllegalArgumentException(String.format(
                "The number of '.values(...)' calls (%d) must match the number of specified keys (%d)",
                valueSets.size(), opBuilder.getNumKeys()));
        }

        if (keys.size() >= OperationBuilder.getBatchOperationThreshold()) {
            return executeBatch();
        }
        else {
            return executeIndividualSync();
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
            Log.debug("BinsValuesBuilder.executeAsync() called for " + keys.size() + " key(s), transaction: " +
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

        if (valueSets.size() != opBuilder.getNumKeys()) {
            throw new IllegalArgumentException(String.format(
                "The number of '.values(...)' calls (%d) must match the number of specified keys (%d)",
                valueSets.size(), opBuilder.getNumKeys()));
        }

        if (keys.size() >= OperationBuilder.getBatchOperationThreshold()) {
            return executeBatch();
        }
        else {
            return executeIndividualAsync();
        }
    }

    private Operation[] getOperationsForValueData(ValueData valueData) {
        Object[] values = valueData.values;
        Operation[] ops = new Operation[binNames.length];
        for (int i = 0; i < binNames.length; i++) {
            ops[i] = Operation.put(new Bin(binNames[i], Value.get(values[i])));
        }
        return ops;
    }

    private int getExpiration(ValueData valueData) {
        if (valueData.expirationInSeconds != Long.MIN_VALUE) {
            return opBuilder.getExpirationAsInt(valueData.expirationInSeconds);
        }
        else {
            return opBuilder.getExpirationAsInt(expirationInSecondsForAll);
        }
    }

    protected RecordStream executeBatch() {
    	/*
        BatchPolicy batchPolicy = opBuilder.getSession().getBehavior().getMutablePolicy(CommandType.BATCH_WRITE);
        batchPolicy.setTxn(txnToUse);

        // Apply where clause if present
        Expression whereExp = null;
        if (this.dsl != null && !keys.isEmpty()) {
            ParseResult parseResult = this.dsl.process(keys.get(0).namespace, opBuilder.getSession());
            whereExp = Exp.build(parseResult.getExp());
        }
        batchPolicy.filterExp = whereExp;
        batchPolicy.failOnFilteredOut = this.failOnFilteredOut;

        List<BatchRecord> batchRecords = new ArrayList<>();
        for (Key key : keys) {
            ValueData valueSet = valueSets.get(key);
            Operation[] ops = getOperationsForValueData(valueSet);
            BatchWritePolicy bwp = new BatchWritePolicy();
            // Fix: Apply policies even when generation is 0
            if (valueSet.generation != 0) {
                bwp.generation = valueSet.generation;
                bwp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            }
            bwp.expiration = getExpiration(valueSet);
            bwp.recordExistsAction = OperationBuilder.recordExistsActionFromOpType(opBuilder.opType);
            batchRecords.add(new BatchWrite(bwp, key, ops));
        }
        batchPolicy.txn = this.txnToUse;

        opBuilder.getSession().getClient().operate(batchPolicy, batchRecords);

        // Handle respondAllKeys and filterExp behavior
        if (!respondAllKeys && whereExp != null) {
            // Remove any items which have been filtered out or not found
            batchRecords.removeIf(br -> (br.resultCode == ResultCode.OK && br.record == null)
                    || (br.resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
                    || (br.resultCode == ResultCode.FILTERED_OUT && !failOnFilteredOut));
        }

        return new RecordStream(batchRecords, 0, 0, null);
        */
    	return null;
    }

    /**
     * Execute operations synchronously for individual keys (< batch threshold).
     * All virtual threads are joined before returning.
     */
    protected RecordStream executeIndividualSync() {
    	/*
        Expression tempWhere = null;
        // Apply where clause if present
        if (this.dsl != null && !keys.isEmpty()) {
            ParseResult parseResult = this.dsl.process(keys.get(0).namespace, opBuilder.getSession());
            tempWhere = Exp.build(parseResult.getExp());
        }
        final Expression whereExp = tempWhere;

        // Single key: synchronous execution
        if (keys.size() == 1) {
            Key key = keys.get(0);
            ValueData valueSet = valueSets.get(key);
            Operation[] ops = getOperationsForValueData(valueSet);
            WritePolicy wp = opBuilder.getWritePolicy(true, valueSet.generation, this.opBuilder.opType);
            wp.expiration = getExpiration(valueSet);
            wp.txn = this.txnToUse;
            wp.filterExp = whereExp;

            List<BatchRecord> records = new ArrayList<>();
            try {
                com.aerospike.client.Record record = opBuilder.getSession().getClient().operate(wp, key, ops);
                if (respondAllKeys || record != null) {
                    records.add(new BatchRecord(key, record, true));
                }
            } catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                    if (failOnFilteredOut || respondAllKeys) {
                        records.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                    }
                } else {
                    opBuilder.showWarningsOnException(ae, txnToUse, key, wp.expiration);
                    records.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                }
            }
            return new RecordStream(records, 0, 0, null);
        }

        // Multiple keys: parallel execution with virtual threads, JOINED before return
        List<BatchRecord> allRecords = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(keys.size());

        for (Key key : keys) {
            ValueData valueSet = valueSets.get(key);
            Thread.startVirtualThread(() -> {
                try {
                    Operation[] ops = getOperationsForValueData(valueSet);
                    WritePolicy wp = opBuilder.getWritePolicy(true, valueSet.generation, this.opBuilder.opType);
                    wp.expiration = getExpiration(valueSet);
                    wp.txn = this.txnToUse;
                    wp.filterExp = whereExp;

                    try {
                        com.aerospike.client.Record record = opBuilder.getSession().getClient().operate(wp, key, ops);
                        if (respondAllKeys || record != null) {
                            allRecords.add(new BatchRecord(key, record, true));
                        }
                    } catch (AerospikeException ae) {
                        if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                            if (failOnFilteredOut || respondAllKeys) {
                                allRecords.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                            }
                        } else {
                            opBuilder.showWarningsOnException(ae, txnToUse, key, wp.expiration);
                            allRecords.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // WAIT for all threads to complete
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for operations to complete", e);
        }

        return new RecordStream(allRecords, 0, 0, null);
        */

    	// This method is only used for single record writes.
    	Session session = opBuilder.getSession();

    	// TODO: BN: Is there a better way to know if we're in SC or AP?
    	Settings policy = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, session.isNamespaceSC(keys.get(0).namespace));

    	// Since getOperationsForValueData() returns all write operations, the read operations are not relevant.
    	// Set read modes to default.
        ReadModeAP readModeAP = ReadModeAP.ONE;
        ReadModeSC readModeSC = ReadModeSC.SESSION;

        Cluster cluster = session.getCluster();
        Key[] keyArray = new Key[keys.size()];
        Record[] recordArray = new Record[keys.size()];

		if (txnToUse != null) {
			TxnMonitor.addKeys(txnToUse, cluster, policy, keys);
		}

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            ValueData valueSet = valueSets.get(key);
            Operation[] ops = getOperationsForValueData(valueSet);

            int ttl = getExpiration(valueSet);

            OperateCommand cmd = new OperateCommand(cluster, txnToUse, key, ops, opBuilder.opType,
				valueSet.generation, ttl, readModeAP, readModeSC, true /* TODO: Need this in external API! */, policy
				);

            try {
            	SyncOperateExecutor exec = new SyncOperateExecutor(cluster, cmd);
            	exec.execute();

            	Record record = exec.getRecord();
	            keyArray[i] = key;
	            recordArray[i] = record;
            }
            catch (AerospikeException ae) {
                opBuilder.showWarningsOnExceptionAndThrow(ae, txnToUse, key, ttl);
            }
        }
        return new RecordStream(keyArray, recordArray, 0, 0, null, true);
    }

    /**
     * Execute operations asynchronously for individual keys (< batch threshold).
     * Returns immediately; virtual threads complete in background.
     */
    protected RecordStream executeIndividualAsync() {
    	/*
        Expression tempWhere = null;
        // Apply where clause if present
        if (this.dsl != null && !keys.isEmpty()) {
            ParseResult parseResult = this.dsl.process(keys.get(0).namespace, opBuilder.getSession());
            tempWhere = Exp.build(parseResult.getExp());
        }
        final Expression whereExp = tempWhere;

        // Even single key: use async execution with virtual thread
        AsyncRecordStream asyncStream = new AsyncRecordStream(keys.size());
        AtomicInteger pendingOps = new AtomicInteger(keys.size());

        for (Key key : keys) {
            ValueData valueSet = valueSets.get(key);
            Thread.startVirtualThread(() -> {
                try {
                    Operation[] ops = getOperationsForValueData(valueSet);
                    WritePolicy wp = opBuilder.getWritePolicy(true, valueSet.generation, this.opBuilder.opType);
                    wp.expiration = getExpiration(valueSet);
                    wp.txn = this.txnToUse;
                    wp.filterExp = whereExp;

                    opBuilder.executeAndPublishSingleOperation(wp, key, ops, asyncStream);
                } finally {
                    if (pendingOps.decrementAndGet() == 0) {
                        asyncStream.complete();
                    }
                }
            });
        }

        return new RecordStream(asyncStream);
        */
    	return null;
    }
}