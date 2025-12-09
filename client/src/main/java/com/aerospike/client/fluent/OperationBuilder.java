package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;

public class OperationBuilder extends AbstractOperationBuilder<OperationBuilder> implements FilterableOperation<OperationBuilder> {
    private final List<Key> keys;
    protected int generation = 0;
    protected long expirationInSecondsForAll = 0;
    protected Txn txnToUse;

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

    public static boolean areOperationsRetryable(Operation[] operations) {
        for (Operation operation : operations) {
            switch (operation.type) {
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

    protected Txn getTxnToUse() {
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

    protected int getNumKeys() {
        return keys.size();
    }

    protected boolean isMultiKey() {
        return keys.size() > 1;
    }

    protected int getExpirationAsInt(long expirationInSeconds) {
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

    protected Settings getSettings(boolean retryable) {
        return session.getBehavior()
                .getSettings(retryable? OpKind.WRITE_RETRYABLE : OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, session.isNamespaceSC(keys.get(0).namespace));
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
    public OperationBuilder failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    @Override
    public OperationBuilder respondAllKeys() {
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
    	/*
        if (Log.debugEnabled()) {
            Log.debug("OperationBuilder.executeSync() called for " + keys.size() + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        Operation[] operations = ops.toArray(new Operation[0]);
        boolean retryable = OperationBuilder.areOperationsRetryable(operations);
        Settings settings = getSettings(retryable);

        // Use batch operations if 10 or more keys
        if (keys.size() >= getBatchOperationThreshold()) {
            return executeBatchSync(settings, operations);
        } else {
            return executeIndividualParallelSync(settings, operations, keys);
        }
        */
    	return null;
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
    	/*
        if (Log.debugEnabled()) {
            Log.debug("OperationBuilder.executeAsync() called for " + keys.size() + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        if (txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using executeSync() or execute() for transactional safety."
            );
        }

        Operation[] operations = ops.toArray(new Operation[0]);
        boolean retryable = OperationBuilder.areOperationsRetryable(operations);
        Settings settings = getSettings(retryable);

        // Use batch operations if 10 or more keys
        if (keys.size() >= getBatchOperationThreshold()) {
            return executeBatchAsync(settings, operations);
        } else {
            return executeIndividualAsync(settings, operations);
        }
        */
    	return null;
    }

    protected Session getSession() {
        return this.session;
    }

    protected RecordStream executeBatchSync(Settings settings, Operation[] operations) {
    	/*
        BatchWritePolicy batchWritePolicy = getBatchWritePolicy();

        BatchPolicy batchPolicy = settingsToBatchPolicy(settings);
        List<BatchRecord> batchRecords = keys.stream()
                .map(key -> new BatchWrite(batchWritePolicy, key, operations))
                .collect(Collectors.toList());

        session.getClient().operate(batchPolicy, batchRecords);

        // Convert BatchRecord to RecordResult with proper filtering and stack trace handling
        AsyncRecordStream recordStream = new AsyncRecordStream(batchRecords.size());
        try {
            for (int i = 0; i < batchRecords.size(); i++) {
                BatchRecord br = batchRecords.get(i);
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

    protected RecordStream executeBatchAsync(Settings settings, Operation[] operations) {
        AsyncRecordStream asyncStream = new AsyncRecordStream(keys.size());
        Thread.startVirtualThread(() -> {
            try {
                BatchWritePolicy batchWritePolicy = getBatchWritePolicy();

                BatchPolicy batchPolicy = settingsToBatchPolicy(settings);
                List<BatchRecord> batchRecords = keys.stream()
                        .map(key -> new BatchWrite(batchWritePolicy, key, operations))
                        .collect(Collectors.toList());

                session.getClient().operate(batchPolicy, batchRecords);

                for (int i = 0; i < keys.size(); i++) {
                    BatchRecord br = batchRecords.get(i);
                    // Use inherited shouldIncludeResult method
                    if (shouldIncludeResult(br.resultCode)) {
                        asyncStream.publish(createRecordResultFromBatchRecord(br, settings, i));
                    }
                }
            }
            finally {
                asyncStream.complete();
            }
        });
        return new RecordStream(asyncStream);
        */
    	return null;
    }

    protected void showWarningsOnExceptionAndThrow(AerospikeException ae, Txn txn, Key key, int expiration) {
        showWarningsOnException(ae, txn, key, expiration);
        throw ae;
    }

    protected void showWarningsOnException(AerospikeException ae, Txn txn, Key key, int expiration) {
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
