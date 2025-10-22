package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.policy.BatchPolicy;
import com.aerospike.client.fluent.policy.BatchWritePolicy;
import com.aerospike.client.fluent.policy.WritePolicy;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;

public class OperationBuilder implements FilterableOperation<OperationBuilder> {
    private final List<Key> keys;
    protected final List<Operation> ops = new ArrayList<>();
    protected final OpType opType;
    protected final Session session;
    protected int generation = 0;
    protected long expirationInSeconds = 0;   // Default, get value from server
    protected long expirationInSecondsForAll = 0;
    protected Txn txnToUse;
    protected WhereClauseProcessor dsl = null;
    protected boolean respondAllKeys = false;
    protected boolean failOnFilteredOut = false;

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
        this.keys = List.of(key);
        this.opType = type;
        this.session = session;
        this.txnToUse = session.getCurrentTransaction();
    }

    public OperationBuilder(Session session, List<Key> keys, OpType type) {
        this.keys = keys;
        this.opType = type;
        this.session = session;
        this.txnToUse = session.getCurrentTransaction();
    }

    public BinBuilder bin(String binName) {
        return new BinBuilder(this, binName);
    }

    public BinsValuesBuilder bins(String binName, String... binNames) {
        return new BinsValuesBuilder(this, keys, binName, binNames);
    }
    protected OperationBuilder setTo(Bin bin) {
        this.ops.add(Operation.put(bin));
        return this;
    }

    protected OperationBuilder get(String binName) {
        this.ops.add(Operation.get(binName));
        return this;
    }

    protected OperationBuilder append(Bin bin) {
        this.ops.add(Operation.append(bin));
        return this;
    }

    protected OperationBuilder prepend(Bin bin) {
        this.ops.add(Operation.prepend(bin));
        return this;
    }

    protected OperationBuilder add(Bin bin) {
        this.ops.add(Operation.add(bin));
        return this;
    }

    protected OperationBuilder addOp(Operation op) {
        this.ops.add(op);
        return this;
    }

    public OperationBuilder ensureGenerationIs(int generation) {
        this.generation = generation;
        return this;
    }

    public OperationBuilder expireRecordAfter(Duration duration) {
        this.expirationInSeconds = duration.toSeconds();
        return this;
    }

    public OperationBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        this.expirationInSeconds = expirationInSeconds;
        return this;
    }

    protected long getExpirationInSecondsAndCheckValue(Date date) {
        long expirationInSeconds = (date.getTime() - new Date().getTime())/ 1000L;
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    public OperationBuilder expireRecordAt(Date date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    protected long getExpirationInSecondsAndCheckValue(LocalDateTime date) {
        LocalDateTime now = LocalDateTime.now();
        long expirationInSeconds = ChronoUnit.SECONDS.between(now, date);
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }


    public OperationBuilder expireRecordAt(LocalDateTime date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    public OperationBuilder withNoChangeInExpiration() {
        this.expirationInSeconds = TTL_NO_CHANGE;
        return this;
    }

    public OperationBuilder neverExpire() {
        this.expirationInSeconds = TTL_NEVER_EXPIRE;
        return this;
    }

    public OperationBuilder expiryFromServerDefault() {
        this.expirationInSeconds = TTL_SERVER_DEFAULT;
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

    private void setWhereClause(WhereClauseProcessor clause) {
        if (this.dsl == null) {
            this.dsl = clause;
        }
        else {
            throw new IllegalArgumentException("Only one 'where' clause can be specified. There is already one of '%s' and another is being set to '%s'"
                    .formatted(this.dsl, clause));
        }
    }

    @Override
    public OperationBuilder where(String dsl, Object ... params) {
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
        WritePolicy wp = getWritePolicy(retryable, generation, opType);
        long effectiveExpiration = (expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll;
        wp.expiration = getExpirationAsInt(effectiveExpiration);
        wp.txn = this.txnToUse;

        // Use batch operations if 10 or more keys
        if (keys.size() >= getBatchOperationThreshold()) {
            BatchPolicy batchPolicy = session.getBehavior().getMutablePolicy(CommandType.BATCH_WRITE);
            batchPolicy.txn = wp.txn;
            BatchWritePolicy bwp = new BatchWritePolicy();
            bwp.expiration = wp.expiration;
            bwp.generation = wp.generation;
            bwp.generationPolicy = wp.generationPolicy;
            return executeBatch(batchPolicy, operations, bwp);
        } else {
            return executeIndividualSync(wp, operations);
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
        WritePolicy wp = getWritePolicy(retryable, generation, opType);
        long effectiveExpiration = (expirationInSeconds != 0) ? expirationInSeconds : expirationInSecondsForAll;
        wp.expiration = getExpirationAsInt(effectiveExpiration);
        wp.txn = this.txnToUse;

        // Use batch operations if 10 or more keys
        if (keys.size() >= getBatchOperationThreshold()) {
            BatchPolicy batchPolicy = session.getBehavior().getMutablePolicy(CommandType.BATCH_WRITE);
            batchPolicy.txn = wp.txn;
            BatchWritePolicy bwp = new BatchWritePolicy();
            bwp.expiration = wp.expiration;
            bwp.generation = wp.generation;
            bwp.generationPolicy = wp.generationPolicy;
            return executeBatch(batchPolicy, operations, bwp);
        } else {
            return executeIndividualAsync(wp, operations);
        }
        */
    	return null;
    }

    protected Session getSession() {
        return this.session;
    }

    protected interface OperationMapper {
        Operation[] operationsForKey(Key key);
    }
    
    protected interface BatchWritePolicyMapper {
        BatchWritePolicy batchWritePolicyForKey(Key key);
    }

    protected RecordStream executeBatch(BatchPolicy batchPolicy, Operation[] operations, BatchWritePolicy batchWritePolicy) {
    	/*
        batchPolicy.txn = this.txnToUse;

        // Apply where clause if present
        Expression whereExp = null;
        if (this.dsl != null && !keys.isEmpty()) {
            ParseResult parseResult = this.dsl.process(keys.get(0).namespace, session);
            whereExp = Exp.build(parseResult.getExp());
        }
        batchPolicy.filterExp = whereExp;
        batchPolicy.failOnFilteredOut = this.failOnFilteredOut;

        List<BatchRecord> batchRecords = keys.stream()
                .map(key -> new BatchWrite(batchWritePolicy, key, operations))
                .collect(Collectors.toList());

        session.getClient().operate(batchPolicy, batchRecords);

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
    /**
     * Execute a single operation and publish the result to the async stream.
     * Handles filtering, error handling, and respondAllKeys logic.
     */
    protected void executeAndPublishSingleOperation(
            WritePolicy wp,
            Key key,
            Operation[] operations,
            AsyncRecordStream asyncStream) {
/*
        try {
            Record record = session.getClient().operate(wp, key, operations);
            if (respondAllKeys || record != null) {
                asyncStream.publish(new RecordResult(key, record));
            }
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                if (failOnFilteredOut || respondAllKeys) {
                    asyncStream.publish(new RecordResult(key, ae.getResultCode(), ae.getInDoubt(), ResultCode.getResultString(ae.getResultCode())));
                }
                // Otherwise skip this record
            } else {
                showWarningsOnException(ae, txnToUse, key, wp.expiration);
                asyncStream.publish(new RecordResult(key, ae.getResultCode(), ae.getInDoubt(), ResultCode.getResultString(ae.getResultCode())));
            }
        }
*/
    }

    /**
     * Execute operations synchronously for individual keys (< batch threshold).
     * All virtual threads are joined before returning.
     */
    protected RecordStream executeIndividualSync(WritePolicy wp, Operation[] operations) {
    	/*
        // Apply where clause if present
        Expression whereExp = null;
        if (this.dsl != null && !keys.isEmpty()) {
            ParseResult parseResult = this.dsl.process(keys.get(0).namespace, session);
            whereExp = Exp.build(parseResult.getExp());
        }
        wp.filterExp = whereExp;

        return executeIndividualParallelSync(wp, operations, keys);
        */
    	return null;
    }

    /**
     * Execute operations asynchronously for individual keys (< batch threshold).
     * Returns immediately; virtual threads complete in background.
     */
    protected RecordStream executeIndividualAsync(WritePolicy wp, Operation[] operations) {
    	/*
        // Apply where clause if present
        Expression whereExp = null;
        if (this.dsl != null && !keys.isEmpty()) {
            ParseResult parseResult = this.dsl.process(keys.get(0).namespace, session);
            whereExp = Exp.build(parseResult.getExp());
        }
        wp.filterExp = whereExp;

        return executeIndividualParallelAsync(wp, operations, keys);
        */
    	return null;
    }

    /**
     * Execute operations in parallel using virtual threads, JOINING all threads before return.
     * Guarantees all operations complete (successfully or exceptionally) before returning.
     */
    protected RecordStream executeIndividualParallelSync(
            WritePolicy wp,
            Operation[] operations,
            List<Key> keysToProcess) {
    	return null;
    	/*

        // Single key: synchronous execution (no threads needed)
        if (keysToProcess.size() == 1) {
            List<BatchRecord> records = new ArrayList<>();
            Key key = keysToProcess.get(0);
            try {
                Record result = session.getClient().operate(wp, key, operations);
                if (respondAllKeys || result != null) {
                    records.add(new BatchRecord(key, result, true));
                }
            } catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                    if (failOnFilteredOut || respondAllKeys) {
                        records.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                    }
                } else {
                    showWarningsOnException(ae, txnToUse, key, wp.expiration);
                    records.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                }
            }
            return new RecordStream(records, 0, 0, null);
        }

        // Multiple keys: parallel execution with virtual threads, JOINED before return
        List<BatchRecord> allRecords = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(keysToProcess.size());

        for (Key key : keysToProcess) {
            Thread.startVirtualThread(() -> {
                try {
                    // Execute operation and collect result
                    try {
                        Record record = session.getClient().operate(wp, key, operations);
                        if (respondAllKeys || record != null) {
                            allRecords.add(new BatchRecord(key, record, true));
                        }
                    } catch (AerospikeException ae) {
                        if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
                            if (failOnFilteredOut || respondAllKeys) {
                                allRecords.add(new BatchRecord(key, null, ae.getResultCode(), ae.getInDoubt(), true));
                            }
                        } else {
                            showWarningsOnException(ae, txnToUse, key, wp.expiration);
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
    }

    /**
     * Execute operations in parallel using virtual threads, WITHOUT joining.
     * Returns immediately with AsyncRecordStream; threads complete in background.
     */
    protected RecordStream executeIndividualParallelAsync(
            WritePolicy wp,
            Operation[] operations,
            List<Key> keysToProcess) {

    	return null;
    	/*
        // Even single key: use async execution with virtual thread
        AsyncRecordStream asyncStream = new AsyncRecordStream(keysToProcess.size());
        AtomicInteger pendingOps = new AtomicInteger(keysToProcess.size());

        for (Key key : keysToProcess) {
            Thread.startVirtualThread(() -> {
                try {
                    executeAndPublishSingleOperation(wp, key, operations, asyncStream);
                } finally {
                    if (pendingOps.decrementAndGet() == 0) {
                        asyncStream.complete();
                    }
                }
            });
        }

        return new RecordStream(asyncStream);
        */
    }
}
