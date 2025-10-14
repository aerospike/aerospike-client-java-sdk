package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.aerospike.client.fluent.policy.BatchPolicy;
import com.aerospike.client.fluent.policy.BatchWritePolicy;
import com.aerospike.client.fluent.policy.WritePolicy;

public class OperationBuilder {
    private final List<Key> keys;
    protected final List<Operation> ops = new ArrayList<>();
    protected final OpType opType;
    protected final Session session;
    protected int generation = 0;
    protected long expirationInSeconds = 0;   // Default, get value from server
    protected Txn txnToUse;

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

    protected long getExpirationInSecondAndCheckValue(Date date) {
        long expirationInSeconds = (date.getTime() - new Date().getTime())/ 1000L;
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    public OperationBuilder expireRecordAt(Date date) {
        this.expirationInSeconds = getExpirationInSecondAndCheckValue(date);
        return this;
    }

    protected long getExpirationInSecondAndCheckValue(LocalDateTime date) {
        LocalDateTime now = LocalDateTime.now();
        long expirationInSeconds = ChronoUnit.SECONDS.between(now, date);
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }


    public OperationBuilder expireRecordAt(LocalDateTime date) {
        this.expirationInSeconds = getExpirationInSecondAndCheckValue(date);
        return this;
    }

    public OperationBuilder withNoChangeInExpiration() {
        this.expirationInSeconds = -2;
        return this;
    }

    public OperationBuilder neverExpire() {
        this.expirationInSeconds = -1;
        return this;
    }

    public OperationBuilder expiryFromServerDefault() {
        this.expirationInSeconds = 0;
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

    public RecordStream execute() {
    	/*
        Operation[] operations = ops.toArray(new Operation[0]);
        boolean retryable = OperationBuilder.areOperationsRetryable(operations);
        WritePolicy wp = getWritePolicy(retryable, generation, opType);
        wp.expiration = getExpirationAsInt(expirationInSeconds);
        wp.txn = this.txnToUse;

        // Use batch operations if 10 or more keys
        if (keys.size() >= 10) {
            BatchPolicy batchPolicy = session.getBehavior().getMutablePolicy(CommandType.BATCH_WRITE);
            batchPolicy.txn= wp.txn;
            BatchWritePolicy bwp = new BatchWritePolicy();
            bwp.expiration = wp.expiration;
            bwp.generation = wp.generation;
            bwp.generationPolicy = wp.generationPolicy;
            return executeBatch(batchPolicy, operations, bwp);
        } else {
            return executeIndividual(wp, operations);
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

        List<BatchRecord> batchRecords = keys.stream()
                .map(key -> new BatchWrite(batchWritePolicy, key, operations))
                .collect(Collectors.toList());

        boolean result = session.getClient().operate(batchPolicy, batchRecords);

        Key[] keyArray = batchRecords.stream().map(batchRecord -> batchRecord.key).toArray(Key[]::new);
        Record[] recordArray = batchRecords.stream().map(batchRecord -> batchRecord.record).toArray(Record[]::new);
        return new RecordStream(keyArray, recordArray, 0, 0, null);
        */
    	return null;
    }

    protected void showWarningsOnExceptionAndThrow(AerospikeException ae, Txn txn, Key key, int expiration) {
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
        throw ae;
    }

    protected RecordStream executeIndividual(WritePolicy wp, Operation[] operations) {
    	/*
        Key[] keyArray = new Key[keys.size()];
        Record[] recordArray = new Record[keys.size()];

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            try {
                Record record = session.getClient().operate(wp, key, operations);
                keyArray[i] = key;
                recordArray[i] = record;
            } catch (AerospikeException ae) {
                showWarningsOnExceptionAndThrow(ae, txnToUse, key, wp.expiration);
            }
        }

        return new RecordStream(keyArray, recordArray, 0, 0, null);
        */
    	return null;
    }
}
