package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for the bins+values pattern in OperationBuilder.
 * This allows setting multiple bin names and then providing values for each record.
 */
public class BinsValuesBuilder {
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
        current.expirationInSeconds = opBuilder.getExpirationInSecondAndCheckValue(date);
        return this;
    }

    public BinsValuesBuilder expireRecordAt(LocalDateTime date) {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = opBuilder.getExpirationInSecondAndCheckValue(date);
        return this;
    }

    public BinsValuesBuilder withNoChangeInExpiration() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = -2;
        return this;
    }

    public BinsValuesBuilder neverExpire() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = -1;
        return this;
    }

    public BinsValuesBuilder expiryFromServerDefault() {
        checkValuesExist("expireRecordAfter");
        current.expirationInSeconds = 0;
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
        this.expirationInSecondsForAll = opBuilder.getExpirationInSecondAndCheckValue(dateTime);
        return this;
    }

    public BinsValuesBuilder expireAllRecordsAt(Date date) {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expireAllRecordsAt() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = opBuilder.getExpirationInSecondAndCheckValue(date);
        return this;
    }

    public BinsValuesBuilder neverExpireAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("neverExpireAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = -1;
        return this;
    }

    public BinsValuesBuilder withNoChangeInExpirationForAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("withNoChangeInExpirationForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = -2;
        return this;
    }

    public BinsValuesBuilder expiryFromServerDefaultForAllRecords() {
        if (!opBuilder.isMultiKey()) {
            throw new IllegalStateException("expiryFromServerDefaultForAllRecords() is only available when multiple keys are specified");
        }
        this.expirationInSecondsForAll = 0;
        return this;
    }

    public RecordStream execute() {
        if (valueSets.size() != opBuilder.getNumKeys()) {
            throw new IllegalArgumentException(String.format(
                "The number of '.values(...)' calls (%d) must match the number of specified keys (%d)",
                valueSets.size(), opBuilder.getNumKeys()));
        }

        // Execute each value set for its corresponding key
        if (keys.size() >= 10) {
            return executeBatch();
        }
        else {
            return executeIndividual();
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

    private int getExpriation(ValueData valueData) {
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
        List<BatchRecord> batchRecords = new ArrayList<>();
        for (Key key : keys) {
            ValueData valueSet = valueSets.get(key);
            Operation[] ops = getOperationsForValueData(valueSet);
            BatchWritePolicy bwp = new BatchWritePolicy();
            if (valueSet.generation > 0) {
                bwp.generation = valueSet.generation;
                bwp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                bwp.expiration = getExpriation(valueSet);
                bwp.recordExistsAction = opBuilder.recordExistsActionFromOpType(opBuilder.opType);
            }
            batchRecords.add(new BatchWrite(bwp, key, ops));
        }
        batchPolicy.txn = this.txnToUse;

        boolean result = opBuilder.getSession().getClient().operate(batchPolicy, batchRecords);

        Key[] keyArray = batchRecords.stream().map(batchRecord -> batchRecord.key).toArray(Key[]::new);
        Record[] recordArray = batchRecords.stream().map(batchRecord -> batchRecord.record).toArray(Record[]::new);
        return new RecordStream(keyArray, recordArray, 0, 0, null);
        */
    	return null;
    }

    protected RecordStream executeIndividual() {
    	/*
        Key[] keyArray = new Key[keys.size()];
        Record[] recordArray = new Record[keys.size()];

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            ValueData valueSet = valueSets.get(key);
            Operation[] ops = getOperationsForValueData(valueSet);
            WritePolicy wp = opBuilder.getWritePolicy(true, valueSet.generation, this.opBuilder.opType);
            wp.expiration = getExpriation(valueSet);
            wp.txn = this.txnToUse;

            try {
                Record record = opBuilder.getSession().getClient().operate(wp, key, ops);
                keyArray[i] = key;
                recordArray[i] = record;
            } catch (AerospikeException ae) {
                opBuilder.showWarningsOnExceptionAndThrow(ae, txnToUse, key, wp.expiration);
            }
        }

        return new RecordStream(keyArray, recordArray, 0, 0, null);
        */
    	return null;
    }

}