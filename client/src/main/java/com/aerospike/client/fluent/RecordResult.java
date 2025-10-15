package com.aerospike.client.fluent;

import com.aerospike.client.fluent.query.KeyRecord;

public record RecordResult(Key key, Record recordOrNull, int resultCode, boolean inDoubt, String message) {

    public RecordResult(Key key, Record rec) {
        this(key, rec, ResultCode.OK, false, null);
    }

    public RecordResult(Key key, int resultCode, boolean inDoubt, String message) {
        this(key, null, resultCode, inDoubt, message);
    }

    public RecordResult(KeyRecord keyRecord) {
        this(keyRecord.key, keyRecord.record, ResultCode.OK, false, null);
    }
    
    public RecordResult(BatchRecord batchRecord) {
        this(batchRecord.key, batchRecord.record, batchRecord.resultCode, batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode));
    }
    
    public boolean isOk() {
        return this.resultCode == ResultCode.OK;
    }
    
    /**
     * If this result contains an error, then throw the appropriate exception, otherwise return this object
     */
    public RecordResult orThrow() {
        if (!isOk()) {
            throw AerospikeException.resultCodeToException(resultCode, message(), inDoubt);
        }
        return this;
    }
    
    public Record recordOrThrow() {
        orThrow();
        return recordOrNull;
    }
}
