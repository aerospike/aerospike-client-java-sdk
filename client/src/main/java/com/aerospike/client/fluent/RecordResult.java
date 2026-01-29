package com.aerospike.client.fluent;

import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.query.KeyRecord;

public record RecordResult(Key key, Record recordOrNull, int resultCode, AerospikeException exception, boolean inDoubt, String message, int index) {

    public RecordResult(Key key, Record rec, int index) {
        this(key, rec, ResultCode.OK, null, false, null, index);
    }
    
    public RecordResult(Key key, int resultCode, boolean inDoubt, String message, int index) {
        this(key, null, resultCode, null, inDoubt, message, index);
    }
    
    public RecordResult(Key key, AerospikeException ae, int index) {
        this(key, null, ae.getResultCode(), ae, ae.getInDoubt(), ae.getMessage(), index);
    }
    
    public RecordResult(KeyRecord keyRecord, int index) {
        this(keyRecord.key, keyRecord.record, ResultCode.OK, null, false, null, index);
    }
    
    public RecordResult(BatchRecord batchRecord, int index) {
        this(batchRecord.key, batchRecord.record, batchRecord.resultCode, null, batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode), index);
    }
    
    public RecordResult(BatchRecord batchRecord, AerospikeException ae, int index) {
        this(batchRecord.key, batchRecord.record, batchRecord.resultCode, ae, batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode), index);
    }
    
    // Constructor with error handling based on stackTraceOnException flag
    public RecordResult(Key key, int resultCode, boolean inDoubt, String message, boolean stackTraceOnException, int index) {
        this(key, null, resultCode, 
             stackTraceOnException && resultCode != ResultCode.OK ? 
                 createExceptionWithCleanedStackTrace(resultCode, message, inDoubt) : null,
             inDoubt, message, index);
    }
    
    // Constructor for BatchRecord with error handling
    public RecordResult(BatchRecord batchRecord, boolean stackTraceOnException, int index) {
        this(batchRecord.key, batchRecord.record, batchRecord.resultCode,
             stackTraceOnException && batchRecord.resultCode != ResultCode.OK ?
                 createExceptionWithCleanedStackTrace(batchRecord.resultCode, 
                     ResultCode.getResultString(batchRecord.resultCode), batchRecord.inDoubt) : null,
             batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode), index);
    }
    
    // Helper method to create exception and clean stack trace
    private static AerospikeException createExceptionWithCleanedStackTrace(int resultCode, String message, boolean inDoubt) {
        AerospikeException ex = AerospikeException.resultCodeToException(resultCode, message, inDoubt);
        // Remove RecordResult constructor and resultCodeToException from stack trace
        StackTraceElement[] stack = ex.getStackTrace();
        int startIndex = 0;
        for (int i = 0; i < stack.length; i++) {
            String className = stack[i].getClassName();
            String methodName = stack[i].getMethodName();
            // Find first frame that's not RecordResult or resultCodeToException
            if (!className.equals("com.aerospike.RecordResult") && 
                !methodName.equals("resultCodeToException")) {
                startIndex = i;
                break;
            }
        }
        if (startIndex > 0) {
            StackTraceElement[] cleanedStack = new StackTraceElement[stack.length - startIndex];
            System.arraycopy(stack, startIndex, cleanedStack, 0, cleanedStack.length);
            ex.setStackTrace(cleanedStack);
        }
        return ex;
    }
    
    public boolean isOk() {
        return this.resultCode == ResultCode.OK;
    }
    
    /**
     * If this result contains an error, then throw the appropriate exception, otherwise return this object
     */
    public RecordResult orThrow() {
        if (!isOk()) {
            if (exception != null) {
                throw exception;
            }
            else {
                throw AerospikeException.resultCodeToException(resultCode, message(), inDoubt);
            }
        }
        return this;
    }
    
    public Record recordOrThrow() {
        orThrow();
        return recordOrNull;
    }
    public boolean asBoolean() {
        if (isOk()) {
            return true;
        }
        else if (this.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            return false;
        }
        orThrow();
        // Just to keep the compiler happy
        return false;
    }

}
