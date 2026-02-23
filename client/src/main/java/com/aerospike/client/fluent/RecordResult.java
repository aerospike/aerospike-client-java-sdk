package com.aerospike.client.fluent;

import java.util.Map;

import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.query.KeyRecord;

/**
 * Represents the result of a single operation in a batch or standalone execution.
 * Contains the key, record data (for record operations), UDF return value (for UDF operations),
 * result code, and error information if the operation failed.
 */
public record RecordResult(Key key, Record recordOrNull, Object udfReturnValue, int resultCode, AerospikeException exception, boolean inDoubt, String message, int index) {

    public RecordResult(Key key, Record rec, int index) {
        this(key, rec, null, ResultCode.OK, null, false, null, index);
    }
    
    public RecordResult(Key key, int resultCode, boolean inDoubt, String message, int index) {
        this(key, null, null, resultCode, null, inDoubt, message, index);
    }
    
    public RecordResult(Key key, AerospikeException ae, int index) {
        this(key, null, null, ae.getResultCode(), ae, ae.getInDoubt(), ae.getMessage(), index);
    }
    
    public RecordResult(KeyRecord keyRecord, int index) {
        this(keyRecord.key, keyRecord.record, null, ResultCode.OK, null, false, null, index);
    }
    
    public RecordResult(BatchRecord batchRecord, int index) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode, null, batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode), index);
    }
    
    public RecordResult(BatchRecord batchRecord, AerospikeException ae, int index) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode, ae, batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode), index);
    }
    
    // Constructor with error handling based on stackTraceOnException flag
    public RecordResult(Key key, int resultCode, boolean inDoubt, String message, boolean stackTraceOnException, int index) {
        this(key, null, null, resultCode, 
             stackTraceOnException && resultCode != ResultCode.OK ? 
                 createExceptionWithCleanedStackTrace(resultCode, message, inDoubt) : null,
             inDoubt, message, index);
    }
    
    // Constructor for BatchRecord with error handling
    public RecordResult(BatchRecord batchRecord, boolean stackTraceOnException, int index) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode,
             stackTraceOnException && batchRecord.resultCode != ResultCode.OK ?
                 createExceptionWithCleanedStackTrace(batchRecord.resultCode, 
                     ResultCode.getResultString(batchRecord.resultCode), batchRecord.inDoubt) : null,
             batchRecord.inDoubt, ResultCode.getResultString(batchRecord.resultCode), index);
    }
    
    /**
     * Constructor for UDF results.
     * 
     * @param key the key the UDF was executed on
     * @param udfReturnValue the value returned by the UDF
     * @param index the index in the batch operation
     */
    public RecordResult(Key key, Object udfReturnValue, int index) {
        this(key, null, udfReturnValue, ResultCode.OK, null, false, null, index);
    }
    
    /**
     * Constructor for UDF results with error.
     * 
     * @param key the key the UDF was executed on
     * @param udfReturnValue the value returned by the UDF (may be null on error)
     * @param ae the exception that occurred
     * @param index the index in the batch operation
     */
    public RecordResult(Key key, Object udfReturnValue, AerospikeException ae, int index) {
        this(key, null, udfReturnValue, ae.getResultCode(), ae, ae.getInDoubt(), ae.getMessage(), index);
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
    
    /**
     * Returns true if this result contains a UDF return value.
     */
    public boolean hasUdfResult() {
        return udfReturnValue != null;
    }
    
    /**
     * Returns the UDF return value converted to the specified type using a RecordMapper.
     * 
     * <p>UDFs written in Lua return Lua types that map to Java types (String, Long, Map, List, etc.).
     * When a UDF returns a Lua table, it becomes a {@code Map<String, Object>} in Java. This method
     * uses the provided RecordMapper to convert that Map to a typed object.</p>
     * 
     * @param <T> the expected return type
     * @param mapper the RecordMapper to use for converting the UDF result Map to the target type
     * @return the UDF return value converted to the specified type
     * @throws AerospikeException if the operation was not successful
     * @throws ClassCastException if the UDF result is not a Map and cannot be converted
     */
    @SuppressWarnings("unchecked")
    public <T> T udfResultAs(RecordMapper<T> mapper) {
        orThrow();
        if (udfReturnValue == null) {
            return null;
        }
        if (!(udfReturnValue instanceof Map)) {
            throw new AerospikeException(ResultCode.OP_NOT_APPLICABLE, 
                    "UDF result is not a Map, cannot use RecordMapper. Actual type: " 
                + udfReturnValue.getClass().getName());
        }
        Map<String, Object> map = (Map<String, Object>) udfReturnValue;
        return mapper.fromMap(map, key, 0);
    }
    
    /**
     * Returns the UDF return value, throwing if the operation was not successful.
     * 
     * @return the UDF return value (may be null if the UDF returned null)
     * @throws AerospikeException if the operation was not successful
     */
    public Object udfResultOrThrow() {
        orThrow();
        return udfReturnValue;
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
