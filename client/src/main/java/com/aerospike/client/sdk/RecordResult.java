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
package com.aerospike.client.sdk;

import java.util.Map;

import com.aerospike.client.sdk.command.BatchRecord;
import com.aerospike.client.sdk.query.KeyRecord;

/**
 * Represents the result of a single operation in a batch or standalone execution.
 * Contains the key, record data (for record operations), UDF return value (for UDF operations),
 * result code, and error information if the operation failed.
 */
public record RecordResult(Key key, Record recordOrNull, Object udfReturnValue, int resultCode, AerospikeException exception, boolean inDoubt, String message, int index) {

    public RecordResult(Key key, Record rec, int index) {
        this(key, rec, null, ResultCode.OK, null, false, null, index);
    }

    RecordResult(Key key, int resultCode, boolean inDoubt, String message, int index) {
        this(key, null, null, resultCode, null, inDoubt, message, index);
    }

    RecordResult(Key key, AerospikeException ae, int index) {
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
    RecordResult(Key key, int resultCode, boolean inDoubt, String message, boolean stackTraceOnException, int index) {
        this(key, null, null, resultCode,
             stackTraceOnException && AbstractFilterableBuilder.isActionableError(resultCode) ?
                 createExceptionWithCleanedStackTrace(resultCode, message, inDoubt) : null,
             inDoubt, message, index);
    }

    // Constructor for BatchRecord with error handling
    RecordResult(BatchRecord batchRecord, boolean stackTraceOnException, int index) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode,
             stackTraceOnException && AbstractFilterableBuilder.isActionableError(batchRecord.resultCode) ?
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
    RecordResult(Key key, Object udfReturnValue, int index) {
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
    RecordResult(Key key, Object udfReturnValue, AerospikeException ae, int index) {
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

    /**
     * Whether the operation completed with {@link ResultCode#OK}.
     *
     * @return {@code true} if successful; {@code false} if any other result code
     */
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

    /**
     * Returns the record payload after {@link #orThrow()}.
     *
     * @return the Aerospike {@link Record}, or {@code null} when no record is attached to this result
     * @throws AerospikeException if {@link #isOk()} is false
     */
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
            throw AerospikeException.resultCodeToException(ResultCode.OP_NOT_APPLICABLE,
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

    /**
     * Interprets the result as a boolean, for example after an {@code exists} call.
     * {@link ResultCode#OK} maps to {@code true}, {@link ResultCode#KEY_NOT_FOUND_ERROR} to {@code false};
     * any other code triggers {@link #orThrow()}.
     *
     * @return {@code true} if OK, {@code false} if key not found
     * @throws AerospikeException for other failure codes
     */
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
