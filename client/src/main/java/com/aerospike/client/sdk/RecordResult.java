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
import java.util.Optional;

import com.aerospike.client.sdk.command.BatchRecord;
import com.aerospike.client.sdk.query.KeyRecord;

/**
 * Represents the result of a single operation in a batch or standalone execution.
 * Contains the key, record data (for record operations), UDF return value (for UDF operations),
 * result code, and error information if the operation failed.
 *
 * <p>When {@link #readMappingClass()} is non-null after a typed read, {@link #readMappingSession()}
 * is also set so {@link #toObject()} and {@link #udfResultAsObject()} can resolve mappers from the
 * session's {@link RecordMappingFactory}. Those references are typically short-lived compared to
 * the session itself.</p>
 *
 * @param readMappingSession when non-null, session paired with {@code readMappingClass}; must be
 *        {@code null} iff {@code readMappingClass} is {@code null}
 * @param readMappingClass when non-null, domain type hint for {@link #toObject()} after reads
 *        issued via {@link TypedKey} (or equivalent batch hints)
 */
public record RecordResult(
    Key key,
    Record recordOrNull,
    Object udfReturnValue,
    int resultCode,
    AerospikeException exception,
    boolean inDoubt,
    String message,
    int index,
    Session readMappingSession,
    Class<?> readMappingClass) {

    public RecordResult {
        if ((readMappingSession == null) != (readMappingClass == null)) {
            throw new IllegalArgumentException(
                "readMappingSession and readMappingClass must both be null or both non-null");
        }
    }

    public RecordResult(Key key, Record rec, int index) {
        this(key, rec, null, ResultCode.OK, null, false, null, index, null, null);
    }

    public RecordResult(Key key, Record rec, int index, Session readMappingSession, Class<?> readMappingClass) {
        this(key, rec, null, ResultCode.OK, null, false, null, index, readMappingSession, readMappingClass);
    }

    RecordResult(Key key, int resultCode, boolean inDoubt, String message, int index) {
        this(key, null, null, resultCode, null, inDoubt, message, index, null, null);
    }

    RecordResult(Key key, AerospikeException ae, int index) {
        this(key, null, null, ae.getResultCode(), ae, ae.getInDoubt(), ae.getMessage(), index, null, null);
    }

    public RecordResult(KeyRecord keyRecord, int index) {
        this(keyRecord.key, keyRecord.record, null, ResultCode.OK, null, false, null, index, null, null);
    }

    public RecordResult(BatchRecord batchRecord, int index) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode, null, batchRecord.inDoubt,
            ResultCode.getResultString(batchRecord.resultCode), index, null, null);
    }

    public RecordResult(BatchRecord batchRecord, int index, Session readMappingSession, Class<?> readMappingClass) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode, null, batchRecord.inDoubt,
            ResultCode.getResultString(batchRecord.resultCode), index, readMappingSession, readMappingClass);
    }

    public RecordResult(BatchRecord batchRecord, AerospikeException ae, int index) {
        this(batchRecord, ae, index, null, null);
    }

    public RecordResult(BatchRecord batchRecord, AerospikeException ae, int index,
        Session readMappingSession, Class<?> readMappingClass) {
        this(batchRecord.key, batchRecord.record, null, batchRecord.resultCode, ae, batchRecord.inDoubt,
            ResultCode.getResultString(batchRecord.resultCode), index, readMappingSession, readMappingClass);
    }

    // Constructor with error handling based on stackTraceOnException flag
    RecordResult(Key key, int resultCode, boolean inDoubt, String message, boolean stackTraceOnException, int index) {
        this(key, null, null, resultCode,
             stackTraceOnException && AbstractFilterableBuilder.isActionableError(resultCode) ?
                 createExceptionWithCleanedStackTrace(resultCode, message, inDoubt) : null,
             inDoubt, message, index, null, null);
    }

    /**
     * UDF success result with optional read-mapping context (typed UDF via {@link TypedKey}).
     *
     * @param readMappingSession session when {@code readMappingClass} is non-null
     */
    public RecordResult(Key key, Object udfReturnValue, int index, Session readMappingSession, Class<?> readMappingClass) {
        this(key, null, udfReturnValue, ResultCode.OK, null, false, null, index, readMappingSession, readMappingClass);
    }

    /**
     * Constructor for UDF results.
     *
     * @param key the key the UDF was executed on
     * @param udfReturnValue the value returned by the UDF
     * @param index the index in the batch operation
     */
    RecordResult(Key key, Object udfReturnValue, int index) {
        this(key, null, udfReturnValue, ResultCode.OK, null, false, null, index, null, null);
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
        this(key, null, udfReturnValue, ae.getResultCode(), ae, ae.getInDoubt(), ae.getMessage(), index, null, null);
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
            if (!className.equals("com.aerospike.client.sdk.RecordResult") &&
                !className.equals("com.aerospike.RecordResult") &&
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
     * Maps this successful read result to a Java object using the {@link RecordMappingFactory}
     * on the embedded session when this result carries a type hint from a {@link TypedKey} read.
     *
     * @param <T> domain type (must match {@link #readMappingClass()})
     * @return mapped object
     * @throws IllegalStateException if no type hint or session is present
     */
    @SuppressWarnings("unchecked")
    public <T> T toObject() {
        if (readMappingClass == null || readMappingSession == null) {
            throw new IllegalStateException(
                "No read mapping context on this result. Use RecordMapper explicitly "
                    + "(e.g. RecordStream#getFirst(RecordMapper)) or perform the read via "
                    + "Session#query(TypedKey) / Session#queryTypedKeys / Session#queryTypedKeysAny so the SDK records the entity type.");
        }
        RecordMapper<T> mapper = MappingSupport.requireMapper(
            readMappingSession.getRecordMappingFactory(), (Class<T>) readMappingClass);
        orThrow();
        Record rec = recordOrNull;
        if (rec == null) {
            throw AerospikeException.resultCodeToException(ResultCode.KEY_NOT_FOUND_ERROR,
                "No record bins to map for key " + key, false);
        }
        RecordReadContext<T> ctx = new RecordReadContext<>(readMappingSession, (Class<T>) readMappingClass);
        return mapper.fromMap(rec.bins, key, rec.generation, ctx);
    }

    /**
     * Returns true if this result contains a UDF return value.
     */
    public boolean hasUdfResult() {
        return udfReturnValue != null;
    }

    /**
     * Maps the UDF return value using the session's {@link RecordMappingFactory} for
     * {@link #readMappingClass()}. Requires {@link #readMappingSession()} and {@link #readMappingClass()}
     * (e.g. typed read / batch hint); otherwise throws {@link IllegalStateException}.
     *
     * <p>If the UDF returned {@code null}, returns {@link Optional#empty()}.</p>
     *
     * @param <T> expected type (must match the embedded read mapping class)
     * @return mapped value, or empty if the UDF result is null
     * @throws AerospikeException if the operation was not successful or the value is not a map
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> udfResultAsObject() {
        if (readMappingSession == null || readMappingClass == null) {
            throw new IllegalStateException(
                "No read mapping context on this result for mapper-less UDF mapping. "
                    + "Use udfResultAsObject(RecordMapper) or udfResultAsObject(RecordMapper, RecordReadContext), "
                    + "or perform the operation via a typed key path once hints are propagated.");
        }
        orThrow();
        if (udfReturnValue == null) {
            return Optional.empty();
        }
        if (!(udfReturnValue instanceof Map)) {
            throw AerospikeException.resultCodeToException(ResultCode.OP_NOT_APPLICABLE,
                "UDF result is not a Map, cannot use RecordMapper. Actual type: "
                    + udfReturnValue.getClass().getName());
        }
        Map<String, Object> map = (Map<String, Object>) udfReturnValue;
        RecordMapper<T> mapper = MappingSupport.requireMapper(
            readMappingSession.getRecordMappingFactory(), (Class<T>) readMappingClass);
        RecordReadContext<T> ctx = new RecordReadContext<>(readMappingSession, (Class<T>) readMappingClass);
        return Optional.of(mapper.fromMap(map, key, 0, ctx));
    }

    /**
     * Returns the UDF return value converted using a {@link RecordMapper}.
     *
     * <p>UDFs written in Lua return Lua types that map to Java types (String, Long, Map, List, etc.).
     * When a UDF returns a Lua table, it becomes a {@code Map<String, Object>} in Java.</p>
     *
     * @param <T> the expected return type
     * @param mapper the RecordMapper to use for converting the UDF result Map to the target type
     * @return present mapped value, or {@link Optional#empty()} if the UDF returned null
     * @throws AerospikeException if the operation was not successful or the value is not a map
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> udfResultAsObject(RecordMapper<T> mapper) {
        orThrow();
        if (udfReturnValue == null) {
            return Optional.empty();
        }
        if (!(udfReturnValue instanceof Map)) {
            throw AerospikeException.resultCodeToException(ResultCode.OP_NOT_APPLICABLE,
                "UDF result is not a Map, cannot use RecordMapper. Actual type: "
                    + udfReturnValue.getClass().getName());
        }
        Map<String, Object> map = (Map<String, Object>) udfReturnValue;
        return Optional.of(mapper.fromMap(map, key, 0));
    }

    /**
     * Like {@link #udfResultAsObject(RecordMapper)} but passes {@link RecordReadContext} into
     * {@link RecordMapper#fromMap(Map, Key, int, RecordReadContext)} so mappers can use the session
     * or {@link RecordReadContext#getRecordMappingFactory()} (e.g. dependent loads).
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> udfResultAsObject(RecordMapper<T> mapper, RecordReadContext<T> ctx) {
        orThrow();
        if (udfReturnValue == null) {
            return Optional.empty();
        }
        if (!(udfReturnValue instanceof Map)) {
            throw AerospikeException.resultCodeToException(ResultCode.OP_NOT_APPLICABLE,
                "UDF result is not a Map, cannot use RecordMapper. Actual type: "
                    + udfReturnValue.getClass().getName());
        }
        Map<String, Object> map = (Map<String, Object>) udfReturnValue;
        return Optional.of(mapper.fromMap(map, key, 0, ctx));
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
