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
public final class RecordResult {
    private final Key key;
    private final Record rec;
    private final Object udfReturnValue;
    private final AerospikeException exception;
    private final String message;
    private final ExpressionTrace expTrace;
    private final Session readMappingSession;
    private final Class<?> readMappingClass;
    private final int resultCode;
    private final int subCode;
    private final int index;
    private final boolean inDoubt;

    /**
     * Single record result.
     */
    public RecordResult(Key key, Record rec, int index) {
        this.key = key;
        this.rec = rec;
        this.udfReturnValue = null;
        this.exception = null;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = null;
        this.readMappingClass = null;
        this.resultCode = ResultCode.OK;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * Single record object mapper result.
     */
    public RecordResult(
        Key key, Record rec, int index, Session readMappingSession, Class<?> readMappingClass
    ) {
        this.key = key;
        this.rec = rec;
        this.udfReturnValue = null;
        this.exception = null;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = readMappingSession;
        this.readMappingClass = readMappingClass;
        this.resultCode = ResultCode.OK;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * Single record server returned error.
     */
    public RecordResult(Key key, int resultCode, boolean inDoubt, String message, int index) {
        this.key = key;
        this.rec = null;
        this.udfReturnValue = null;
        this.exception = null;
        this.message = message;
        this.expTrace = null;
        this.readMappingSession = null;
        this.readMappingClass = null;
        this.resultCode = resultCode;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = inDoubt;
    }

    /**
     * Single record client exception.
     */
    RecordResult(Key key, AerospikeException ae, int index) {
        this.key = key;
        this.rec = null;
        this.udfReturnValue = null;
        this.exception = ae;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = null;
        this.readMappingClass = null;
        this.resultCode = ResultCode.OK;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * Query record result.
     */
    public RecordResult(KeyRecord keyRecord, int index) {
        this.key = keyRecord.key;
        this.rec = keyRecord.record;
        this.udfReturnValue = null;
        this.exception = null;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = null;
        this.readMappingClass = null;
        this.resultCode = ResultCode.OK;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * UDF success result with optional read-mapping context (typed UDF via {@link TypedKey}).
     */
    public RecordResult(
        Key key, Object udfReturnValue, int index, Session readMappingSession,
        Class<?> readMappingClass
    ) {
        this.key = key;
        this.rec = null;
        this.udfReturnValue = udfReturnValue;
        this.exception = null;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = readMappingSession;
        this.readMappingClass = readMappingClass;
        this.resultCode = ResultCode.OK;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * Batch record result.
     */
    private RecordResult(
        BatchRecord br, int index, Session readMappingSession, Class<?> readMappingClass
    ) {
        this.key = br.key;
        this.rec = br.record;
        this.udfReturnValue = null;
        this.exception = null;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = readMappingSession;
        this.readMappingClass = readMappingClass;
        this.resultCode = br.resultCode;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * Batch record server returned error.
     */
    private RecordResult(
        BatchRecord br, int index, Session readMappingSession, Class<?> readMappingClass,
        boolean error
    ) {
        this.key = br.key;
        this.rec = null;
        this.udfReturnValue = null;
        this.exception = null;
        this.message = getMessage(br);
        this.expTrace = br.expTrace;
        this.readMappingSession = readMappingSession;
        this.readMappingClass = readMappingClass;
        this.resultCode = br.resultCode;
        this.subCode = br.subCode;
        this.index = index;
        this.inDoubt = br.inDoubt;
    }

    /**
     * Batch record client exception.
     */
    private RecordResult(BatchRecord br, int index, AerospikeException ae) {
        this.key = br.key;
        this.rec = null;
        this.udfReturnValue = null;
        this.exception = ae;
        this.message = null;
        this.expTrace = null;
        this.readMappingSession = null;
        this.readMappingClass = null;
        this.resultCode = br.resultCode;
        this.subCode = SubCode.NONE;
        this.index = index;
        this.inDoubt = false;
    }

    /**
     * Batch record result.
     */
    public static RecordResult batchRecord(BatchRecord br, int index) {
        return new RecordResult(br, index, null, null);
    }

    /**
     * Batch record object mapper result.
     */
    public static RecordResult batchRecord(
        BatchRecord br, int index, Session readMappingSession, Class<?> readMappingClass
    ) {
        return new RecordResult(br, index, readMappingSession, readMappingClass);
    }

    /**
     * Batch record server returned error.
     */
    public static RecordResult batchError(BatchRecord br, int index) {
        return new RecordResult(br, index, null, null, true);
    }

    /**
     * Batch record server returned error with object mapper.
     */
    public static RecordResult batchError(
        BatchRecord br, int index, Session readMappingSession, Class<?> readMappingClass
    ) {
        return new RecordResult(br, index, readMappingSession, readMappingClass, true);
    }

    /**
     * Batch record client exception.
     */
    public static RecordResult batchError(
        BatchRecord br, int index, AerospikeException ae
    ) {
        return new RecordResult(br, index, ae);
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
                throw AerospikeException.toException(resultCode, subCode, message, expTrace, inDoubt);
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
        return rec;
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
                    + "Session#query(TypedKey) / Session#queryTypedKeys / Session#queryTypedKeysAny / TypedKeyList so the SDK records the entity type.");
        }
        RecordMapper<T> mapper = MappingSupport.requireMapper(
            readMappingSession.getRecordMappingFactory(), (Class<T>) readMappingClass);
        orThrow();

        if (rec == null) {
            throw AerospikeException.toException(ResultCode.KEY_NOT_FOUND_ERROR, SubCode.NONE,
                "No record bins to map for key " + key, null, false);
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
            throw AerospikeException.toException(ResultCode.OP_NOT_APPLICABLE,
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
            throw AerospikeException.toException(ResultCode.OP_NOT_APPLICABLE,
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
            throw AerospikeException.toException(ResultCode.OP_NOT_APPLICABLE,
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

    private static String getMessage(BatchRecord br) {
        return (br.message != null)?  br.message : ResultCode.getResultString(br.resultCode);
    }

    /**
     * Convert to an exception.
     */
    public AerospikeException toException() {
        return (exception != null) ?
            exception :
            AerospikeException.toException(resultCode, subCode, message, expTrace, inDoubt);
    }

    /**
     * Return key.
     */
    public Key getKey() {
        return key;
    }

    /**
     * Return record.
     */
    public Record getRecord() {
        return rec;
    }

    /**
     * Return UDF result.
     */
    public Object getUdfReturnValue() {
        return udfReturnValue;
    }

    /**
     * Return exception which may be null.
     */
    public AerospikeException getException() {
        return exception;
    }

    /**
     * Return exception message if exception exists.
     * Otherwise, return server returned message.
     */
    public String getMessage() {
        return (exception != null)? exception.getMessage() : message;
    }

    /**
     * Return exception expression trace if exception exists.
     * Otherwise, return server returned expression trace.
     */
    public ExpressionTrace getExpTrace() {
        return (exception != null)? exception.getExpressionTrace() : expTrace;
    }

    /**
     * Return exception resultCode if exception exists.
     * Otherwise, return server returned resultCode.
     */
    public int getResultCode() {
        return (exception != null)? exception.getResultCode() : resultCode;
    }

    /**
     * Return exception subCode if exception exists.
     * Otherwise, return server returned subCode.
     */
    public int getSubCode() {
        return (exception != null)? exception.getSubCode() : subCode;
    }

    /**
     * Return index offset.
     */
    public int getIndex() {
        return index;
    }

    /**
     * Return exception inDoubt if exception exists.
     * Otherwise, return local inDoubt.
     */
    public boolean isInDoubt() {
        return (exception != null)? exception.getInDoubt() : inDoubt;
    }
}
