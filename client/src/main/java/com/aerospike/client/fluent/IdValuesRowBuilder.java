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
package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.aerospike.client.fluent.command.Txn;

/**
 * Builder that accumulates id+values rows for the bins/id/values pattern.
 * Each call to {@code .id(x).values(...)} defines one record, co-locating
 * the key with its data.
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * session.upsert(set)
 *     .bins("name", "age")
 *     .id(1).values("Tim", 30)
 *     .id(2).values("Jane", 28).expireRecordAfter(Duration.ofDays(5))
 *     .id(3).values("Bob", 54)
 *     .defaultExpireRecordAfter(Duration.ofDays(30))
 *     .execute();
 * }</pre>
 *
 * @see IdValuesBuilder
 */
public class IdValuesRowBuilder {
    private static class RowData {
        final Key key;
        Object[] values;
        int generation = 0;
        long expirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;

        RowData(Key key) {
            this.key = key;
        }
    }

    private final Session session;
    private final DataSet dataSet;
    private final OpType opType;
    private final String[] binNames;
    private final List<RowData> rows = new ArrayList<>();
    private RowData currentRow = null;
    private long defaultExpirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;
    private Txn txnToUse;

    IdValuesRowBuilder(Session session, DataSet dataSet, OpType opType, String[] binNames) {
        this.session = session;
        this.dataSet = dataSet;
        this.opType = opType;
        this.binNames = binNames;
        this.txnToUse = session.getCurrentTransaction();
    }

    private void finalizeCurrentRow() {
        if (currentRow != null) {
            if (currentRow.values == null) {
                throw new IllegalStateException(
                    "id() was called without a subsequent values() call. Each id() must be followed by values().");
            }
            rows.add(currentRow);
            currentRow = null;
        }
    }

    // ========================================
    // Row definition methods
    // ========================================

    /**
     * Begin a row for the record with the given integer key.
     *
     * @param id the integer identifier for the record
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder id(int id) {
        finalizeCurrentRow();
        currentRow = new RowData(dataSet.id(id));
        return this;
    }

    /**
     * Begin a row for the record with the given long key.
     *
     * @param id the long identifier for the record
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder id(long id) {
        finalizeCurrentRow();
        currentRow = new RowData(dataSet.id(id));
        return this;
    }

    /**
     * Begin a row for the record with the given String key.
     *
     * @param id the String identifier for the record
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder id(String id) {
        finalizeCurrentRow();
        currentRow = new RowData(dataSet.id(id));
        return this;
    }

    /**
     * Begin a row for the record with the given byte array key.
     *
     * @param id the byte array identifier for the record
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder id(byte[] id) {
        finalizeCurrentRow();
        currentRow = new RowData(dataSet.id(id));
        return this;
    }

    /**
     * Specify the values for the current row. The number of values must match
     * the number of bin names declared in {@code .bins(...)}.
     *
     * @param values the values for this record's bins
     * @return this builder for method chaining
     * @throws IllegalStateException if id() has not been called
     * @throws IllegalArgumentException if the value count doesn't match the bin count
     */
    public IdValuesRowBuilder values(Object... values) {
        if (currentRow == null) {
            throw new IllegalStateException("values() must be called after id()");
        }
        if (currentRow.values != null) {
            throw new IllegalStateException("values() has already been called for this id. Call id() to start a new row.");
        }
        if (values.length != binNames.length) {
            throw new IllegalArgumentException(String.format(
                "The number of values (%d) must match the number of bins (%d) specified in .bins(...).",
                values.length, binNames.length));
        }
        currentRow.values = values;
        return this;
    }

    // ========================================
    // Per-record policies (apply to most recent .values() call)
    // ========================================

    private void checkValuesExist(String methodName) {
        if (currentRow == null || currentRow.values == null) {
            throw new IllegalStateException(
                methodName + "() must be called after values() to apply to a specific record.");
        }
    }

    /**
     * Ensure the operation only succeeds if the record generation matches.
     * Applies to the most recent {@code .values()} call.
     *
     * @param generation the expected generation value
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder ensureGenerationIs(int generation) {
        if (generation <= 0) {
            throw new IllegalArgumentException("Generation must be greater than 0");
        }
        checkValuesExist("ensureGenerationIs");
        currentRow.generation = generation;
        return this;
    }

    /**
     * Set the expiration for this record relative to the current time.
     *
     * @param duration the duration after which the record should expire
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder expireRecordAfter(Duration duration) {
        checkValuesExist("expireRecordAfter");
        currentRow.expirationInSeconds = duration.toSeconds();
        return this;
    }

    /**
     * Set the expiration for this record in seconds.
     *
     * @param expirationInSeconds the number of seconds after which the record should expire
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        checkValuesExist("expireRecordAfterSeconds");
        currentRow.expirationInSeconds = expirationInSeconds;
        return this;
    }

    /**
     * Set the expiration for this record to an absolute date/time.
     *
     * @param date the date at which the record should expire
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder expireRecordAt(Date date) {
        checkValuesExist("expireRecordAt");
        long seconds = (date.getTime() - new Date().getTime()) / 1000L;
        if (seconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        currentRow.expirationInSeconds = seconds;
        return this;
    }

    /**
     * Set the expiration for this record to an absolute date/time.
     *
     * @param date the date/time at which the record should expire
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder expireRecordAt(LocalDateTime date) {
        checkValuesExist("expireRecordAt");
        long seconds = ChronoUnit.SECONDS.between(LocalDateTime.now(), date);
        if (seconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        currentRow.expirationInSeconds = seconds;
        return this;
    }

    /**
     * Do not change the expiration of this record (TTL = -2).
     *
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder withNoChangeInExpiration() {
        checkValuesExist("withNoChangeInExpiration");
        currentRow.expirationInSeconds = AbstractOperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set this record to never expire (TTL = -1).
     *
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder neverExpire() {
        checkValuesExist("neverExpire");
        currentRow.expirationInSeconds = AbstractOperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Use the server's default expiration for this record (TTL = 0).
     *
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder expiryFromServerDefault() {
        checkValuesExist("expiryFromServerDefault");
        currentRow.expirationInSeconds = AbstractOperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    // ========================================
    // Default policies (apply to all records without explicit per-record settings)
    // ========================================

    /**
     * Set the default expiration for all records that don't have an explicit per-record expiration.
     *
     * @param duration the duration after which records should expire
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder defaultExpireRecordAfter(Duration duration) {
        this.defaultExpirationInSeconds = duration.getSeconds();
        return this;
    }

    /**
     * Set the default expiration for all records that don't have an explicit per-record expiration.
     *
     * @param seconds the number of seconds after which records should expire
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder defaultExpireRecordAfterSeconds(long seconds) {
        this.defaultExpirationInSeconds = seconds;
        return this;
    }

    /**
     * Set all records without explicit expiration to never expire (TTL = -1).
     *
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder defaultNeverExpire() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Set all records without explicit expiration to use the server default (TTL = 0).
     *
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder defaultExpiryFromServerDefault() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    // ========================================
    // Transaction support
    // ========================================

    /**
     * Specify that these operations are not to be included in any transaction.
     *
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder notInAnyTransaction() {
        this.txnToUse = null;
        return this;
    }

    /**
     * Specify the transaction to use for this call.
     *
     * @param txn the transaction to use
     * @return this builder for method chaining
     */
    public IdValuesRowBuilder inTransaction(Txn txn) {
        this.txnToUse = txn;
        return this;
    }

    // ========================================
    // Execution
    // ========================================

    /**
     * Execute all accumulated rows as a batch operation.
     *
     * @return RecordStream containing the results
     * @throws IllegalStateException if no rows have been defined
     */
    public RecordStream execute() {
        List<OperationSpec> specs = materializeToSpecs();
        return OperationSpecExecutor.execute(session, specs, null, defaultExpirationInSeconds, txnToUse);
    }

    /**
     * Execute all accumulated rows asynchronously with errors embedded in the stream.
     *
     * @param strategy the error strategy (must not be null)
     * @return RecordStream that will be populated as results arrive
     * @throws IllegalStateException if no rows have been defined
     */
    public RecordStream executeAsync(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeAsyncInternal(null);
    }

    /**
     * Execute all accumulated rows asynchronously with errors dispatched to the handler.
     * Error results are excluded from the returned stream.
     *
     * @param handler the error handler callback (must not be null)
     * @return RecordStream containing only successful results
     * @throws IllegalStateException if no rows have been defined
     */
    public RecordStream executeAsync(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        return executeAsyncInternal(handler);
    }

    private RecordStream executeAsyncInternal(ErrorHandler errorHandler) {
        List<OperationSpec> specs = materializeToSpecs();

        if (txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using execute() for transactional safety."
            );
        }

        int totalKeys = specs.stream().mapToInt(spec -> spec.getKeys().size()).sum();
        AsyncRecordStream asyncStream = new AsyncRecordStream(totalKeys);

        Cluster cluster = session.getCluster();
        cluster.startVirtualThread(() -> {
            try {
                RecordStream syncResult = OperationSpecExecutor.execute(
                    session, specs, null, defaultExpirationInSeconds, txnToUse);
                syncResult.forEach(result -> AbstractFilterableBuilder.dispatchResult(result, asyncStream, errorHandler));
            } finally {
                asyncStream.complete();
            }
        });

        return new RecordStream(asyncStream);
    }


    private List<OperationSpec> materializeToSpecs() {
        finalizeCurrentRow();

        if (rows.isEmpty()) {
            throw new IllegalStateException("No rows defined. Call .id(x).values(...) at least once.");
        }

        List<OperationSpec> specs = new ArrayList<>(rows.size());
        for (RowData row : rows) {
            OperationSpec spec = new OperationSpec(List.of(row.key), opType);
            for (int i = 0; i < binNames.length; i++) {
                spec.getOperations().add(Operation.put(new Bin(binNames[i], Value.get(row.values[i]))));
            }
            if (row.generation > 0) {
                spec.setGeneration(row.generation);
            }
            if (row.expirationInSeconds != AbstractOperationBuilder.NOT_EXPLICITLY_SET) {
                spec.setExpirationInSeconds(row.expirationInSeconds);
            }
            specs.add(spec);
        }
        return specs;
    }
}
