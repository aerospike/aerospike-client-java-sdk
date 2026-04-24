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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import com.aerospike.client.sdk.command.Txn;

/**
 * Abstract base class for session-based operation builders that provides common functionality
 * for building database operations without bin-specific operations.
 *
 * <p>This class contains shared functionality for operations including expiration management,
 * generation handling, transaction support, and filter expressions. It does NOT provide
 * bin-level operations like {@code bin()}, which are available in subclasses that need them.</p>
 *
 * <p>The hierarchy is:
 * <ul>
 *   <li>{@link AbstractSessionOperationBuilder} - Base with session, expiration, generation, transactions (this class)</li>
 *   <li>{@link AbstractOperationBuilder} - Adds bin operations via {@code bin()} method</li>
 *   <li>{@link ChainableNoBinsBuilder} - Direct subclass for no-bin operations (exists, touch, delete)</li>
 * </ul>
 * </p>
 *
 * @param <T> the concrete builder type (for method chaining)
 */
public abstract class AbstractSessionOperationBuilder<T extends AbstractSessionOperationBuilder<T>> extends AbstractFilterableBuilder {
    protected final Session session;
    protected OpType opType;
    protected long expirationInSeconds = 0;  // Default, get value from server
    protected int generation = 0;
    protected Txn txnToUse;
    protected boolean notInAnyTransaction;
    protected boolean transactionSet;
    protected Boolean durableDeleteDefault;

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

    protected AbstractSessionOperationBuilder(Session session, OpType opType) {
        this.session = session;
        this.opType = opType;
        this.txnToUse = session.getCurrentTransaction();
    }

    /**
     * Returns this builder cast to the concrete type.
     * Used for method chaining.
     */
    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }

    /**
     * Returns the effective includeMissingKeys value, considering operation type.
     *
     * <p>UPDATE and REPLACE_IF_EXISTS operations must always return KEY_NOT_FOUND_ERROR
     * results because these operations are expected to fail if the record doesn't exist.
     * Users need to see these failures even without explicitly calling includeMissingKeys().</p>
     */
    protected boolean isEffectiveIncludeMissingKeys() {
        return includeMissingKeys || opType == OpType.UPDATE || opType == OpType.REPLACE_IF_EXISTS;
    }

    /**
     * Set the expiration for the record relative to the current time.
     *
     * @param duration The duration after which the record should expire
     * @return This builder for method chaining
     */
    public T expireRecordAfter(Duration duration) {
        this.expirationInSeconds = duration.toSeconds();
        return self();
    }

    /**
     * Set the expiration for the record relative to the current time.
     *
     * @param expirationInSeconds The number of seconds after which the record should expire
     * @return This builder for method chaining
     */
    public T expireRecordAfterSeconds(int expirationInSeconds) {
        this.expirationInSeconds = expirationInSeconds;
        return self();
    }

    /**
     * Validate and calculate expiration from a Date object.
     * <p/>
     * This method is public as it is needed to satisfy the interface and cannot have its visibility reduced
     */
    public long getExpirationInSecondsAndCheckValue(Date date) {
        long expirationInSeconds = (date.getTime() - new Date().getTime()) / 1000L;
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    /**
     * Set the expiration for the record to an absolute date/time.
     *
     * @param date The date at which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if the date is in the past
     */
    public T expireRecordAt(Date date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return self();
    }

    /**
     * Validate and calculate expiration from a LocalDateTime object.
     * <p/>
     * This method is public as it is needed to satisfy the interface and cannot have its visibility reduced
     */
    public long getExpirationInSecondsAndCheckValue(LocalDateTime date) {
        LocalDateTime now = LocalDateTime.now();
        long expirationInSeconds = ChronoUnit.SECONDS.between(now, date);
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    /**
     * Set the expiration for the record to an absolute date/time.
     *
     * @param date The LocalDateTime at which the record should expire
     * @return This builder for method chaining
     * @throws IllegalArgumentException if the date is in the past
     */
    public T expireRecordAt(LocalDateTime date) {
        this.expirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return self();
    }

    /**
     * Do not change the expiration of the record (TTL = -2).
     *
     * @return This builder for method chaining
     */
    public T withNoChangeInExpiration() {
        this.expirationInSeconds = TTL_NO_CHANGE;
        return self();
    }

    /**
     * Set the record to never expire (TTL = -1).
     *
     * @return This builder for method chaining
     */
    public T neverExpire() {
        this.expirationInSeconds = TTL_NEVER_EXPIRE;
        return self();
    }

    /**
     * Use the server's default expiration for the record (TTL = 0).
     *
     * @return This builder for method chaining
     */
    public T expiryFromServerDefault() {
        this.expirationInSeconds = TTL_SERVER_DEFAULT;
        return self();
    }

    /**
     * Convert expiration seconds to int, handling overflow.
     */
    protected int getExpirationAsInt(long expirationInSeconds) {
        if (expirationInSeconds > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) expirationInSeconds;
        }
    }

    /**
     * Get expiration as int for current operation.
     */
    protected int getExpirationAsInt() {
        return getExpirationAsInt(expirationInSeconds);
    }

    /**
     * Ensure the operation only succeeds if the record generation matches.
     *
     * @param generation the expected generation value
     * @return this builder for method chaining
     * @throws IllegalArgumentException if generation is &lt;= 0
     */
    public T ensureGenerationIs(int generation) {
        if (generation <= 0) {
            throw new IllegalArgumentException("Generation must be greater than 0");
        }
        this.generation = generation;
        return self();
    }

    protected Txn getTxnToUse() {
        return this.txnToUse;
    }

    protected boolean getNotInAnyTransaction() {
        return this.notInAnyTransaction;
    }

    protected OpType getOpType() {
        return this.opType;
    }

    /**
     * Specify that these operations are not to be included in any transaction, even if a
     * transaction exists on the underlying session
     */
    public T notInAnyTransaction() {
        if (transactionSet) {
            throw AerospikeException.resultCodeToException(ResultCode.PARAMETER_ERROR,
                "The transaction mode has already been set");
        }
        this.transactionSet = true;
        this.notInAnyTransaction = true;
        this.txnToUse = null;
        return self();
    }

    /**
     * Specify the transaction to use for this call.
     *
     * @param txn - the transaction to use
     */
    public T inTransaction(Txn txn) {
        if (transactionSet) {
            throw AerospikeException.resultCodeToException(ResultCode.PARAMETER_ERROR,
                "The transaction mode has already been set");
        }
        this.transactionSet = true;
        this.txnToUse = txn;
        this.notInAnyTransaction = false;
        return self();
    }

    /**
     * Get the session associated with this builder.
     */
    protected Session getSession() {
        return this.session;
    }

    /**
     * Enable durable delete for this operation.
     * <p>
     * If the command results in a record deletion, leave a tombstone for the record.
     * This prevents deleted records from reappearing after node failures.
     * Valid for Aerospike Server Enterprise Edition only.
     * </p>
     *
     * @return this builder for method chaining
     */
    public T defaultWithDurableDelete() {
        this.durableDeleteDefault = true;
        return self();
    }

    /**
     * Disable durable delete for this operation (default behavior).
     *
     * @return this builder for method chaining
     */
    public T defaultWithoutDurableDelete() {
        this.durableDeleteDefault = false;
        return self();
    }
}

