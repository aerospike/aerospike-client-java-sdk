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
import com.aerospike.client.sdk.policy.CommitLevel;
import com.aerospike.client.sdk.policy.ReadModeAP;
import com.aerospike.client.sdk.policy.ReadModeSC;
import com.aerospike.client.sdk.policy.Replica;

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

    // Timeout settings (null = use default from behavior/policy)
    protected Integer connectTimeout = null;
    protected Integer socketTimeout = null;
    protected Integer totalTimeout = null;
    protected Integer timeoutDelay = null;

    // Retry settings
    protected Integer maxRetries = null;
    protected Integer sleepBetweenRetries = null;

    // Read mode settings
    protected ReadModeAP readModeAP = null;
    protected ReadModeSC readModeSC = null;
    protected Replica replica = null;
    protected Integer readTouchTtlPercent = null;

    // Write behavior settings
    protected CommitLevel commitLevel = null;
    protected Boolean durableDelete = null;
    protected Boolean onLockingOnly = null;
    protected Boolean xdr = null;

    // Other settings
    protected Boolean sendKey = null;
    protected Boolean compress = null;

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
     * {@inheritDoc}
     *
     * <p>Overridden to ensure UPDATE and REPLACE_IF_EXISTS operations always include
     * KEY_NOT_FOUND_ERROR results, since these operations are expected to fail if the
     * record doesn't exist.</p>
     */
    @Override
    public boolean shouldIncludeResult(int resultCode) {
        if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            // UPDATE and REPLACE_IF_EXISTS must report KEY_NOT_FOUND_ERROR because
            // these operations are semantically expected to fail on non-existent records.
            return isEffectiveIncludeMissingKeys();
        }
        return super.shouldIncludeResult(resultCode);
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
     * @throws IllegalArgumentException if generation is <= 0
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

    // ========================================================================
    // TIMEOUT SETTINGS - API Methods
    // ========================================================================

    /**
     * Set the socket connection timeout for this operation.
     * <p>
     * Socket connection timeout in milliseconds. If connectTimeout is zero,
     * there will be no connection timeout.
     * </p>
     *
     * @param connectTimeout timeout in milliseconds
     * @return this builder for method chaining
     */
    public T withConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return self();
    }

    /**
     * Set the socket connection timeout for this operation.
     *
     * @param duration timeout duration
     * @return this builder for method chaining
     */
    public T withConnectTimeout(Duration duration) {
        this.connectTimeout = (int) duration.toMillis();
        return self();
    }

    /**
     * Set the socket idle timeout for this operation.
     * <p>
     * Socket idle timeout in milliseconds. If socketTimeout is zero and
     * totalTimeout is non-zero, there will be no socket idle timeout.
     * </p>
     *
     * @param socketTimeout timeout in milliseconds
     * @return this builder for method chaining
     */
    public T withSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return self();
    }

    /**
     * Set the socket idle timeout for this operation.
     *
     * @param duration timeout duration
     * @return this builder for method chaining
     */
    public T withSocketTimeout(Duration duration) {
        this.socketTimeout = (int) duration.toMillis();
        return self();
    }

    /**
     * Set the total transaction timeout for this operation.
     * <p>
     * Total transaction timeout in milliseconds. If totalTimeout is not zero and
     * totalTimeout is reached before the operation completes, the operation will abort.
     * </p>
     *
     * @param totalTimeout timeout in milliseconds
     * @return this builder for method chaining
     */
    public T withTotalTimeout(int totalTimeout) {
        this.totalTimeout = totalTimeout;
        return self();
    }

    /**
     * Set the total transaction timeout for this operation.
     *
     * @param duration timeout duration
     * @return this builder for method chaining
     */
    public T withTotalTimeout(Duration duration) {
        this.totalTimeout = (int) duration.toMillis();
        return self();
    }

    /**
     * Set the timeout delay for this operation.
     * <p>
     * Delay in milliseconds after the socket timeout to allow for transaction
     * cleanup. This field is only applicable to asynchronous commands.
     * </p>
     *
     * @param timeoutDelay delay in milliseconds
     * @return this builder for method chaining
     */
    public T withTimeoutDelay(int timeoutDelay) {
        this.timeoutDelay = timeoutDelay;
        return self();
    }

    /**
     * Set the timeout delay for this operation.
     *
     * @param duration delay duration
     * @return this builder for method chaining
     */
    public T withTimeoutDelay(Duration duration) {
        this.timeoutDelay = (int) duration.toMillis();
        return self();
    }

    /**
     * Set all timeouts at once (connect, socket, and total).
     *
     * @param timeout timeout value to apply to all timeout settings (in milliseconds)
     * @return this builder for method chaining
     */
    public T withTimeout(int timeout) {
        this.connectTimeout = timeout;
        this.socketTimeout = timeout;
        this.totalTimeout = timeout;
        return self();
    }

    /**
     * Set all timeouts at once (connect, socket, and total).
     *
     * @param duration timeout duration to apply to all timeout settings
     * @return this builder for method chaining
     */
    public T withTimeout(Duration duration) {
        int millis = (int) duration.toMillis();
        this.connectTimeout = millis;
        this.socketTimeout = millis;
        this.totalTimeout = millis;
        return self();
    }

    // ========================================================================
    // RETRY SETTINGS - API Methods
    // ========================================================================

    /**
     * Set the maximum number of retries for this operation.
     * <p>
     * Maximum number of retries before aborting the current operation.
     * The initial attempt is not counted as a retry.
     * </p>
     *
     * @param maxRetries maximum number of retries
     * @return this builder for method chaining
     */
    public T withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return self();
    }

    /**
     * Set the delay between retries for this operation.
     * <p>
     * Milliseconds to sleep between retries. Only applies when maxRetries &gt; 0.
     * </p>
     *
     * @param sleepBetweenRetries delay in milliseconds
     * @return this builder for method chaining
     */
    public T withSleepBetweenRetries(int sleepBetweenRetries) {
        this.sleepBetweenRetries = sleepBetweenRetries;
        return self();
    }

    /**
     * Set the delay between retries for this operation.
     *
     * @param duration delay duration
     * @return this builder for method chaining
     */
    public T withSleepBetweenRetries(Duration duration) {
        this.sleepBetweenRetries = (int) duration.toMillis();
        return self();
    }

    /**
     * Disable retries for this operation.
     *
     * @return this builder for method chaining
     */
    public T withoutRetries() {
        this.maxRetries = 0;
        return self();
    }

    // ========================================================================
    // READ MODE SETTINGS - API Methods
    // ========================================================================

    /**
     * Set the read mode for AP (availability) namespaces for this operation.
     * <p>
     * Read policy for AP (availability) namespaces. How duplicates should be
     * consulted in a read operation. Only makes a difference during migrations.
     * </p>
     *
     * @param readModeAP read mode (ONE or ALL)
     * @return this builder for method chaining
     */
    public T withReadModeAP(ReadModeAP readModeAP) {
        this.readModeAP = readModeAP;
        return self();
    }

    /**
     * Set the read mode for SC (strong consistency) namespaces for this operation.
     * <p>
     * Read policy for SC (strong consistency) namespaces. Determines SC read
     * consistency options.
     * </p>
     *
     * @param readModeSC read mode (SESSION, LINEARIZE, ALLOW_REPLICA, ALLOW_UNAVAILABLE)
     * @return this builder for method chaining
     */
    public T withReadModeSC(ReadModeSC readModeSC) {
        this.readModeSC = readModeSC;
        return self();
    }

    /**
     * Set the replica selection algorithm for this operation.
     * <p>
     * Determines which node to use for read operations when there are multiple
     * replicas.
     * </p>
     *
     * @param replica replica algorithm (MASTER, MASTER_PROLES, SEQUENCE, PREFER_RACK, RANDOM)
     * @return this builder for method chaining
     */
    public T withReplica(Replica replica) {
        this.replica = replica;
        return self();
    }

    /**
     * Set the read touch TTL percent for this operation.
     * <p>
     * Determine how record TTL (time to live) is affected on reads. When enabled,
     * the server can efficiently operate as a read-based LRU cache.
     * <ul>
     * <li>0: Use server config default-read-touch-ttl-pct for the record's namespace/set</li>
     * <li>-1: Do not reset record TTL on reads</li>
     * <li>1-100: Reset record TTL on reads when within this percentage of the most recent write TTL</li>
     * </ul>
     * </p>
     *
     * @param readTouchTtlPercent percentage value
     * @return this builder for method chaining
     */
    public T withReadTouchTtlPercent(int readTouchTtlPercent) {
        this.readTouchTtlPercent = readTouchTtlPercent;
        return self();
    }

    /**
     * Disable read touch TTL for this operation (do not reset TTL on reads).
     *
     * @return this builder for method chaining
     */
    public T withoutReadTouchTtl() {
        this.readTouchTtlPercent = -1;
        return self();
    }

    // ========================================================================
    // WRITE BEHAVIOR SETTINGS - API Methods
    // ========================================================================

    /**
     * Set the write operation to only create new records (fail if exists).
     *
     * @return this builder for method chaining
     */
    public T createOnly() {
        this.opType = OpType.INSERT;
        return self();
    }

    /**
     * Set the write operation to only update existing records (fail if not exists).
     *
     * @return this builder for method chaining
     */
    public T updateOnly() {
        this.opType = OpType.UPDATE;
        return self();
    }

    /**
     * Set the write operation to replace the entire record (create or replace).
     *
     * @return this builder for method chaining
     */
    public T replaceRecord() {
        this.opType = OpType.REPLACE;
        return self();
    }

    /**
     * Set the write operation to replace only existing records (fail if not exists).
     *
     * @return this builder for method chaining
     */
    public T replaceOnly() {
        this.opType = OpType.REPLACE_IF_EXISTS;
        return self();
    }

    /**
     * Set the commit level for write operations.
     * <p>
     * Desired consistency guarantee when committing a command:
     * <ul>
     * <li>COMMIT_ALL: Wait for master and all replica commits</li>
     * <li>COMMIT_MASTER: Wait for master commit only</li>
     * </ul>
     * </p>
     *
     * @param commitLevel commit level
     * @return this builder for method chaining
     */
    public T withCommitLevel(CommitLevel commitLevel) {
        this.commitLevel = commitLevel;
        return self();
    }

    /**
     * Set commit level to wait for all replicas.
     *
     * @return this builder for method chaining
     */
    public T commitAll() {
        this.commitLevel = CommitLevel.COMMIT_ALL;
        return self();
    }

    /**
     * Set commit level to wait for master only.
     *
     * @return this builder for method chaining
     */
    public T commitMasterOnly() {
        this.commitLevel = CommitLevel.COMMIT_MASTER;
        return self();
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
    public T withDurableDelete() {
        this.durableDelete = true;
        return self();
    }

    /**
     * Disable durable delete for this operation (default behavior).
     *
     * @return this builder for method chaining
     */
    public T withoutDurableDelete() {
        this.durableDelete = false;
        return self();
    }

    /**
     * Execute the write command only if the record is not already locked by this transaction.
     * <p>
     * If true and the record is already locked, the command will throw an exception
     * with MRT_ALREADY_LOCKED error code. Useful for safely retrying non-idempotent writes.
     * </p>
     *
     * @return this builder for method chaining
     */
    public T onlyIfNotLocked() {
        this.onLockingOnly = true;
        return self();
    }

    /**
     * Operate in XDR mode for this operation.
     * <p>
     * Indicates that the write operation should be processed in XDR mode.
     * </p>
     *
     * @return this builder for method chaining
     */
    public T asXdrWrite() {
        this.xdr = true;
        return self();
    }

    // ========================================================================
    // OTHER SETTINGS - API Methods
    // ========================================================================

    /**
     * Send the user-defined key in addition to the hash digest.
     * <p>
     * If true, the key will be stored with the record on the server and can be
     * retrieved later.
     * </p>
     *
     * @return this builder for method chaining
     */
    public T sendKey() {
        this.sendKey = true;
        return self();
    }

    /**
     * Do not send the user-defined key (default behavior).
     *
     * @return this builder for method chaining
     */
    public T withoutSendingKey() {
        this.sendKey = false;
        return self();
    }

    /**
     * Use zlib compression on command buffers sent to the server and responses received.
     * <p>
     * This will increase cpu and memory usage (for extra compressed buffers), but
     * decrease the size of data sent over the network.
     * Requires Enterprise Edition Server.
     * </p>
     *
     * @return this builder for method chaining
     */
    public T withCompression() {
        this.compress = true;
        return self();
    }

    /**
     * Do not use compression (default behavior).
     *
     * @return this builder for method chaining
     */
    public T withoutCompression() {
        this.compress = false;
        return self();
    }
}

