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

import com.aerospike.client.sdk.command.AbortStatus;
import com.aerospike.client.sdk.command.CommitStatus;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.command.TxnRoll;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;

/**
 * A session that supports transactional operations with automatic retry logic.
 *
 * <p>This class extends the base Session class to provide transactional capabilities.
 * It manages Aerospike transactions with built-in retry logic for transient failures
 * and proper cleanup of transaction resources.</p>
 *
 * <p>The transaction system supports:</p>
 * <ul>
 *   <li><strong>Automatic retry:</strong> Retries on transient failures like MRT_BLOCKED, MRT_VERSION_MISMATCH, and TXN_FAILED</li>
 *   <li><strong>Nested transactions:</strong> Supports nested transaction calls without creating multiple transaction contexts</li>
 *   <li><strong>Resource cleanup:</strong> Automatically aborts transactions on exceptions</li>
 *   <li><strong>Return values:</strong> Supports both void and value-returning operations</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * TransactionalSession session = new TransactionalSession(cluster, behavior);
 *
 * // Void transaction
 * session.doInTransaction(txSession -> {
 *     // Perform operations within transaction
 *     txSession.put(key, bin);
 * });
 *
 * // Value-returning transaction
 * String result = session.doInTransaction(txSession -> {
 *     Record record = txSession.get(key);
 *     return record.getString("value");
 * });
 * }</pre>
 *
 * @author Aerospike
 * @since 1.0
 */
public class TransactionalSession extends Session{
    private final Txn txn;
    private int count = 0;

    /**
     * Creates a new TransactionalSession with the specified cluster and behavior.
     *
     * @param cluster the Aerospike cluster
     * @param behavior the behavior configuration for this session
     */
    protected TransactionalSession(Cluster cluster, Behavior behavior) {
        super(cluster, behavior);
        this.txn = new Txn();
    }

    /**
     * Gets the current transaction object.
     *
     * @return the current Txn instance
     */
    @Override
    public Txn getCurrentTransaction() {
        return this.txn;
    }

    /**
     * Executes a transactional operation that returns a value.
     *
     * <p>Use this method when your transaction needs to return a result, such as
     * reading data or computing a value based on transactional operations.</p>
     *
     * <p><b>Why the different name?</b> This method is named differently from
     * {@link #doInTransaction(Session.TransactionalVoid)} to avoid Java type inference
     * ambiguity with complex lambda bodies. Without distinct names, the compiler cannot
     * determine which overload to use when the lambda contains control flow statements
     * like {@code while} loops, forcing users to add explicit {@code return null;} statements.</p>
     *
     * <p>This method provides automatic retry logic for transient failures and ensures
     * proper transaction cleanup. The operation will be retried automatically for
     * the following result codes:</p>
     * <ul>
     *   <li>MRT_BLOCKED</li>
     *   <li>MRT_VERSION_MISMATCH</li>
     *   <li>TXN_FAILED</li>
     * </ul>
     *
     * <p>For other failures, the exception will be thrown immediately without retry.</p>
     *
     * <p><b>Example usage:</b>
     * <pre>{@code
     * String userName = txSession.doInTransactionReturning(tx -> {
     *     RecordStream results = tx.query(users.id(userId)).execute();
     *     Record record = results.getFirst().get().recordOrThrow();
     *     return record.getString("name");
     * });
     * }</pre>
     *
     * @param <T> the type of value returned by the operation
     * @param operation the transactional operation to execute
     * @return the result of the operation
     * @throws AerospikeException if the operation fails with a non-retryable error
     * @throws RuntimeException if any other exception occurs during execution
     * @see #doInTransaction(Session.TransactionalVoid)
     */
    public <T> T doInTransactionReturning(Transactional<T> operation) {
        try {
            if (++count > 1) {
                // Nested transaction, do not enforce transaction semantics
                return operation.execute(this);
            }
            else {
                // Outermost transaction, commit when complete.
                SystemSettings settings = getCluster().getSystemSettings();
                int maxAttempts = settings.getNumberOfAttempts();
                Duration sleepBetweenAttempts = settings.getSleepBetweenAttempts();

                for (int attempt = 1; ; attempt++) {
                    T result;

                    try {
                        result = operation.execute(this);
                    }
                    catch (AbortException abortex) {
                        abortTxn();
                        return null;
                    }
                    catch (AerospikeException ae) {
                        abortTxn();

                        if (retryCommit(ae) && attempt < maxAttempts) {
                            sleepBetweenRetries(sleepBetweenAttempts);
                            continue;
                        }
                        throw ae;
                    }
                    catch (Exception e) {
                        abortTxn();
                        throw e;
                    }

                    commitTxn();
                    return result;
                }
            }
        }
        finally {
            count--;
        }
    }

    /**
     * Executes a transactional operation that does not return a value.
     *
     * <p>Use this method when your transaction only needs to perform operations
     * without returning a result to the caller. This is the most common case for
     * transactional writes and updates.</p>
     *
     * <p>This method provides automatic retry logic for transient failures and ensures
     * proper transaction cleanup. The operation will be retried automatically for
     * the following result codes:</p>
     * <ul>
     *   <li>MRT_BLOCKED</li>
     *   <li>MRT_VERSION_MISMATCH</li>
     *   <li>TXN_FAILED</li>
     * </ul>
     *
     * <p>For other failures, the exception will be thrown immediately without retry.</p>
     *
     * <p><b>Example usage:</b>
     * <pre>{@code
     * txSession.doInTransaction(tx -> {
     *     tx.upsert(accounts.id("acc1"))
     *         .bin("balance").add(-100)
     *         .execute();
     *     tx.upsert(accounts.id("acc2"))
     *         .bin("balance").add(100)
     *         .execute();
     * });
     * }</pre>
     *
     * @param operation the transactional operation to execute
     * @throws AerospikeException if the operation fails with a non-retryable error
     * @throws RuntimeException if any other exception occurs during execution
     * @see #doInTransactionReturning(Transactional)
     */
    public void doInTransaction(TransactionalVoid operation) {
        try {
            if (++count > 1) {
                // Nested transaction, do not enforce transaction semantics
                operation.execute(this);
            }
            else {
                // Outermost transaction, commit when complete.
                SystemSettings settings = getCluster().getSystemSettings();
                int maxAttempts = settings.getNumberOfAttempts();
                Duration sleepBetweenAttempts = settings.getSleepBetweenAttempts();

                for (int attempt = 1; ; attempt++) {
                    try {
                        operation.execute(this);
                    }
                    catch (AbortException abortex) {
                        abortTxn();
                        return;
                    }
                    catch (AerospikeException ae) {
                        abortTxn();

                        if (retryCommit(ae) && attempt < maxAttempts) {
                            sleepBetweenRetries(sleepBetweenAttempts);
                            continue;
                        }
                        throw ae;
                    }
                    catch (Exception e) {
                        abortTxn();
                        throw e;
                    }

                    commitTxn();
                    return;
                }
            }
        }
        finally {
            count--;
        }
    }

    /**
     * Aborts the current transaction.
     *
     * <p>This method can be called from within a transaction lambda to programmatically
     * abort the transaction. When called, it throws an internal exception that is caught
     * and handled by the transaction framework, which then aborts the transaction and
     * returns control to the caller.</p>
     *
     * <p><b>Example usage:</b></p>
     * <pre>{@code
     * txSession.doInTransaction(tx -> {
     *     Record record = tx.get(key);
     *     if (record == null || record.getInt("status") == INACTIVE) {
     *         tx.abort(); // Abort if precondition not met
     *         return;
     *     }
     *     // Continue with transaction operations
     *     tx.upsert(key).bin("value").set(newValue).execute();
     * });
     * }</pre>
     *
     * <p>Or with a value-returning transaction:</p>
     * <pre>{@code
     * String result = txSession.doInTransactionReturning(tx -> {
     *     Record record = tx.get(key);
     *     if (record == null) {
     *         tx.abort(); // Returns null to caller
     *     }
     *     return record.getString("value");
     * });
     * }</pre>
     *
     * @throws RuntimeException always throws an internal AbortException that is caught
     *                          and handled by the transaction framework
     */
    public void abort() {
        throw new AbortException();
    }

    private boolean retryCommit(AerospikeException ae) {
        switch (ae.getResultCode()) {
            case ResultCode.MRT_BLOCKED:
            case ResultCode.MRT_VERSION_MISMATCH:
            case ResultCode.TXN_FAILED:
                // These can be retried from the beginning
                return true;

            default:
                // These cannot be retried
                return false;
        }
    }

    private static void sleepBetweenRetries(Duration sleepBetweenAttempts) {
        if (sleepBetweenAttempts != null && sleepBetweenAttempts.isPositive()) {
            try {
                Thread.sleep(sleepBetweenAttempts.toMillis());
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private CommitStatus commitTxn() {
        TxnRoll tr = new TxnRoll(getCluster(), txn);
        Settings verifyPolicy = getBehavior().getSettings(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY);
        Settings rollPolicy = getBehavior().getSettings(OpKind.SYSTEM_TXN_ROLL, OpShape.SYSTEM, Mode.ANY);

        switch (txn.getState()) {
            default:
            case OPEN:
                tr.verify(verifyPolicy, rollPolicy);
                return tr.commit(rollPolicy);

            case VERIFIED:
                return tr.commit(rollPolicy);

            case COMMITTED:
                return CommitStatus.ALREADY_COMMITTED;

            case ABORTED:
                throw AerospikeException.resultCodeToException(ResultCode.TXN_ALREADY_ABORTED, "Transaction already aborted");
        }
    }

    private AbortStatus abortTxn() {
        TxnRoll tr = new TxnRoll(getCluster(), txn);
        Settings rollPolicy = getBehavior().getSettings(OpKind.SYSTEM_TXN_ROLL, OpShape.SYSTEM, Mode.ANY);

        switch (txn.getState()) {
            default:
            case OPEN:
            case VERIFIED:
                return tr.abort(rollPolicy);

            case COMMITTED:
                throw AerospikeException.resultCodeToException(ResultCode.TXN_ALREADY_COMMITTED, "Transaction already committed");

            case ABORTED:
                return AbortStatus.ALREADY_ABORTED;
        }
    }

    private static final class AbortException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
