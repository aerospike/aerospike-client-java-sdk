package com.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.policy.Behavior;

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
     * @param <T> the type of value returned by the operation
     * @param operation the transactional operation to execute
     * @return the result of the operation
     * @throws AerospikeException if the operation fails with a non-retryable error
     * @throws RuntimeException if any other exception occurs during execution
     */
    public <T> T doInTransaction(Transactional<T> operation) {
        try {
            if (++count > 1) {
                // Nested transaction, do not enforce transaction semantics
                return operation.execute(this);
            }
            else {
                // Outmost transaction, commit when complete.
                while (true) {
                    try {
                        T result = operation.execute(this);
                        this.getClient().commit(txn);
                        return result;
                    }
                    catch (AerospikeException ae) {
                        this.getClient().abort(txn);
                        switch (ae.getResultCode()) {
                        case ResultCode.MRT_BLOCKED:
                        case ResultCode.MRT_VERSION_MISMATCH:
                        case ResultCode.TXN_FAILED:
                            // These can be retried from the beginning
                            break;
                        default:
                            // These cannot be retried
                            throw ae;
                        }
                    }
                    catch (Exception e) {
                        this.getClient().abort(txn);
                        throw e;
                    }
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
     * @param operation the transactional operation to execute
     * @throws AerospikeException if the operation fails with a non-retryable error
     * @throws RuntimeException if any other exception occurs during execution
     */
    public void doInTransaction(TransactionalVoid operation) {
        try {
            if (++count > 1) {
                // Nested transaction, do not enforce transaction semantics
                operation.execute(this);
            }
            else {
                // Outmost transaction, commit when complete.
                while (true) {
                    try {
                        operation.execute(this);
                        this.getClient().commit(txn);
                    }
                    catch (AerospikeException ae) {
                        this.getClient().abort(txn);
                        switch (ae.getResultCode()) {
                        case ResultCode.MRT_BLOCKED:
                        case ResultCode.MRT_VERSION_MISMATCH:
                        case ResultCode.TXN_FAILED:
                            // These can be retried from the beginning
                            break;
                        default:
                            // These cannot be retried
                            throw ae;
                        }
                    }
                    catch (Exception e) {
                        this.getClient().abort(txn);
                        throw e;
                    }
                }
            }
        }
        finally {
            count--;
        }
    }

}
