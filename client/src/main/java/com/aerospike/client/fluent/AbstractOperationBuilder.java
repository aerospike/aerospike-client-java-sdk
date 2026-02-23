package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for operation builders that support bin-level operations.
 *
 * <p>This class extends {@link AbstractSessionOperationBuilder} and adds support for
 * bin-level operations via the {@code bin()} method and operations list.</p>
 *
 * <p>This class contains shared functionality between key-specific operations
 * and set-level background operations ({@link BackgroundOperationBuilder}).</p>
 *
 * <p>Subclasses must implement the {@code execute()} method with their specific
 * return type (e.g., RecordStream for key operations, ExecuteTask for background operations).</p>
 *
 * @param <T> the concrete builder type (for fluent method chaining)
 */
public abstract class AbstractOperationBuilder<T extends AbstractOperationBuilder<T>> extends AbstractSessionOperationBuilder<T> {

    // ========================================
    // TTL Constants
    // ========================================

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

    /**
     * Sentinel value indicating that expiration has not been explicitly set.
     * Used to distinguish between "not set" and valid TTL values (including 0 for server default).
     */
    public static final long NOT_EXPLICITLY_SET = Long.MIN_VALUE;

    // ========================================
    // Batch Operation Threshold
    // ========================================

    /**
     * The threshold for determining when to use batch operations vs individual operations.
     * Operations with item counts >= this threshold will use batch mode.
     * Operations with item counts < this threshold will use individual parallel execution.
     */
    public static final int BATCH_OPERATION_THRESHOLD = 10;

    /**
     * Returns the threshold for determining when to use batch operations vs individual operations.
     * Operations with item counts >= this threshold will use batch mode.
     * Operations with item counts < this threshold will use individual parallel execution.
     *
     * @return the batch operation threshold
     */
    public static int getBatchOperationThreshold() {
        return BATCH_OPERATION_THRESHOLD;
    }

    // ========================================
    // Operation Retryability
    // ========================================

    /**
     * Determines if a list of operations are safe to retry.
     * Non-idempotent operations (ADD, APPEND, PREPEND, and modify operations) are not retryable.
     *
     * @param ops the list of operations to check
     * @return true if all operations are retryable, false otherwise
     */
    public static boolean areOperationsRetryable(List<Operation> ops) {
        for (Operation op : ops) {
            switch (op.type) {
            case ADD:
            case APPEND:
            case PREPEND:
                // Definitely not retryable
                return false;

            case BIT_MODIFY:
            case CDT_MODIFY:
            case EXP_MODIFY:
            case HLL_MODIFY:
            case MAP_MODIFY:
                // These are questionable. For example, MAP_MODIFY could be CLEAR (retryable) or DECREMENT (non-retryable)
                // For now return as not retryable, but will need further information in API v2
                return false;

            case BIT_READ:
            case CDT_READ:
            case DELETE:
            case EXP_READ:
            case HLL_READ:
            case MAP_READ:
            case READ:
            case READ_HEADER:
            case TOUCH:
            case WRITE:
                // definitely retryable
            }
        }
        return true;
    }

    // ========================================
    // Instance Fields and Constructor
    // ========================================

    protected final List<Operation> ops = new ArrayList<>();

    protected AbstractOperationBuilder(Session session, OpType opType) {
        super(session, opType);
    }

    /**
     * Returns a bin builder for operating on a specific bin.
     *
     * @param binName the name of the bin
     * @return BinBuilder for constructing bin operations
     */
    public BinBuilder<T> bin(String binName) {
        return new BinBuilder<>(self(), binName);
    }

    /**
     * Set a bin value using a Bin object.
     * Protected method used by BinBuilder.
     */
    protected T setTo(Bin bin) {
        this.ops.add(Operation.put(bin));
        return self();
    }

    /**
     * Read a bin value.
     * Protected method used by BinBuilder.
     */
    protected T get(String binName) {
        this.ops.add(Operation.get(binName));
        return self();
    }

    /**
     * Append to a bin value.
     * Protected method used by BinBuilder.
     */
    protected T append(Bin bin) {
        this.ops.add(Operation.append(bin));
        return self();
    }

    /**
     * Prepend to a bin value.
     * Protected method used by BinBuilder.
     */
    protected T prepend(Bin bin) {
        this.ops.add(Operation.prepend(bin));
        return self();
    }

    /**
     * Add to a bin value (numeric addition).
     * Protected method used by BinBuilder.
     */
    protected T add(Bin bin) {
        this.ops.add(Operation.add(bin));
        return self();
    }

    /**
     * Add a custom operation.
     * Protected method used by BinBuilder.
     */
    protected T addOp(Operation op) {
        this.ops.add(op);
        return self();
    }
}

