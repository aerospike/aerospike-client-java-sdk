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
 * ({@link OperationBuilder}) and set-level background operations
 * ({@link BackgroundOperationBuilder}).</p>
 *
 * <p>Subclasses must implement the {@code execute()} method with their specific
 * return type (e.g., RecordStream for key operations, ExecuteTask for background operations).</p>
 *
 * @param <T> the concrete builder type (for fluent method chaining)
 */
public abstract class AbstractOperationBuilder<T extends AbstractOperationBuilder<T>> extends AbstractSessionOperationBuilder<T> {
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

