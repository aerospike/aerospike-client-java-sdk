package com.aerospike.client.fluent;

import java.util.List;

/**
 * Specialized builder for DELETE operations that extends {@link OperationWithNoBinsBuilder}
 * to add delete-specific functionality like durable delete.
 *
 * <p>This class provides the same functionality as OperationWithNoBinsBuilder but adds
 * the {@link #durablyDelete(boolean)} method which is only applicable to delete operations.</p>
 */
public class DeleteBuilder extends OperationWithNoBinsBuilder {

    public DeleteBuilder(Session session, Key key) {
        super(session, key, OpType.DELETE);
    }

    public DeleteBuilder(Session session, List<Key> keys) {
        super(session, keys, OpType.DELETE);
    }

    /**
     * Specify whether this delete operation should be durable.
     * This overrides the durable delete setting in the behavior.
     *
     * @param durable true for durable delete, false for normal delete
     * @return this builder for method chaining
     */
    public DeleteBuilder durablyDelete(boolean durable) {
        this.durablyDelete = durable;
        return this;
    }
}

