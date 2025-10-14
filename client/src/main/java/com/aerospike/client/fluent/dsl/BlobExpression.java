package com.aerospike.client.fluent.dsl;

/**
 * Represents a Blob expression that can be used in equality comparison operations.
 */
public interface BlobExpression extends BooleanExpression {
    // Only equality operations are supported for blobs
    BooleanExpression eq(byte[] value);
    BooleanExpression ne(byte[] value);
    
    // Comparison with other Blob expressions
    BooleanExpression eq(BlobExpression other);
    BooleanExpression ne(BlobExpression other);
} 