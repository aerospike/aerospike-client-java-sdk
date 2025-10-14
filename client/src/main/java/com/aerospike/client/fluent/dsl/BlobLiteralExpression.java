package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a literal Blob value expression.
 */
public class BlobLiteralExpression implements BlobExpression {
    private final byte[] value;

    public BlobLiteralExpression(byte[] value) {
        this.value = value;
    }

    // Comparison operations (only equality for blobs)
    @Override
    public BooleanExpression eq(byte[] value) {
        return new Comparison<byte[]>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(byte[] value) {
        return new Comparison<byte[]>(this, ComparisonOp.NE, value);
    }

    // Comparison with other Blob expressions
    @Override
    public BooleanExpression eq(BlobExpression other) {
        return new Comparison<BlobExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(BlobExpression other) {
        return new Comparison<BlobExpression>(this, ComparisonOp.NE, other);
    }

    // Logical operations
    @Override
    public BooleanExpression and(BooleanExpression other) {
        return new LogicalExpression(LogicalOp.AND, this, other);
    }

    @Override
    public BooleanExpression or(BooleanExpression other) {
        return new LogicalExpression(LogicalOp.OR, this, other);
    }

    @Override
    public BooleanExpression not() {
        return new LogicalExpression(LogicalOp.NOT, this, null);
    }

    // Boolean operations (required by BooleanExpression)
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<byte[]>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<byte[]>(this, ComparisonOp.NE, value);
    }

    @Override
    public String toString() {
        return "blob[" + value.length + " bytes]";
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.val(blob[" + value.length + " bytes])";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.val(value);
    }
}