package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a literal Boolean value expression.
 */
public class BooleanLiteralExpression implements BooleanExpression {
    private final Boolean value;

    public BooleanLiteralExpression(Boolean value) {
        this.value = value;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
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

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.val(" + value.toString() + ")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.val(value);
    }
}