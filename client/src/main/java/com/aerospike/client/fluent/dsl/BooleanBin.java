package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a Boolean bin in an Aerospike record.
 */
public class BooleanBin implements BooleanExpression {
    private final String name;

    public BooleanBin(String name) {
        this.name = name;
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
        return name;
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.boolBin(\"" + name + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.boolBin(name);
    }
}
