package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a String bin in an Aerospike record.
 */
public class StringBin implements StringExpression {
    private final String name;

    public StringBin(String name) {
        this.name = name;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(String value) {
        return new Comparison<String>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(String value) {
        return new Comparison<String>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(String value) {
        return new Comparison<String>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(String value) {
        return new Comparison<String>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(String value) {
        return new Comparison<String>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(String value) {
        return new Comparison<String>(this, ComparisonOp.LTE, value);
    }

    // Comparison with other String expressions
    @Override
    public BooleanExpression eq(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.NE, other);
    }

    @Override
    public BooleanExpression gt(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.GT, other);
    }

    @Override
    public BooleanExpression lt(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.LT, other);
    }

    @Override
    public BooleanExpression gte(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.GTE, other);
    }

    @Override
    public BooleanExpression lte(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.LTE, other);
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
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.stringBin(\"" + name + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.stringBin(name);
    }
}
