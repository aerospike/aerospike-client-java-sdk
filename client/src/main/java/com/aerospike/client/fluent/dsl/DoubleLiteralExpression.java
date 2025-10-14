package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a literal Double value expression.
 */
public class DoubleLiteralExpression implements DoubleExpression {
    private final Double value;

    public DoubleLiteralExpression(Double value) {
        this.value = value;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(Double value) {
        return new Comparison<Double>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Double value) {
        return new Comparison<Double>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Double value) {
        return new Comparison<Double>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Double value) {
        return new Comparison<Double>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Double value) {
        return new Comparison<Double>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Double value) {
        return new Comparison<Double>(this, ComparisonOp.LTE, value);
    }

    // Comparison with Float values
    @Override
    public BooleanExpression eq(Float value) {
        return new Comparison<Double>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Float value) {
        return new Comparison<Double>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Float value) {
        return new Comparison<Double>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Float value) {
        return new Comparison<Double>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Float value) {
        return new Comparison<Double>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Float value) {
        return new Comparison<Double>(this, ComparisonOp.LTE, value);
    }

    // Comparison with other Double expressions
    @Override
    public BooleanExpression eq(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.NE, other);
    }

    @Override
    public BooleanExpression gt(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.GT, other);
    }

    @Override
    public BooleanExpression lt(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.LT, other);
    }

    @Override
    public BooleanExpression gte(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.GTE, other);
    }

    @Override
    public BooleanExpression lte(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.LTE, other);
    }

    // Arithmetic with literals
    @Override
    public DoubleExpression add(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.PLUS, new DoubleLiteralExpression(value));
    }

    @Override
    public DoubleExpression sub(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.MINUS, new DoubleLiteralExpression(value));
    }

    @Override
    public DoubleExpression mul(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.TIMES, new DoubleLiteralExpression(value));
    }

    @Override
    public DoubleExpression div(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.DIV, new DoubleLiteralExpression(value));
    }

    // Arithmetic with Float values
    @Override
    public DoubleExpression add(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.PLUS, new DoubleLiteralExpression(value.doubleValue()));
    }

    @Override
    public DoubleExpression sub(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.MINUS, new DoubleLiteralExpression(value.doubleValue()));
    }

    @Override
    public DoubleExpression mul(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.TIMES, new DoubleLiteralExpression(value.doubleValue()));
    }

    @Override
    public DoubleExpression div(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.DIV, new DoubleLiteralExpression(value.doubleValue()));
    }

    // Arithmetic with other numeric expressions
    @Override
    public NumericExpression add(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.PLUS, other);
    }

    @Override
    public NumericExpression sub(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.MINUS, other);
    }

    @Override
    public NumericExpression mul(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.TIMES, other);
    }

    @Override
    public NumericExpression div(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.DIV, other);
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

    // Boolean operations
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<Double>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Double>(this, ComparisonOp.NE, value);
    }

    // Conversion functions
    @Override
    public LongExpression toInt() {
        return new DoubleToLongExpression(this);
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