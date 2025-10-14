package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a Long bin in an Aerospike record.
 */
public class LongBin implements LongExpression {
    private final String name;

    public LongBin(String name) {
        this.name = name;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(Long value) {
        return new Comparison<Long>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Long value) {
        return new Comparison<Long>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Long value) {
        return new Comparison<Long>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Long value) {
        return new Comparison<Long>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Long value) {
        return new Comparison<Long>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Long value) {
        return new Comparison<Long>(this, ComparisonOp.LTE, value);
    }

    // Comparison with Integer values
    @Override
    public BooleanExpression eq(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.LTE, value);
    }

    // Comparison with other Long expressions
    @Override
    public BooleanExpression eq(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.NE, other);
    }

    @Override
    public BooleanExpression gt(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.GT, other);
    }

    @Override
    public BooleanExpression lt(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.LT, other);
    }

    @Override
    public BooleanExpression gte(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.GTE, other);
    }

    @Override
    public BooleanExpression lte(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.LTE, other);
    }

    // Arithmetic with literals
    @Override
    public LongExpression add(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.PLUS, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression sub(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.MINUS, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression mul(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.TIMES, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression div(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.DIV, new LongLiteralExpression(value));
    }

    // Arithmetic with Integer values
    @Override
    public LongExpression add(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.PLUS, new LongLiteralExpression(value.longValue()));
    }

    @Override
    public LongExpression sub(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.MINUS, new LongLiteralExpression(value.longValue()));
    }

    @Override
    public LongExpression mul(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.TIMES, new LongLiteralExpression(value.longValue()));
    }

    @Override
    public LongExpression div(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.DIV, new LongLiteralExpression(value.longValue()));
    }

    // Support for IF expressions in arithmetic
    public LongExpression add(IfExpression ifExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.PLUS, new IfLongExpression(ifExpr));
    }

    public LongExpression sub(IfExpression ifExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.MINUS, new IfLongExpression(ifExpr));
    }

    public LongExpression mul(IfExpression ifExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.TIMES, new IfLongExpression(ifExpr));
    }

    public LongExpression div(IfExpression ifExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.DIV, new IfLongExpression(ifExpr));
    }

    // Support for LocalVariableExpression in arithmetic
    public LongExpression add(LocalVariableExpression localVarExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.PLUS, new LocalVariableLongExpression(localVarExpr));
    }

    public LongExpression sub(LocalVariableExpression localVarExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.MINUS, new LocalVariableLongExpression(localVarExpr));
    }

    public LongExpression mul(LocalVariableExpression localVarExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.TIMES, new LocalVariableLongExpression(localVarExpr));
    }

    public LongExpression div(LocalVariableExpression localVarExpr) {
        return new LongArithmeticExpression(this, ArithmeticOp.DIV, new LocalVariableLongExpression(localVarExpr));
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

    // Boolean operations (required by BooleanExpression)
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
    }

    // Conversion functions
    @Override
    public DoubleExpression toFloat() {
        return new LongToDoubleExpression(this);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.intBin(\"" + name + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.intBin(name);
    }
}