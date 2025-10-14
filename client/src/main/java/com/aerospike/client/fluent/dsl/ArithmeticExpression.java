package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents an arithmetic operation between numeric expressions.
 */
public class ArithmeticExpression implements NumericExpression {
    private final NumericExpression left;
    private final ArithmeticOp operator;
    private final NumericExpression right;

    public ArithmeticExpression(NumericExpression left, ArithmeticOp operator, NumericExpression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

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

    @Override
    public String toString() {
        return "(" + left + " " + operator + " " + right + ")";
    }

    @Override
    public String toAerospikeExpr() {
        switch (operator) {
        case PLUS:  return "Exp.add(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        case MINUS:  return "Exp.sub(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        case TIMES:  return "Exp.mul(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        case DIV:
        default: return "Exp.div(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        }
    }

    @Override
    public Exp toAerospikeExp() {
        switch (operator) {
        case PLUS:  return Exp.add(left.toAerospikeExp(), right.toAerospikeExp());
        case MINUS:  return Exp.sub(left.toAerospikeExp(), right.toAerospikeExp());
        case TIMES:  return Exp.mul(left.toAerospikeExp(), right.toAerospikeExp());
        case DIV: return Exp.div(left.toAerospikeExp(), right.toAerospikeExp());
        }

        throw new IllegalStateException("Unknown operator in ArithmeticException: " + operator);
    }
}
