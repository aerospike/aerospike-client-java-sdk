package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a comparison operation between two values or expressions.
 */
public class Comparison<T> implements BooleanExpression {
    private final DslExpression left;
    private final ComparisonOp operator;
    private final Object right;

    public Comparison(DslExpression left, ComparisonOp operator, Object right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

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
    public BooleanExpression eq(Boolean value) {
        return new Comparison<>(left, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<>(left, ComparisonOp.NE, value);
    }

    public static String val(Object o) {
        if (o instanceof String) {
            return "Exp.val(\"" + o + "\")";
        }
        return "Exp.val(" + o.toString() + ")";
    }

    @Override
    public String toString() {
        return left.toString() + " " + operator.getValue() + " " + (right instanceof DslExpression ? right.toString() : right);
    }

    @Override
    public String toAerospikeExpr() {
        switch (operator) {
            case EQ:
                return "Exp.eq(" + left.toAerospikeExpr() + ", " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
            case NE:
                return "Exp.ne(" + left.toAerospikeExpr() + ", " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
            case GT:
                return "Exp.gt(" + left.toAerospikeExpr() + ", " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
            case LT:
                return "Exp.lt(" + left.toAerospikeExpr() + ", " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
            case GTE:
                return "Exp.ge(" + left.toAerospikeExpr() + ", " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
            case LTE:
                return "Exp.le(" + left.toAerospikeExpr() + ", " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
            default:
                return "(" + left.toAerospikeExpr() + " " + operator + " " + (right instanceof DslExpression ? ((DslExpression) right).toAerospikeExpr() : val(right)) + ")";
        }
    }

    private Exp rightAsExp() {
        if (right instanceof DslExpression) {
            return ((DslExpression) right).toAerospikeExp();
        }
        else {
            if (right instanceof String) {
				return Exp.val((String)right);
			}
            if (right instanceof Double) {
				return Exp.val((Double)right);
			}
            if (right instanceof Float) {
				return Exp.val((Float)right);
			}
            if (right instanceof Integer) {
				return Exp.val((Integer)right);
			}
            if (right instanceof Long) {
				return Exp.val((Long)right);
			}
            if (right instanceof byte[]) {
				return Exp.val((byte [])right);
			}
            if (right instanceof Boolean) {
				return Exp.val((Boolean)right);
			}
        }
        throw new IllegalStateException("Unexpected right hand side type in Comparison: " + right.getClass());
    }

    @Override
    public Exp toAerospikeExp() {
        switch (operator) {
        case EQ:  return Exp.eq(left.toAerospikeExp(), rightAsExp());
        case NE:  return Exp.ne(left.toAerospikeExp(), rightAsExp());
        case GT:  return Exp.gt(left.toAerospikeExp(), rightAsExp());
        case GTE: return Exp.ge(left.toAerospikeExp(), rightAsExp());
        case LT:  return Exp.lt(left.toAerospikeExp(), rightAsExp());
        case LTE: return Exp.le(left.toAerospikeExp(), rightAsExp());
        }
        throw new IllegalStateException("Unknown operator in Comparison: " + operator);
    }
}
