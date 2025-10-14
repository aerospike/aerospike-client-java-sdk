package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a logical operation (AND, OR, NOT) between boolean expressions.
 */
public class LogicalExpression implements BooleanExpression {
    private final LogicalOp op;
    private final BooleanExpression left;
    private final BooleanExpression right;

    public LogicalExpression(LogicalOp op, BooleanExpression left, BooleanExpression right) {
        this.op = op;
        this.left = left;
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
        return new Comparison<>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<>(this, ComparisonOp.NE, value);
    }

    @Override
    public String toString() {
        return right == null ? "NOT (" + left + ")" : "(" + left + " " + op + " " + right + ")";
    }

    @Override
    public String toAerospikeExpr() {
        switch (op) {
            case AND:
                return "Exp.and(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
            case OR:
                return "Exp.or(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
            case NOT:
                return "Exp.not(" + left.toAerospikeExpr() + ")";
            default:
                return right == null ? "NOT (" + left.toAerospikeExpr() + ")" : "(" + left.toAerospikeExpr() + " " + op + " " + right.toAerospikeExpr() + ")";
        }
    }

    @Override
    public Exp toAerospikeExp() {
        switch (op) {
        case AND: return Exp.and(left.toAerospikeExp(), right.toAerospikeExp());
        case OR: return Exp.or(left.toAerospikeExp(), right.toAerospikeExp());
        case NOT: return Exp.not(left.toAerospikeExp());
        }
        throw new IllegalStateException("Unknown operator in LogicalExpression: " + op);
    }
}
