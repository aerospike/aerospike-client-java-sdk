package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a literal value expression.
 */
public class LiteralExpression implements DslExpression {
    private final Object value;

    public LiteralExpression(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public String toAerospikeExpr() {
        return value.toString();
    }

    @Override
    public Exp toAerospikeExp() {
        if (value instanceof DslExpression) {
            return ((DslExpression) value).toAerospikeExp();
        }
        else {
            if (value instanceof String) {
				return Exp.val((String)value);
			}
            if (value instanceof Double) {
				return Exp.val((Double)value);
			}
            if (value instanceof Float) {
				return Exp.val((Float)value);
			}
            if (value instanceof Integer) {
				return Exp.val((Integer)value);
			}
            if (value instanceof Long) {
				return Exp.val((Long)value);
			}
            if (value instanceof byte[]) {
				return Exp.val((byte [])value);
			}
            if (value instanceof Boolean) {
				return Exp.val((Boolean)value);
			}
        }
        throw new IllegalStateException("Unexpected value hand side type in LiteralExpression: " + value.getClass());

    }
}
