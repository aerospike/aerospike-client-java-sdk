package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a function call expression.
 */
public class FunctionCallExpression implements DslExpression {
    private final String function;
    private final DslExpression argument;

    public FunctionCallExpression(String function, DslExpression argument) {
        this.function = function;
        this.argument = argument;
    }

    @Override
    public String toString() {
        return function + "(" + argument + ")";
    }

    @Override
    public String toAerospikeExpr() {
        return function + "(" + argument.toAerospikeExpr() + ")";
    }

    // TODO: Is this class neededd?
    @Override
    public Exp toAerospikeExp() {
        // TODO Auto-generated method stub
        return null;
    }
}
