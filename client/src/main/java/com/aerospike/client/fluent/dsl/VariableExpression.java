package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a reference to a local variable.
 * This is used within expressions that have local variable scope.
 */
public class VariableExpression implements DslExpression {
    private final String variableName;

    public VariableExpression(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return variableName;
    }

    @Override
    public String toString() {
        return "var(" + variableName + ")";
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.var(\"" + variableName + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.var(variableName);
    }
}