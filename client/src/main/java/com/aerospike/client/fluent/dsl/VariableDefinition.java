package com.aerospike.client.fluent.dsl;

/**
 * Represents a local variable definition with a name and value.
 */
public class VariableDefinition {
    private final String name;
    private final DslExpression value;

    public VariableDefinition(String name, DslExpression value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public DslExpression getValue() {
        return value;
    }

    @Override
    public String toString() {
        return name + " = " + value;
    }

    public String toAerospikeExpr() {
        return "Exp.def(\"" + name + "\", " + value.toAerospikeExpr() + ")";
    }
} 