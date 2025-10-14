package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents any DSL expression. This is the base interface for all expressions.
 */
public interface DslExpression {
    String toAerospikeExpr();
    Exp toAerospikeExp();
}
