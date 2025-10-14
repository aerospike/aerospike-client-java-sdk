package com.aerospike.client.fluent.dsl;

/**
 * Represents a numeric expression that can be used in arithmetic operations.
 * This interface is implemented by both Long and Double expressions.
 */
public interface NumericExpression extends DslExpression {
    NumericExpression add(NumericExpression other);
    NumericExpression sub(NumericExpression other);
    NumericExpression mul(NumericExpression other);
    NumericExpression div(NumericExpression other);
} 