package com.aerospike.client.fluent.dsl;

/**
 * Represents a String expression that can be used in comparison operations.
 */
public interface StringExpression extends BooleanExpression {
    // Comparison operations
    BooleanExpression eq(String value);
    BooleanExpression ne(String value);
    BooleanExpression gt(String value);
    BooleanExpression lt(String value);
    BooleanExpression gte(String value);
    BooleanExpression lte(String value);
    
    // Comparison with other String expressions
    BooleanExpression eq(StringExpression other);
    BooleanExpression ne(StringExpression other);
    BooleanExpression gt(StringExpression other);
    BooleanExpression lt(StringExpression other);
    BooleanExpression gte(StringExpression other);
    BooleanExpression lte(StringExpression other);
} 