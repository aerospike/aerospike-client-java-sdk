package com.aerospike.client.fluent.dsl;

/**
 * Represents a boolean expression that can be used in logical operations.
 * This is the base interface for all expressions that can be combined with AND, OR, NOT.
 */
public interface BooleanExpression extends DslExpression {
    BooleanExpression and(BooleanExpression other);
    BooleanExpression or(BooleanExpression other);
    BooleanExpression not();
    
    // Comparison operations for boolean values
    BooleanExpression eq(Boolean value);
    BooleanExpression ne(Boolean value);
} 