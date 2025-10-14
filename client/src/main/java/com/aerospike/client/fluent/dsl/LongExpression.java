package com.aerospike.client.fluent.dsl;

/**
 * Represents a Long expression that can be used in arithmetic and comparison operations.
 */
public interface LongExpression extends NumericExpression, BooleanExpression {
    // Comparison operations
    BooleanExpression eq(Long value);
    BooleanExpression ne(Long value);
    BooleanExpression gt(Long value);
    BooleanExpression lt(Long value);
    BooleanExpression gte(Long value);
    BooleanExpression lte(Long value);
    
    // Comparison with Integer values
    BooleanExpression eq(Integer value);
    BooleanExpression ne(Integer value);
    BooleanExpression gt(Integer value);
    BooleanExpression lt(Integer value);
    BooleanExpression gte(Integer value);
    BooleanExpression lte(Integer value);
    
    // Comparison with other Long expressions
    BooleanExpression eq(LongExpression other);
    BooleanExpression ne(LongExpression other);
    BooleanExpression gt(LongExpression other);
    BooleanExpression lt(LongExpression other);
    BooleanExpression gte(LongExpression other);
    BooleanExpression lte(LongExpression other);
    
    // Arithmetic with literals
    LongExpression add(Long value);
    LongExpression sub(Long value);
    LongExpression mul(Long value);
    LongExpression div(Long value);
    
    // Arithmetic with Integer values
    LongExpression add(Integer value);
    LongExpression sub(Integer value);
    LongExpression mul(Integer value);
    LongExpression div(Integer value);
    
    // Conversion functions
    DoubleExpression toFloat();
} 