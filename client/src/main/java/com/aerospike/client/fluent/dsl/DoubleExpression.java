package com.aerospike.client.fluent.dsl;

/**
 * Represents a Double expression that can be used in arithmetic and comparison operations.
 */
public interface DoubleExpression extends NumericExpression, BooleanExpression {
    // Comparison operations
    BooleanExpression eq(Double value);
    BooleanExpression ne(Double value);
    BooleanExpression gt(Double value);
    BooleanExpression lt(Double value);
    BooleanExpression gte(Double value);
    BooleanExpression lte(Double value);
    
    // Comparison with Float values
    BooleanExpression eq(Float value);
    BooleanExpression ne(Float value);
    BooleanExpression gt(Float value);
    BooleanExpression lt(Float value);
    BooleanExpression gte(Float value);
    BooleanExpression lte(Float value);
    
    // Comparison with other Double expressions
    BooleanExpression eq(DoubleExpression other);
    BooleanExpression ne(DoubleExpression other);
    BooleanExpression gt(DoubleExpression other);
    BooleanExpression lt(DoubleExpression other);
    BooleanExpression gte(DoubleExpression other);
    BooleanExpression lte(DoubleExpression other);
    
    // Arithmetic with literals
    DoubleExpression add(Double value);
    DoubleExpression sub(Double value);
    DoubleExpression mul(Double value);
    DoubleExpression div(Double value);
    
    // Arithmetic with Float values
    DoubleExpression add(Float value);
    DoubleExpression sub(Float value);
    DoubleExpression mul(Float value);
    DoubleExpression div(Float value);
    
    // Conversion functions
    LongExpression toInt();
} 