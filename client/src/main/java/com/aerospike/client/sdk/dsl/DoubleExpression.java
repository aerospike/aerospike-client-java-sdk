/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk.dsl;

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