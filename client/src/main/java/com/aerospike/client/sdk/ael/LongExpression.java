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
package com.aerospike.client.sdk.ael;

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