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