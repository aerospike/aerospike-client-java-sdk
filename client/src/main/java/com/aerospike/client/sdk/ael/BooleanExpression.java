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