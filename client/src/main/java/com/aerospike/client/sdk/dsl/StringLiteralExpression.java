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

import com.aerospike.client.sdk.exp.Exp;

/**
 * Represents a literal String value expression.
 */
public class StringLiteralExpression implements StringExpression {
    private final String value;

    public StringLiteralExpression(String value) {
        this.value = value;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(String value) {
        return new Comparison<String>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(String value) {
        return new Comparison<String>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(String value) {
        return new Comparison<String>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(String value) {
        return new Comparison<String>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(String value) {
        return new Comparison<String>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(String value) {
        return new Comparison<String>(this, ComparisonOp.LTE, value);
    }

    // Comparison with other String expressions
    @Override
    public BooleanExpression eq(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.NE, other);
    }

    @Override
    public BooleanExpression gt(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.GT, other);
    }

    @Override
    public BooleanExpression lt(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.LT, other);
    }

    @Override
    public BooleanExpression gte(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.GTE, other);
    }

    @Override
    public BooleanExpression lte(StringExpression other) {
        return new Comparison<StringExpression>(this, ComparisonOp.LTE, other);
    }

    // Logical operations
    @Override
    public BooleanExpression and(BooleanExpression other) {
        return new LogicalExpression(LogicalOp.AND, this, other);
    }

    @Override
    public BooleanExpression or(BooleanExpression other) {
        return new LogicalExpression(LogicalOp.OR, this, other);
    }

    @Override
    public BooleanExpression not() {
        return new LogicalExpression(LogicalOp.NOT, this, null);
    }

    // Boolean operations (required by BooleanExpression)
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<String>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<String>(this, ComparisonOp.NE, value);
    }

    @Override
    public String toString() {
        return "'" + value + "'";
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.val(\"" + value + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.val(value);
    }
}