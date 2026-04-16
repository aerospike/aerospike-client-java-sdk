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

import com.aerospike.client.sdk.exp.Exp;

/**
 * Represents a Boolean bin in an Aerospike record.
 */
public class BooleanBin implements BooleanExpression {
    private final String name;

    public BooleanBin(String name) {
        this.name = name;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
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

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.boolBin(\"" + name + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.boolBin(name);
    }
}
