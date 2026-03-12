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
package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents a Blob bin in an Aerospike record.
 */
public class BlobBin implements BlobExpression {
    private final String name;

    public BlobBin(String name) {
        this.name = name;
    }

    // Comparison operations (only equality for blobs)
    @Override
    public BooleanExpression eq(byte[] value) {
        return new Comparison<byte[]>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(byte[] value) {
        return new Comparison<byte[]>(this, ComparisonOp.NE, value);
    }

    // Comparison with other Blob expressions
    @Override
    public BooleanExpression eq(BlobExpression other) {
        return new Comparison<BlobExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(BlobExpression other) {
        return new Comparison<BlobExpression>(this, ComparisonOp.NE, other);
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
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.blobBin(\"" + name + "\")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.blobBin(name);
    }
}
