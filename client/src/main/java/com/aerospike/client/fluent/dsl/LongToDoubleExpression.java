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
 * Represents the conversion from Long to Double expression.
 */
public class LongToDoubleExpression implements DoubleExpression {
    private final LongExpression longExpr;

    public LongToDoubleExpression(LongExpression longExpr) {
        this.longExpr = longExpr;
    }

    // Comparison operations
    @Override
    public BooleanExpression eq(Double value) {
        return new Comparison<Double>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Double value) {
        return new Comparison<Double>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Double value) {
        return new Comparison<Double>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Double value) {
        return new Comparison<Double>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Double value) {
        return new Comparison<Double>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Double value) {
        return new Comparison<Double>(this, ComparisonOp.LTE, value);
    }

    // Comparison with Float values
    @Override
    public BooleanExpression eq(Float value) {
        return new Comparison<Float>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Float value) {
        return new Comparison<Float>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Float value) {
        return new Comparison<Float>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Float value) {
        return new Comparison<Float>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Float value) {
        return new Comparison<Float>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Float value) {
        return new Comparison<Float>(this, ComparisonOp.LTE, value);
    }

    // Comparison with other Double expressions
    @Override
    public BooleanExpression eq(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.NE, other);
    }

    @Override
    public BooleanExpression gt(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.GT, other);
    }

    @Override
    public BooleanExpression lt(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.LT, other);
    }

    @Override
    public BooleanExpression gte(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.GTE, other);
    }

    @Override
    public BooleanExpression lte(DoubleExpression other) {
        return new Comparison<DoubleExpression>(this, ComparisonOp.LTE, other);
    }

    // Arithmetic with literals
    @Override
    public DoubleExpression add(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.PLUS, new DoubleLiteralExpression(value));
    }

    @Override
    public DoubleExpression sub(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.MINUS, new DoubleLiteralExpression(value));
    }

    @Override
    public DoubleExpression mul(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.TIMES, new DoubleLiteralExpression(value));
    }

    @Override
    public DoubleExpression div(Double value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.DIV, new DoubleLiteralExpression(value));
    }

    // Arithmetic with Float values
    @Override
    public DoubleExpression add(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.PLUS, new DoubleLiteralExpression(value.doubleValue()));
    }

    @Override
    public DoubleExpression sub(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.MINUS, new DoubleLiteralExpression(value.doubleValue()));
    }

    @Override
    public DoubleExpression mul(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.TIMES, new DoubleLiteralExpression(value.doubleValue()));
    }

    @Override
    public DoubleExpression div(Float value) {
        return new DoubleArithmeticExpression(this, ArithmeticOp.DIV, new DoubleLiteralExpression(value.doubleValue()));
    }

    // Arithmetic with other numeric expressions
    @Override
    public NumericExpression add(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.PLUS, other);
    }

    @Override
    public NumericExpression sub(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.MINUS, other);
    }

    @Override
    public NumericExpression mul(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.TIMES, other);
    }

    @Override
    public NumericExpression div(NumericExpression other) {
        return new ArithmeticExpression(this, ArithmeticOp.DIV, other);
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

    // Boolean operations
    @Override
    public BooleanExpression eq(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
    }

    // Conversion functions
    @Override
    public LongExpression toInt() {
        return new DoubleToLongExpression(this);
    }

    @Override
    public String toString() {
        return "toFloat(" + longExpr + ")";
    }

    @Override
    public String toAerospikeExpr() {
        return "Exp.toFloat(" + longExpr.toAerospikeExpr() + ")";
    }

    @Override
    public Exp toAerospikeExp() {
        return Exp.toFloat(longExpr.toAerospikeExp());
    }
}