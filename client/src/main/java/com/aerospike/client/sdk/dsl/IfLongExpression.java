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
 * Wrapper class that allows IfExpression to be used in Long arithmetic operations.
 * This enables expressions like: $.bina + if(binb > 10, 20, 30)
 */
public class IfLongExpression implements LongExpression {
    private final IfExpression ifExpr;

    public IfLongExpression(IfExpression ifExpr) {
        this.ifExpr = ifExpr;
    }

    @Override
    public BooleanExpression eq(Long value) {
        return new Comparison<Long>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Long value) {
        return new Comparison<Long>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Long value) {
        return new Comparison<Long>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Long value) {
        return new Comparison<Long>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Long value) {
        return new Comparison<Long>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Long value) {
        return new Comparison<Long>(this, ComparisonOp.LTE, value);
    }

    @Override
    public BooleanExpression eq(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.NE, value);
    }

    @Override
    public BooleanExpression gt(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.GT, value);
    }

    @Override
    public BooleanExpression lt(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.LT, value);
    }

    @Override
    public BooleanExpression gte(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.GTE, value);
    }

    @Override
    public BooleanExpression lte(Integer value) {
        return new Comparison<Integer>(this, ComparisonOp.LTE, value);
    }

    @Override
    public BooleanExpression eq(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.EQ, other);
    }

    @Override
    public BooleanExpression ne(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.NE, other);
    }

    @Override
    public BooleanExpression gt(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.GT, other);
    }

    @Override
    public BooleanExpression lt(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.LT, other);
    }

    @Override
    public BooleanExpression gte(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.GTE, other);
    }

    @Override
    public BooleanExpression lte(LongExpression other) {
        return new Comparison<LongExpression>(this, ComparisonOp.LTE, other);
    }

    @Override
    public LongExpression add(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.PLUS, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression sub(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.MINUS, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression mul(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.TIMES, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression div(Long value) {
        return new LongArithmeticExpression(this, ArithmeticOp.DIV, new LongLiteralExpression(value));
    }

    @Override
    public LongExpression add(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.PLUS, new LongLiteralExpression(value.longValue()));
    }

    @Override
    public LongExpression sub(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.MINUS, new LongLiteralExpression(value.longValue()));
    }

    @Override
    public LongExpression mul(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.TIMES, new LongLiteralExpression(value.longValue()));
    }

    @Override
    public LongExpression div(Integer value) {
        return new LongArithmeticExpression(this, ArithmeticOp.DIV, new LongLiteralExpression(value.longValue()));
    }

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
    public BooleanExpression eq(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.EQ, value);
    }

    @Override
    public BooleanExpression ne(Boolean value) {
        return new Comparison<Boolean>(this, ComparisonOp.NE, value);
    }

    @Override
    public DoubleExpression toFloat() {
        return new LongToDoubleExpression(this);
    }

    @Override
    public String toString() {
        return ifExpr.toString();
    }

    @Override
    public String toAerospikeExpr() {
        return ifExpr.toAerospikeExpr();
    }

    @Override
    public Exp toAerospikeExp() {
        return ifExpr.toAerospikeExp();
    }
}