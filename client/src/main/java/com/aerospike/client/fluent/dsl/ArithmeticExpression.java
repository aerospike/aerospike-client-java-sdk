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
 * Represents an arithmetic operation between numeric expressions.
 */
public class ArithmeticExpression implements NumericExpression {
    private final NumericExpression left;
    private final ArithmeticOp operator;
    private final NumericExpression right;

    public ArithmeticExpression(NumericExpression left, ArithmeticOp operator, NumericExpression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
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
    public String toString() {
        return "(" + left + " " + operator + " " + right + ")";
    }

    @Override
    public String toAerospikeExpr() {
        switch (operator) {
        case PLUS:  return "Exp.add(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        case MINUS:  return "Exp.sub(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        case TIMES:  return "Exp.mul(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        case DIV:
        default: return "Exp.div(" + left.toAerospikeExpr() + ", " + right.toAerospikeExpr() + ")";
        }
    }

    @Override
    public Exp toAerospikeExp() {
        switch (operator) {
        case PLUS:  return Exp.add(left.toAerospikeExp(), right.toAerospikeExp());
        case MINUS:  return Exp.sub(left.toAerospikeExp(), right.toAerospikeExp());
        case TIMES:  return Exp.mul(left.toAerospikeExp(), right.toAerospikeExp());
        case DIV: return Exp.div(left.toAerospikeExp(), right.toAerospikeExp());
        }

        throw new IllegalStateException("Unknown operator in ArithmeticException: " + operator);
    }
}
