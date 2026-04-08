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
 * Represents a literal value expression.
 */
public class LiteralExpression implements AelExpression {
    private final Object value;

    public LiteralExpression(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public String toAerospikeExpr() {
        return value.toString();
    }

    @Override
    public Exp toAerospikeExp() {
        if (value instanceof AelExpression) {
            return ((AelExpression) value).toAerospikeExp();
        }
        else {
            if (value instanceof String) {
				return Exp.val((String)value);
			}
            if (value instanceof Double) {
				return Exp.val((Double)value);
			}
            if (value instanceof Float) {
				return Exp.val((Float)value);
			}
            if (value instanceof Integer) {
				return Exp.val((Integer)value);
			}
            if (value instanceof Long) {
				return Exp.val((Long)value);
			}
            if (value instanceof byte[]) {
				return Exp.val((byte [])value);
			}
            if (value instanceof Boolean) {
				return Exp.val((Boolean)value);
			}
        }
        throw new IllegalStateException("Unexpected value hand side type in LiteralExpression: " + value.getClass());

    }
}
