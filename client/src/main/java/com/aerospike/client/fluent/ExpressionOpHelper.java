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
package com.aerospike.client.fluent;

import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.ExpOperation;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.query.Filter;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.impl.DSLParserImpl;

/**
 * Helper class for creating expression operations from various DSL input types.
 * 
 * <p>Supports the following input types:</p>
 * <ul>
 *   <li>{@code String} - DSL string expression</li>
 *   <li>{@code BooleanExpression} - Programmatic boolean expression</li>
 *   <li>{@code PreparedDsl} - Prepared DSL statement with parameters</li>
 *   <li>{@code Exp} - Low-level expression builder</li>
 *   <li>{@code Expression} - Compiled expression</li>
 * </ul>
 */
public final class ExpressionOpHelper {

    private ExpressionOpHelper() {
        // Utility class
    }

    // ========================================
    // Read operations - from String DSL
    // ========================================

    public static Operation createReadOp(String binName, String dsl, int flags) {
        return ExpOperation.read(binName, parseStringDsl(dsl), flags);
    }

    public static Operation createReadOp(String binName, String dsl, Object[] params, int flags) {
        return ExpOperation.read(binName, parseStringDsl(dsl, params), flags);
    }

    // ========================================
    // Read operations - from other DSL types
    // ========================================

    public static Operation createReadOp(String binName, BooleanExpression dsl, int flags) {
        return ExpOperation.read(binName, fromBooleanExpression(dsl), flags);
    }

    public static Operation createReadOp(String binName, PreparedDsl dsl, Object[] params, int flags) {
        return ExpOperation.read(binName, fromPreparedDsl(dsl, params), flags);
    }

    public static Operation createReadOp(String binName, Exp exp, int flags) {
        return ExpOperation.read(binName, Exp.build(exp), flags);
    }

    public static Operation createReadOp(String binName, Expression exp, int flags) {
        return ExpOperation.read(binName, exp, flags);
    }

    // ========================================
    // Write operations - from String DSL
    // ========================================

    public static Operation createWriteOp(String binName, String dsl, int flags) {
        return ExpOperation.write(binName, parseStringDsl(dsl), flags);
    }

    public static Operation createWriteOp(String binName, String dsl, Object[] params, int flags) {
        return ExpOperation.write(binName, parseStringDsl(dsl, params), flags);
    }

    // ========================================
    // Write operations - from other DSL types
    // ========================================

    public static Operation createWriteOp(String binName, BooleanExpression dsl, int flags) {
        return ExpOperation.write(binName, fromBooleanExpression(dsl), flags);
    }

    public static Operation createWriteOp(String binName, PreparedDsl dsl, Object[] params, int flags) {
        return ExpOperation.write(binName, fromPreparedDsl(dsl, params), flags);
    }

    public static Operation createWriteOp(String binName, Exp exp, int flags) {
        return ExpOperation.write(binName, Exp.build(exp), flags);
    }

    public static Operation createWriteOp(String binName, Expression exp, int flags) {
        return ExpOperation.write(binName, exp, flags);
    }

    // ========================================
    // Helper methods for AbstractOperationBuilder
    // ========================================

    public static <T extends AbstractOperationBuilder<T>> T addReadOp(T opBuilder, String binName, String dsl, int flags) {
        return opBuilder.addOp(createReadOp(binName, dsl, flags));
    }

    public static <T extends AbstractOperationBuilder<T>> T addWriteOp(T opBuilder, String binName, String dsl, int flags) {
        return opBuilder.addOp(createWriteOp(binName, dsl, flags));
    }

    // ========================================
    // DSL parsing methods
    // ========================================

    private static Expression parseStringDsl(String dsl) {
        DSLParser parser = new DSLParserImpl();
        ExpressionContext context = ExpressionContext.of(dsl);
        ParsedExpression parseResult = parser.parseExpression(context);
        Exp exp = parseResult.getResult().getExp();
        
        if (Log.debugEnabled()) {
                Log.debug(String.format("Dsl(\"%s\") => (Exp: %s)",
                        dsl,
                        exp));
        }

        return Exp.build(exp);
    }

    private static Expression parseStringDsl(String dsl, Object[] params) {
        // For now, format the DSL string with params
        // TODO: Use proper prepared statement support when DSL supports it
        String formattedDsl = formatDsl(dsl, params);
        return parseStringDsl(formattedDsl);
    }

    private static Expression fromBooleanExpression(BooleanExpression dsl) {
        return Exp.build(dsl.toAerospikeExp());
    }

    private static Expression fromPreparedDsl(PreparedDsl dsl, Object[] params) {
        String formattedDsl = dsl.formValue(params);
        return parseStringDsl(formattedDsl);
    }

    /**
     * Format a DSL string with parameters.
     * Uses simple positional replacement for ? placeholders.
     */
    private static String formatDsl(String dsl, Object[] params) {
        if (params == null || params.length == 0) {
            return dsl;
        }
        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        for (int i = 0; i < dsl.length(); i++) {
            char c = dsl.charAt(i);
            if (c == '?' && paramIndex < params.length) {
                result.append(formatParam(params[paramIndex++]));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private static String formatParam(Object param) {
        if (param == null) {
            return "null";
        } else if (param instanceof String) {
            return "\"" + param + "\"";
        } else if (param instanceof Number || param instanceof Boolean) {
            return param.toString();
        } else {
            return "\"" + param.toString() + "\"";
        }
    }
}
