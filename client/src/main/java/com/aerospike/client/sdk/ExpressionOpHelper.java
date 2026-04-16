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
package com.aerospike.client.sdk;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.impl.AelParserImpl;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpOperation;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Helper class for creating expression operations from various AEL input types.
 *
 * <p>Supports the following input types:</p>
 * <ul>
 *   <li>{@code String} - AEL string expression</li>
 *   <li>{@code BooleanExpression} - Programmatic boolean expression</li>
 *   <li>{@code PreparedAel} - Prepared AEL statement with parameters</li>
 *   <li>{@code Exp} - Low-level expression builder</li>
 *   <li>{@code Expression} - Compiled expression</li>
 * </ul>
 */
public final class ExpressionOpHelper {

    private ExpressionOpHelper() {
        // Utility class
    }

    // ========================================
    // Read operations - from String AEL
    // ========================================

    /**
     * Builds a read expression operation from a AEL string.
     *
     * @param binName target bin
     * @param ael AEL text parsed to an {@link Expression}
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, String ael, int flags) {
        return ExpOperation.read(binName, parseStringAel(ael), flags);
    }

    /**
     * Builds a read expression operation from a AEL string with {@code ?} placeholders replaced by {@code params}.
     *
     * @param binName target bin
     * @param ael AEL template
     * @param params values substituted for {@code ?} in order
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, String ael, Object[] params, int flags) {
        return ExpOperation.read(binName, parseStringAel(ael, params), flags);
    }

    // ========================================
    // Read operations - from other AEL types
    // ========================================

    /**
     * Builds a read expression operation from a {@link BooleanExpression}.
     *
     * @param binName target bin
     * @param ael programmatic boolean expression
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, BooleanExpression ael, int flags) {
        return ExpOperation.read(binName, fromBooleanExpression(ael), flags);
    }

    /**
     * Builds a read expression operation from a {@link PreparedAel} with bound parameters.
     *
     * @param binName target bin
     * @param ael prepared AEL
     * @param params bound parameter values
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, PreparedAel ael, Object[] params, int flags) {
        return ExpOperation.read(binName, fromPreparedAel(ael, params), flags);
    }

    /**
     * Builds a read expression operation from a {@link Exp} builder.
     *
     * @param binName target bin
     * @param exp expression builder (compiled via {@link Exp#build(Exp)})
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, Exp exp, int flags) {
        return ExpOperation.read(binName, Exp.build(exp), flags);
    }

    /**
     * Builds a read expression operation from a compiled {@link Expression}.
     *
     * @param binName target bin
     * @param exp compiled expression
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, Expression exp, int flags) {
        return ExpOperation.read(binName, exp, flags);
    }

    // ========================================
    // Write operations - from String AEL
    // ========================================

    /**
     * Builds a write expression operation from a AEL string.
     *
     * @param binName target bin
     * @param ael AEL text parsed to an {@link Expression}
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, String ael, int flags) {
        return ExpOperation.write(binName, parseStringAel(ael), flags);
    }

    /**
     * Builds a write expression operation from a AEL string with {@code ?} placeholders replaced by {@code params}.
     *
     * @param binName target bin
     * @param ael AEL template
     * @param params values substituted for {@code ?} in order
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, String ael, Object[] params, int flags) {
        return ExpOperation.write(binName, parseStringAel(ael, params), flags);
    }

    // ========================================
    // Write operations - from other AEL types
    // ========================================

    /**
     * Builds a write expression operation from a {@link BooleanExpression}.
     *
     * @param binName target bin
     * @param ael programmatic boolean expression
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, BooleanExpression ael, int flags) {
        return ExpOperation.write(binName, fromBooleanExpression(ael), flags);
    }

    /**
     * Builds a write expression operation from a {@link PreparedAel} with bound parameters.
     *
     * @param binName target bin
     * @param ael prepared AEL
     * @param params bound parameter values
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, PreparedAel ael, Object[] params, int flags) {
        return ExpOperation.write(binName, fromPreparedAel(ael, params), flags);
    }

    /**
     * Builds a write expression operation from a {@link Exp} builder.
     *
     * @param binName target bin
     * @param exp expression builder (compiled via {@link Exp#build(Exp)})
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, Exp exp, int flags) {
        return ExpOperation.write(binName, Exp.build(exp), flags);
    }

    /**
     * Builds a write expression operation from a compiled {@link Expression}.
     *
     * @param binName target bin
     * @param exp compiled expression
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, Expression exp, int flags) {
        return ExpOperation.write(binName, exp, flags);
    }

    // ========================================
    // Helper methods for AbstractOperationBuilder
    // ========================================

    /**
     * Appends a AEL-based read expression operation to {@code opBuilder}.
     *
     * @param opBuilder operation list builder
     * @param binName target bin
     * @param ael AEL text
     * @param flags {@link com.aerospike.client.exp.ExpReadFlags} bitmask
     * @return {@code opBuilder} for chaining
     */
    public static <T extends AbstractOperationBuilder<T>> T addReadOp(T opBuilder, String binName, String ael, int flags) {
        return opBuilder.addOp(createReadOp(binName, ael, flags));
    }

    /**
     * Appends a AEL-based write expression operation to {@code opBuilder}.
     *
     * @param opBuilder operation list builder
     * @param binName target bin
     * @param ael AEL text
     * @param flags {@link com.aerospike.client.exp.ExpWriteFlags} bitmask
     * @return {@code opBuilder} for chaining
     */
    public static <T extends AbstractOperationBuilder<T>> T addWriteOp(T opBuilder, String binName, String ael, int flags) {
        return opBuilder.addOp(createWriteOp(binName, ael, flags));
    }

    // ========================================
    // AEL parsing methods
    // ========================================

    private static Expression parseStringAel(String ael) {
        AelParser parser = new AelParserImpl();
        ExpressionContext context = ExpressionContext.of(ael);
        ParsedExpression parseResult = parser.parseExpression(context);
        Exp exp = parseResult.getResult().getExp();

        if (Log.debugEnabled()) {
                Log.debug(String.format("Ael(\"%s\") => (Exp: %s)",
                        ael,
                        exp));
        }

        return Exp.build(exp);
    }

    private static Expression parseStringAel(String ael, Object[] params) {
        // For now, format the AEL string with params
        // TODO: Use proper prepared statement support when AEL supports it
        String formattedAel = formatAel(ael, params);
        return parseStringAel(formattedAel);
    }

    private static Expression fromBooleanExpression(BooleanExpression ael) {
        return Exp.build(ael.toAerospikeExp());
    }

    private static Expression fromPreparedAel(PreparedAel ael, Object[] params) {
        String formattedAel = ael.formValue(params);
        return parseStringAel(formattedAel);
    }

    /**
     * Format a AEL string with parameters.
     * Uses simple positional replacement for ? placeholders.
     */
    private static String formatAel(String ael, Object[] params) {
        if (params == null || params.length == 0) {
            return ael;
        }
        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        for (int i = 0; i < ael.length(); i++) {
            char c = ael.charAt(i);
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
