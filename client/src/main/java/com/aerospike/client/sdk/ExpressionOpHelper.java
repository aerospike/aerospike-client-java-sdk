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
     * @param ael AEL text (encoding follows {@link Cluster#supportsAel()} for this cluster)
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
     * @param cluster cluster whose minimum server version determines client vs server AEL wire form
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, String ael, int flags, Cluster cluster) {
        return ExpOperation.read(binName, AelMaterializer.expressionFromString(cluster, ael), flags);
    }

    /**
     * Builds a read expression operation from a AEL string with {@code ?0}, {@code ?1}, ... placeholders.
     *
     * @param binName target bin
     * @param ael AEL template
     * @param params values substituted for {@code ?0}, {@code ?1}, ... in order
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
     * @param cluster cluster whose minimum server version determines client vs server AEL wire form
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, String ael, Object[] params, int flags, Cluster cluster) {
        return ExpOperation.read(binName, AelMaterializer.expressionFromString(cluster, ael, params), flags);
    }

    // ========================================
    // Read operations - from other AEL types
    // ========================================

    /**
     * Builds a read expression operation from a {@link BooleanExpression}.
     *
     * @param binName target bin
     * @param ael programmatic boolean expression
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
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
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
     * @param cluster cluster whose minimum server version determines client vs server AEL wire form
     * @return Aerospike {@link Operation} for expression read
     */
    public static Operation createReadOp(String binName, PreparedAel ael, Object[] params, int flags, Cluster cluster) {
        return ExpOperation.read(binName, AelMaterializer.expressionFromPrepared(cluster, ael, params), flags);
    }

    /**
     * Builds a read expression operation from a {@link Exp} builder.
     *
     * @param binName target bin
     * @param exp expression builder (compiled via {@link Exp#build(Exp)})
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
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
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
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
     * @param ael AEL text (encoding follows {@link Cluster#supportsAel()} for this cluster)
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
     * @param cluster cluster whose minimum server version determines client vs server AEL wire form
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, String ael, int flags, Cluster cluster) {
        return ExpOperation.write(binName, AelMaterializer.expressionFromString(cluster, ael), flags);
    }

    /**
     * Builds a write expression operation from a AEL string with {@code ?0}, {@code ?1}, ... placeholders.
     *
     * @param binName target bin
     * @param ael AEL template
     * @param params values substituted for {@code ?0}, {@code ?1}, ... in order
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
     * @param cluster cluster whose minimum server version determines client vs server AEL wire form
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, String ael, Object[] params, int flags, Cluster cluster) {
        return ExpOperation.write(binName, AelMaterializer.expressionFromString(cluster, ael, params), flags);
    }

    // ========================================
    // Write operations - from other AEL types
    // ========================================

    /**
     * Builds a write expression operation from a {@link BooleanExpression}.
     *
     * @param binName target bin
     * @param ael programmatic boolean expression
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
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
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
     * @param cluster cluster whose minimum server version determines client vs server AEL wire form
     * @return Aerospike {@link Operation} for expression write
     */
    public static Operation createWriteOp(String binName, PreparedAel ael, Object[] params, int flags, Cluster cluster) {
        return ExpOperation.write(binName, AelMaterializer.expressionFromPrepared(cluster, ael, params), flags);
    }

    /**
     * Builds a write expression operation from a {@link Exp} builder.
     *
     * @param binName target bin
     * @param exp expression builder (compiled via {@link Exp#build(Exp)})
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
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
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
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
     * @param flags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask
     * @return {@code opBuilder} for chaining
     */
    public static <T extends AbstractOperationBuilder<T>> T addReadOp(T opBuilder, String binName, String ael, int flags) {
        return opBuilder.addOp(createReadOp(binName, ael, flags, opBuilder.getSession().getCluster()));
    }

    /**
     * Appends a AEL-based write expression operation to {@code opBuilder}.
     *
     * @param opBuilder operation list builder
     * @param binName target bin
     * @param ael AEL text
     * @param flags {@link com.aerospike.client.sdk.exp.ExpWriteFlags} bitmask
     * @return {@code opBuilder} for chaining
     */
    public static <T extends AbstractOperationBuilder<T>> T addWriteOp(T opBuilder, String binName, String ael, int flags) {
        return opBuilder.addOp(createWriteOp(binName, ael, flags, opBuilder.getSession().getCluster()));
    }

    private static Expression fromBooleanExpression(BooleanExpression ael) {
        return Exp.build(ael.toAerospikeExp());
    }
}
