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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Expression;

/**
 * Server-compiled AEL materialization for field {@code 43} paths.
 */
class AelMaterializerWhereTest extends ClusterTest {

    @Test
    void expressionFromString_usesServerCompiledOp128WhenSupportsAel() {
        Expression expression = AelMaterializer.expressionFromString(cluster, "$.age > 30");

        assertThat(expression).isNotNull();
        if (cluster.supportsAel()) {
            assertThat(isServerCompiledAelWire(expression)).isTrue();
        }
    }

    @Test
    void whereClauseProcessor_stringAel_materializesServerCompiledFilter() {
        var where = com.aerospike.client.sdk.query.WhereClauseProcessor.from("$.age > 30");
        Expression expression = where.toFilterExpression(session);

        assertThat(expression).isNotNull();
        if (cluster.supportsAel()) {
            assertThat(isServerCompiledAelWire(expression)).isTrue();
        }
    }

    private static boolean isServerCompiledAelWire(Expression expression) {
        byte[] bytes = expression.getBytes();
        return bytes.length >= 3
            && bytes[0] == (byte) 0x92
            && bytes[2] == (byte) Expression.SERVER_COMPILED_AEL_EXPRESSION_OP;
    }
}
