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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.query.WhereClauseProcessor;

/**
 * Server-compiled AEL materialization for field {@code 43} paths.
 *
 * <p>Wire layout details are covered by {@link com.aerospike.client.sdk.exp.ServerCompiledFilterWireTest}.</p>
 */
public class AelMaterializerWhereTest extends ClusterTest {

    private static final String AEL = "$.age > 30";

    @BeforeAll
    static void requireAel() {
        assumeSupportsAel();
    }

    @Test
    void stringAel_materializesServerCompiledFilterFromMaterializerAndWhereProcessor() {
        Expression fromMaterializer = AelMaterializer.expressionFromString(cluster, AEL);
        Expression fromWhere = com.aerospike.client.sdk.query.WhereClauseProcessor.from(AEL)
            .toFilterExpression(session);

        assertThat(fromMaterializer).isNotNull();
        assertThat(fromWhere).isNotNull();
        assertThat(isServerCompiledAelWire(fromMaterializer)).isTrue();
        assertThat(isServerCompiledAelWire(fromWhere)).isTrue();
    }

    @Test
    void preparedAel_materializesServerCompiledFilterFromWhereProcessor() {
        PreparedAel prepared = PreparedAel.prepare("$.age > ?0");
        Expression fromWhere = WhereClauseProcessor.from(prepared, 30)
            .toFilterExpression(session);

        assertThat(fromWhere).isNotNull();
        assertThat(isServerCompiledAelWire(fromWhere)).isTrue();
    }

    @Test
    void toFilterExp_session_delegatesToCluster() {
        WhereClauseProcessor where = WhereClauseProcessor.from(AEL);

        assertThat(where.toFilterExp(session)).isNotNull();
    }

    @Test
    void toFilterExpression_nullExpWhere_returnsNull() {
        assertNull(WhereClauseProcessor.from((Exp) null).toFilterExpression(session));
    }

    private static boolean isServerCompiledAelWire(Expression expression) {
        byte[] bytes = expression.getBytes();
        return bytes.length >= 3
            && bytes[0] == (byte) 0x92
            && bytes[2] == (byte) Expression.SERVER_COMPILED_AEL_EXPRESSION_OP;
    }
}
