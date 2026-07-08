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

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.exp.Expression;

/**
 * WHERE materialization for field {@code 43} paths ({@code parseWhereFromString}).
 */
class AelMaterializerWhereTest extends ClusterTest {

    @Test
    void parseWhereFromString_allowsIndexFalse_usesServerCompiledOp128WhenSupportsAel() {
        ParseResult result = AelMaterializer.parseWhereFromString(
            session,
            false,
            args.namespace,
            null,
            "$.age > 30"
        );

        assertThat(result.getFilter()).isNull();
        assertThat(result.getExpression()).isNotNull();
        if (cluster.supportsAel()) {
            assertThat(isServerCompiledAelWire(result.getExpression())).isTrue();
        }
        else {
            assertThat(isServerCompiledAelWire(result.getExpression())).isFalse();
        }
    }

    @Test
    void parseWhereFromString_allowsIndexTrue_mayProduceFilter() {
        ParseResult result = AelMaterializer.parseWhereFromString(
            session,
            true,
            args.namespace,
            "aelmaterializerwhere",
            "$.age > 30"
        );

        assertThat(result.getExpression()).isNotNull();
        assertThat(isServerCompiledAelWire(result.getExpression())).isFalse();
    }

    @Test
    void requiresLegacyClientIndexSelection_scalarInteger_false() {
        assertThat(AelMaterializer.requiresLegacyClientIndexSelection(
            session, true, args.namespace, "users", "$.age > 30")).isFalse();
    }

    @Test
    void requiresLegacyClientIndexSelection_primaryIndexPredicate_false() {
        assertThat(AelMaterializer.requiresLegacyClientIndexSelection(
            session, true, args.namespace, "qselint", "$.country == 'US'")).isFalse();
    }

    @Test
    void requiresLegacyClientIndexSelection_mapKeys_true() {
        assertThat(AelMaterializer.requiresLegacyClientIndexSelection(
            session,
            true,
            args.namespace,
            "querycoll",
            "$.map_bin.mkey2.get(return: EXISTS) == true")).isTrue();
    }

    @Test
    void requiresLegacyClientIndexSelection_blob_true() {
        assertThat(AelMaterializer.requiresLegacyClientIndexSelection(
            session, true, args.namespace, "queryblob", "$.bb == x'000000000000c350'")).isTrue();
    }

    private static boolean isServerCompiledAelWire(Expression expression) {
        byte[] bytes = expression.getBytes();
        return bytes.length >= 3
            && bytes[0] == (byte) 0x92
            && bytes[2] == (byte) Expression.SERVER_COMPILED_AEL_EXPRESSION_OP;
    }
}
