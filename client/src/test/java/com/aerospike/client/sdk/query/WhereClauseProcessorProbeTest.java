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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.AelMaterializer;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.exp.Expression;

class WhereClauseProcessorProbeTest extends ClusterTest {

    @Test
    void toProbeExpressionUsesFullClientCompileWithoutIndexSelection() {
        String ael = "$.age > 30 && $.country == \"US\"";
        WhereClauseProcessor where = WhereClauseProcessor.from(true, ael);

        ParseResult executeParse = where.process(args.namespace, args.set.getSet(), session);
        Expression probeExpression = where.toProbeExpression(session);
        Expression expected = AelMaterializer.expressionForQueryProbe(ael);

        assertNotNull(probeExpression.getBytes());
        assertArrayEquals(expected.getBytes(), probeExpression.getBytes());

        if (executeParse.getFilter() != null) {
            assertNotNull(executeParse.getExpression());
        }
    }

    @Test
    void toProbeExpressionFromBooleanExpression() {
        WhereClauseProcessor where = WhereClauseProcessor.from(
            com.aerospike.client.sdk.ael.Ael.longBin("age").gt(30));
        Expression probeExpression = where.toProbeExpression(session);
        assertNotNull(probeExpression.getBytes());
    }
}
