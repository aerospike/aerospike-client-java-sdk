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
package com.aerospike.client.sdk.exp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/**
 * Client-side wire-encoding tests for {@link VectorExp}.
 */
public class VectorExpTest {

    @Test
    void distancePacksExpectedWireFormat() {
        final Vector query = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
        final byte[] queryBytes = query.getWireBytes();

        final byte[] packed = Exp.build(
            VectorExp.distance(VectorDistanceMetric.COSINE, query, Exp.vectorBin("embedding")))
            .getBytes();

        // [cosine-opcode, vector-bin-expression, full-vector BLOB literal].
        assertEquals((byte)(0x90 | 3), packed[0]);
        assertEquals(54, packed[1] & 0xff);
        assertEquals((byte)(0x90 | 3), packed[2]);
        assertEquals(81, packed[3] & 0xff);
        assertEquals(10, packed[4] & 0xff);

        // The query literal is a BLOB containing the complete VECTOR wire value.
        final int expectedHeader = 0xa0 | (queryBytes.length + 1);
        final int literalStart = 2 + 4 + "embedding".length();
        assertEquals((byte)expectedHeader, packed[literalStart]);
        assertEquals(ParticleType.BLOB, packed[literalStart + 1] & 0xff);

        final byte[] embedded = Arrays.copyOfRange(
            packed, literalStart + 2, literalStart + 2 + queryBytes.length);
        assertArrayEquals(queryBytes, embedded);
    }

    @Test
    void distanceEncodesEachMetricCode() {
        final Vector query = Vector.ofFloat32(new float[] {1.0f, 2.0f});

        for (final VectorDistanceMetric metric : VectorDistanceMetric.values()) {
            final byte[] packed = Exp.build(
                VectorExp.distance(metric, query, Exp.vectorBin("v"))).getBytes();

            assertEquals(expectedOpcode(metric), packed[1] & 0xff);
        }
    }

    @Test
    void distanceUsesFullVectorWireValueForQuery() {
        final Vector query = Vector.ofInt32(new int[] {-5, 0, 7, 12345});
        final byte[] queryBytes = query.getWireBytes();

        assertEquals(Vector.HEADER_SIZE + query.dimensions * Vector.ElementType.INT32.getByteSize(),
            queryBytes.length);

        final byte[] packed = Exp.build(
            VectorExp.distance(VectorDistanceMetric.EUCLIDEAN, query, Exp.vectorBin("v"))).getBytes();

        final int literalStart = 2 + 4 + 1;
        final byte[] embedded = Arrays.copyOfRange(
            packed, literalStart + 2, literalStart + 2 + queryBytes.length);
        assertArrayEquals(queryBytes, embedded);
    }

    private static int expectedOpcode(VectorDistanceMetric metric) {
        return switch (metric) {
            case EUCLIDEAN -> 52;
            case DOT_PRODUCT -> 53;
            case COSINE -> 54;
        };
    }

    @Test
    void distanceExpressionReportsHasVector() {
        assertTrue(Exp.build(VectorExp.distance(
            VectorDistanceMetric.COSINE, Vector.ofFloat32(new float[] {1.0f, 2.0f}),
            Exp.vectorBin("v"))).hasVector());
    }

    @Test
    void plainExpressionReportsNoVector() {
        assertFalse(Exp.build(Exp.eq(Exp.intBin("n"), Exp.val(1))).hasVector());
    }

    @Test
    void expressionOperationPropagatesVectorFlagForGuard() {
        // Vector distance expressions must propagate hasVector.
        Operation vectorOp = ExpOperation.read("dist", Exp.build(VectorExp.distance(
            VectorDistanceMetric.EUCLIDEAN, Vector.ofFloat32(new float[] {0.0f, 0.0f}),
            Exp.vectorBin("v"))), 0);
        assertTrue(vectorOp.value.hasVector());

        Operation plainOp = ExpOperation.read("out",
            Exp.build(Exp.add(Exp.intBin("a"), Exp.val(1))), 0);
        assertFalse(plainOp.value.hasVector());
    }

    @Test
    void wrappedVectorExpressionPropagatesHasVector() {
        Expression inner = Exp.build(VectorExp.distance(
            VectorDistanceMetric.COSINE, Vector.ofFloat32(new float[] {1.0f, 2.0f}),
            Exp.vectorBin("v")));

        assertTrue(Exp.build(Exp.gt(Exp.expr(inner), Exp.val(0.5))).hasVector());
        assertFalse(Exp.build(Exp.gt(
            Exp.expr(Exp.build(Exp.intBin("n"))), Exp.val(0L))).hasVector());
    }
}
