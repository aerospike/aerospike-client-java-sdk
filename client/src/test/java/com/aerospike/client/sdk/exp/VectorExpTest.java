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

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/**
 * Client-side wire-encoding tests for {@link VectorExp}. These validate that the
 * expression packs to the expected MessagePack layout regardless of server
 * support (the server-side vector distance expression is still in progress).
 */
class VectorExpTest {

    private static final int VECTOR_DIST_OP = 52;

    @Test
    void distancePacksExpectedWireFormat() {
        final Vector query = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
        final byte[] queryBytes = query.getElementBytes();

        final byte[] packed = Exp.build(
            VectorExp.distance(VectorDistanceMetric.COSINE, query, Exp.vectorBin("embedding")))
            .getBytes();

        // Array header for the 4-element VectorDist node: 0x90 | 4.
        assertEquals((byte)(0x90 | 4), packed[0]);
        // VECTOR_DIST op code.
        assertEquals(VECTOR_DIST_OP, packed[1] & 0xff);
        // Metric code (COSINE == 2).
        assertEquals(VectorDistanceMetric.COSINE.getCode(), packed[2] & 0xff);

        // packParticleBytes(query): string header for (len + 1), then the BLOB
        // particle type byte, then the little-endian query element bytes.
        final int expectedHeader = 0xa0 | (queryBytes.length + 1);
        assertEquals((byte)expectedHeader, packed[3]);
        assertEquals(ParticleType.BLOB, packed[4] & 0xff);

        final byte[] embedded = Arrays.copyOfRange(packed, 5, 5 + queryBytes.length);
        assertArrayEquals(queryBytes, embedded);

        // Immediately after the query payload, the bin sub-expression begins with
        // its own 3-element array header (0x90 | 3) and the BIN op code (81).
        final int binStart = 5 + queryBytes.length;
        assertEquals((byte)(0x90 | 3), packed[binStart]);
        assertEquals(81, packed[binStart + 1] & 0xff);
    }

    @Test
    void distanceEncodesEachMetricCode() {
        final Vector query = Vector.ofFloat32(new float[] {1.0f, 2.0f});

        for (final VectorDistanceMetric metric : VectorDistanceMetric.values()) {
            final byte[] packed = Exp.build(
                VectorExp.distance(metric, query, Exp.vectorBin("v"))).getBytes();

            assertEquals(VECTOR_DIST_OP, packed[1] & 0xff);
            assertEquals(metric.getCode(), packed[2] & 0xff);
        }
    }

    @Test
    void distanceUsesHeaderlessElementBytesForQuery() {
        // The query is sent as headerless little-endian element bytes (not the
        // full vector wire value with its 8-byte header).
        final Vector query = Vector.ofInt32(new int[] {-5, 0, 7, 12345});
        final byte[] queryBytes = query.getElementBytes();

        assertEquals(query.dimensions * Vector.ElementType.INT32.getByteSize(), queryBytes.length);

        final byte[] packed = Exp.build(
            VectorExp.distance(VectorDistanceMetric.EUCLIDEAN, query, Exp.vectorBin("v"))).getBytes();

        final byte[] embedded = Arrays.copyOfRange(packed, 5, 5 + queryBytes.length);
        assertArrayEquals(queryBytes, embedded);
    }

    @Test
    void metricCodes() {
        assertEquals(0, VectorDistanceMetric.EUCLIDEAN.getCode());
        assertEquals(1, VectorDistanceMetric.DOT_PRODUCT.getCode());
        assertEquals(2, VectorDistanceMetric.COSINE.getCode());
    }
}
