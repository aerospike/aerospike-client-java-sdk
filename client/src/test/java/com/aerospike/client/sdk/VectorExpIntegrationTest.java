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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.VectorExp;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/**
 * Server round-trip tests for the vector distance expression ({@link VectorExp#distance}).
 * <p>
 * DISABLED: the server does not yet implement the vector distance expression op. The distance math
 * kernels exist ({@code as/src/base/vector_math.c}: euclidean-squared, dot-product, cosine), but the
 * expression engine has no {@code EXP_VECTOR_DIST} op code: {@code as/include/exp/exp_wire.h} defines
 * {@code EXP_MIN=50}, {@code EXP_MAX=51}, then jumps to {@code EXP_META_DIGEST_MOD=64}; op 52 is
 * rejected at expression build time with {@code PARAMETER_ERROR}.
 * <p>
 * When the server wires up {@code EXP_VECTOR_DIST}, re-enable these and revisit metric semantics: the
 * server currently computes Euclidean as <b>L2-squared</b> (see {@code vector_type_design.md}), which
 * differs from the raw-L2 reading in {@link VectorDistanceMetric}.
 */
@Disabled("Server op EXP_VECTOR_DIST (52) is not implemented; see exp_wire.h.")
public class VectorExpIntegrationTest extends ClusterTest {
    private static final String vecBin = "embedding";
    private static final String distBin = "dist";
    private static final String keyPrefix = "vecexp";
    private static final int size = 8;
    private static final int dims = 4;

    private List<Key> seed() {
        final List<Key> keys = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            final Key key = args.set.id(keyPrefix + i);
            session.delete(key).execute();

            final float[] data = new float[dims];
            for (int d = 0; d < dims; d++) {
                data[d] = i + d * 0.1f;
            }
            session.upsert(key).bin(vecBin).setTo(Vector.ofFloat32(data)).execute();
            keys.add(key);
        }
        return keys;
    }

    private static Vector query(final int base) {
        final float[] data = new float[dims];
        for (int d = 0; d < dims; d++) {
            data[d] = base + d * 0.1f;
        }
        return Vector.ofFloat32(data);
    }

    @Test
    public void euclideanDistanceProjection() {
        assertDistanceProjectionComputes(VectorDistanceMetric.EUCLIDEAN);
    }

    @Test
    public void dotProductDistanceProjection() {
        assertDistanceProjectionComputes(VectorDistanceMetric.DOT_PRODUCT);
    }

    @Test
    public void cosineDistanceProjection() {
        assertDistanceProjectionComputes(VectorDistanceMetric.COSINE);
    }

    private void assertDistanceProjectionComputes(final VectorDistanceMetric metric) {
        final List<Key> keys = seed();

        final RecordStream rs = session.query(keys)
            .bin(distBin).selectFrom(
                VectorExp.distance(metric, query(0), Exp.vectorBin(vecBin)))
            .execute();

        int count = 0;
        try {
            while (rs.hasNext()) {
                final Record rec = rs.next().recordOrThrow();
                // The server returns a 64-bit float distance for each record.
                final double dist = rec.getDouble(distBin);
                assertTrue(Double.isFinite(dist),
                    "distance should be a finite number, got " + dist);
                count++;
            }
        }
        finally {
            rs.close();
        }

        assertEquals(size, count, "every seeded record should yield a projected distance");
    }

    @Test
    public void euclideanSelfDistanceIsZero() {
        // Regardless of whether EUCLIDEAN is L2 or L2-squared on the server, the
        // distance from a vector to itself must be zero.
        final List<Key> keys = seed();
        final int self = 3;

        final RecordStream rs = session.query(keys)
            .bin("id").selectFrom("$." + vecBin)      // placeholder read to keep bin ordering deterministic
            .bin(distBin).selectFrom(
                VectorExp.distance(VectorDistanceMetric.EUCLIDEAN, query(self), Exp.vectorBin(vecBin)))
            .execute();

        double selfDistance = Double.NaN;
        try {
            while (rs.hasNext()) {
                final Record rec = rs.next().recordOrThrow();
                final double dist = rec.getDouble(distBin);
                // Identify the record whose embedding equals query(self): its first
                // element equals `self`.
                final Vector stored = rec.getVector("id");
                if (stored != null && stored.getFloat32Data()[0] == (float)self) {
                    selfDistance = dist;
                }
            }
        }
        finally {
            rs.close();
        }

        assertEquals(0.0, selfDistance, 1e-6, "distance from a vector to itself should be zero");
    }
}
