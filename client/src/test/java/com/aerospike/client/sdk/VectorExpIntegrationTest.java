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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assumptions;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.VectorExp;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/** Vector-distance expression integration tests. */
public class VectorExpIntegrationTest extends ClusterTest {
    private static final String vecBin = "embedding";
    private static final String distBin = "dist";
    private static final String keyPrefix = "vecexp";
    private static final int size = 8;
    private static final int dims = 4;

    @BeforeAll
    static void requireVectorServer() {
        Assumptions.assumeTrue(
            cluster.getVersion().isGreaterOrEqual(com.aerospike.client.sdk.util.Version.SERVER_VERSION_8_1_3),
            "vector expressions require server version 8.1.3+");
    }

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
        final List<Key> keys = seed();
        final int self = 3;

        final RecordStream rs = session.query(keys)
            .bin(distBin).selectFrom(
                VectorExp.distance(VectorDistanceMetric.EUCLIDEAN, query(self), Exp.vectorBin(vecBin)))
            .execute();

        double nearestDistance = Double.POSITIVE_INFINITY;
        try {
            while (rs.hasNext()) {
                final Record rec = rs.next().recordOrThrow();
                final double dist = rec.getDouble(distBin);
                nearestDistance = Math.min(nearestDistance, dist);
            }
        }
        finally {
            rs.close();
        }

        assertEquals(0.0, nearestDistance, 1e-6, "distance from a vector to itself should be zero");
    }

    @Test
    public void cosineSelfSimilarityIsOne() {
        final List<Key> keys = seed();
        final int self = 3;

        double best = Double.NEGATIVE_INFINITY;
        try (RecordStream rs = session.query(keys)
            .bin(distBin).selectFrom(
                VectorExp.distance(VectorDistanceMetric.COSINE, query(self), Exp.vectorBin(vecBin)))
            .execute()) {
            while (rs.hasNext()) {
                best = Math.max(best, rs.next().recordOrThrow().getDouble(distBin));
            }
        }

        assertEquals(1.0, best, 1e-6, "cosine self-similarity should be one");
    }

    @Test
    public void dotProductRanksLargerAsMoreSimilar() {
        final DataSet dataSet = DataSet.of(args.namespace, "vector_dot_dir");
        for (int i = 1; i <= 4; i++) {
            final Key key = dataSet.id("dot-" + i);
            session.delete(key).execute();
            session.upsert(key).bin("id").setTo((long)i)
                .bin(vecBin).setTo(Vector.ofFloat32(new float[] {i, 0.0f})).execute();
        }

        long bestId = -1;
        double bestDot = Double.NEGATIVE_INFINITY;
        try (RecordStream rs = session.query(dataSet)
            .bin("id").get()
            .bin(distBin).selectFrom(VectorExp.distance(
                VectorDistanceMetric.DOT_PRODUCT,
                Vector.ofFloat32(new float[] {1.0f, 0.0f}),
                Exp.vectorBin(vecBin)))
            .execute()) {
            while (rs.hasNext()) {
                final Record rec = rs.next().recordOrThrow();
                final double dot = rec.getDouble(distBin);
                if (dot > bestDot) {
                    bestDot = dot;
                    bestId = rec.getLong("id");
                }
            }
        }

        assertEquals(4L, bestId);
        assertEquals(4.0, bestDot, 1e-6);
    }

    @Test
    public void vectorKnnTopKReturnsNearestRecordsInDistanceOrder() {
        final DataSet dataSet = DataSet.of(args.namespace, "vector_knn_topk");
        for (int i = 0; i < 6; i++) {
            final Key key = dataSet.id("knn-" + i);
            session.delete(key).execute();
            session.upsert(key)
                .bin(vecBin).setTo(Vector.ofFloat32(new float[] {i, 0.0f}))
                .execute();
        }

        final List<Double> distances = new ArrayList<>();
        try (RecordStream rs = session.query(dataSet)
            .bin(distBin).selectFrom(VectorExp.distance(
                VectorDistanceMetric.EUCLIDEAN,
                Vector.ofFloat32(new float[] {0.0f, 0.0f}),
                Exp.vectorBin(vecBin)))
            .orderBy(distBin, com.aerospike.client.sdk.query.OrderByType.DOUBLE,
                com.aerospike.client.sdk.query.Order.ASC)
            .topK(3)
            .execute()) {
            while (rs.hasNext()) {
                distances.add(rs.next().recordOrThrow().getDouble(distBin));
            }
        }

        assertEquals(List.of(0.0, 1.0, 4.0), distances);
    }

    @Test
    public void incomparableDistanceOperandsYieldNilWithNoFail() {
        assertUnknownDistance("wrong-type", "not-a-vector");
        assertUnknownDistance("wrong-dimensions", Vector.ofFloat32(new float[] {1.0f, 2.0f}));
        assertUnknownDistance("wrong-element-type", Vector.ofInt32(new int[] {1, 2, 3, 4}));
    }

    private void assertUnknownDistance(String suffix, Object value) {
        Key key = args.set.id("vecexp-unknown-" + suffix);
        session.delete(key).execute();
        session.upsert(key).bin(vecBin).setTo(value).execute();

        Record result = session.query(key)
            .bin(distBin).selectFrom(
                VectorExp.distance(VectorDistanceMetric.EUCLIDEAN, query(0), Exp.vectorBin(vecBin)),
                options -> options.ignoreEvalFailure())
            .execute()
            .getFirstRecord();

        assertNull(result.getValue(distBin), "incomparable vector " + suffix + " must evaluate to NIL");
    }
}
