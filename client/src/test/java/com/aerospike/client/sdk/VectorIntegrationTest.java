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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.util.Version;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.Vector.ElementType;

/** VECTOR integration tests. */
public class VectorIntegrationTest extends ClusterTest {
    private static final String binName = "vecbin";

    @BeforeAll
    static void requireVectorServer() {
        Assumptions.assumeTrue(
            cluster.getServerVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_3),
            "VECTOR requires server version 8.1.3+");
    }

    /** Representative four-element vector. */
    private static Vector sample(final ElementType t) {
        switch (t) {
            case FLOAT16: return Vector.ofFloat16(new short[] {0x3c00, (short)0xbc00, 0x4000, 0x0000});
            case INT32:   return Vector.ofInt32(new int[] {-5, 0, 7, 12345});
            case FLOAT32: return Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.14159f, 0.0f});
            case FLOAT64: return Vector.ofFloat64(new double[] {1.5, -2.25, 3.14159, 0.0});
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    /**
     * A vector exercising special/boundary values for the given element type. Only finite values:
     * the server rejects NaN and infinity (see {@link #nonFiniteElementsAreRejected}).
     */
    private static Vector special(final ElementType t) {
        switch (t) {
            case FLOAT16:
                // 0, -0, 1.0, -1.0, smallest subnormal, largest subnormal, smallest normal,
                // largest finite (65504), most negative finite (-65504).
                return Vector.ofFloat16(new short[] {
                    0x0000, (short)0x8000, 0x3c00, (short)0xbc00,
                    0x0001, 0x03ff, 0x0400, 0x7bff, (short)0xfbff});
            case INT32:
                return Vector.ofInt32(new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE});
            case FLOAT32:
                return Vector.ofFloat32(new float[] {
                    0.0f, -0.0f, 1.5f, -2.25f, Float.MIN_VALUE, Float.MIN_NORMAL,
                    Float.MAX_VALUE, -Float.MAX_VALUE});
            case FLOAT64:
                return Vector.ofFloat64(new double[] {
                    0.0, -0.0, 1.5, -2.25, Double.MIN_VALUE, Double.MIN_NORMAL,
                    Double.MAX_VALUE, -Double.MAX_VALUE});
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    /**
     * Vectors that each hold one non-finite element (NaN, +Inf, -Inf) among finite ones. INT32 has
     * no non-finite values.
     */
    private static List<Vector> nonFinite(final ElementType t) {
        switch (t) {
            case FLOAT16:
                // NaN (quiet), NaN (other payload), +Inf, -Inf.
                return List.of(
                    Vector.ofFloat16(new short[] {0x3c00, 0x7e00}),
                    Vector.ofFloat16(new short[] {0x3c00, 0x7c01}),
                    Vector.ofFloat16(new short[] {0x3c00, 0x7c00}),
                    Vector.ofFloat16(new short[] {0x3c00, (short)0xfc00}));
            case FLOAT32:
                return List.of(
                    Vector.ofFloat32(new float[] {1.5f, Float.NaN}),
                    Vector.ofFloat32(new float[] {1.5f, Float.POSITIVE_INFINITY}),
                    Vector.ofFloat32(new float[] {1.5f, Float.NEGATIVE_INFINITY}));
            case FLOAT64:
                return List.of(
                    Vector.ofFloat64(new double[] {1.5, Double.NaN}),
                    Vector.ofFloat64(new double[] {1.5, Double.POSITIVE_INFINITY}),
                    Vector.ofFloat64(new double[] {1.5, Double.NEGATIVE_INFINITY}));
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    /** A single-element vector of the given type. */
    private static Vector single(final ElementType t) {
        switch (t) {
            case FLOAT16: return Vector.ofFloat16(new short[] {0x3c00});
            case INT32:   return Vector.ofInt32(new int[] {42});
            case FLOAT32: return Vector.ofFloat32(new float[] {3.5f});
            case FLOAT64: return Vector.ofFloat64(new double[] {3.5});
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    /** A large (high-dimensional) vector of the given type. */
    private static Vector large(final ElementType t, final int dims) {
        switch (t) {
            case FLOAT16: {
                final short[] a = new short[dims];
                for (int i = 0; i < dims; i++) {
                    a[i] = (short)(i & 0xffff);
                }
                return Vector.ofFloat16(a);
            }
            case INT32: {
                final int[] a = new int[dims];
                for (int i = 0; i < dims; i++) {
                    a[i] = i - dims / 2;
                }
                return Vector.ofInt32(a);
            }
            case FLOAT32: {
                final float[] a = new float[dims];
                for (int i = 0; i < dims; i++) {
                    a[i] = i * 0.25f;
                }
                return Vector.ofFloat32(a);
            }
            case FLOAT64: {
                final double[] a = new double[dims];
                for (int i = 0; i < dims; i++) {
                    a[i] = i * 0.5;
                }
                return Vector.ofFloat64(a);
            }
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    // ------------------------------------------------------------------
    // Per-element-type round trips
    // ------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(ElementType.class)
    public void putGetPreservesTypeDimsAndValues(final ElementType t) {
        final Vector v = sample(t);
        final Key key = key("vecput_" + t);
        session.upsert(key).bin(binName).setTo(v).execute();

        final Vector got = readVector(key, binName);
        assertNotNull(got, "vector bin should be present");
        assertEquals(v, got);
        assertSame(t, got.elementType, "element type must be preserved");
        assertEquals(v.dimensions, got.dimensions, "dimensions must be preserved");
        assertArrayEquals(v.getElementBytes(), got.getElementBytes(), "element bytes must be preserved");
    }

    @ParameterizedTest
    @EnumSource(ElementType.class)
    public void specialAndBoundaryValuesRoundTripBitExact(final ElementType t) {
        final Vector v = special(t);
        final Key key = key("vecspecial_" + t);
        session.upsert(key).bin(binName).setTo(v).execute();

        final Vector got = readVector(key, binName);
        assertNotNull(got);
        assertSame(t, got.elementType);
        assertEquals(v.dimensions, got.dimensions);
        // Bit-exact comparison catches negative zero and subnormals.
        assertArrayEquals(v.getElementBytes(), got.getElementBytes());
        assertEquals(v, got);
    }

    /**
     * The server rejects any vector containing NaN or an infinite value (spec R8, R10). The client
     * doesn't check for this itself (optional, R30), so the error comes back from the server.
     */
    @ParameterizedTest
    @EnumSource(value = ElementType.class, mode = EnumSource.Mode.EXCLUDE, names = "INT32")
    public void nonFiniteElementsAreRejected(final ElementType t) {
        int i = 0;
        for (final Vector v : nonFinite(t)) {
            final Key key = key("vecnonfinite_" + t + "_" + i++);
            final AerospikeException e = assertThrows(AerospikeException.class,
                () -> session.upsert(key).bin(binName).setTo(v).execute(), v.toString());
            assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode(), v.toString());
            assertTrue(e.getMessage().contains("vector: elements must be finite (no NaN or infinity)"),
                v + ": unexpected message: " + e.getMessage());
        }
    }

    @ParameterizedTest
    @EnumSource(ElementType.class)
    public void singleDimensionRoundTrip(final ElementType t) {
        final Vector v = single(t);
        final Key key = key("vecsingle_" + t);
        session.upsert(key).bin(binName).setTo(v).execute();

        final Vector got = readVector(key, binName);
        assertEquals(1, got.dimensions);
        assertEquals(v, got);
    }

    @ParameterizedTest
    @EnumSource(ElementType.class)
    public void largeVectorRoundTrip(final ElementType t) {
        final Vector v = large(t, 1024);
        final Key key = key("veclarge_" + t);
        session.upsert(key).bin(binName).setTo(v).execute();

        final Vector got = readVector(key, binName);
        assertEquals(1024, got.dimensions);
        assertArrayEquals(v.getElementBytes(), got.getElementBytes());
        assertEquals(v, got);
    }

    /**
     * Round-trips a 9000-element fp64 vector (72,006 bytes) in a top-level bin, crossing the 16-bit
     * msgpack length boundary.
     */
    @Test
    public void largeVectorCrossingMsgpackLengthBoundaryRoundTrips() {
        final Vector v = large(ElementType.FLOAT64, 9000);
        assertTrue(v.getWireSize() > 0xffff, "test vector must exceed the 16-bit msgpack length boundary");

        final Key topKey = key("vecbig_top");
        session.upsert(topKey).bin(binName).setTo(v).execute();
        final Vector topGot = readVector(topKey, binName);
        assertEquals(9000, topGot.dimensions);
        assertArrayEquals(v.getElementBytes(), topGot.getElementBytes());
        assertEquals(v, topGot);
    }

    /**
     * A record with no vector bin must not surface the vector bin as an empty vector. Reading an
     * absent bin returns null rather than a zero-dimension {@link Vector}.
     */
    @Test
    public void absentVectorBinIsNotMaterializedAsEmptyVector() {
        final Key key = key("vecabsent");
        session.upsert(key).bin("other").setTo(42L).execute();

        final Record rec = readRecord(key);
        assertNotNull(rec, "record should exist");
        assertEquals(42L, rec.getLong("other"));
        assertNull(rec.getValue(binName), "absent vector bin should be null, not an empty vector");
        assertNull(rec.getVector(binName), "absent vector bin should not materialize as an empty vector");
    }

    // ------------------------------------------------------------------
    // Vectors in lists/maps are rejected (spec R71, R77)
    // ------------------------------------------------------------------

    /**
     * A vector can't be an element of a list or a map, at any depth. The server rejects such a
     * write with a parameter error and "list/map cannot hold a vector" (R71); vectors in lists and
     * maps are not in Phase 1 (R77).
     */
    @ParameterizedTest
    @EnumSource(ElementType.class)
    public void vectorInListOrMapIsRejected(final ElementType t) {
        final Vector v = sample(t);

        // Whole list/map bins.
        assertNestedVectorRejected("list bin",
            () -> session.upsert(key("vecnest_list_" + t)).bin("l").setTo(List.of(v)).execute());
        assertNestedVectorRejected("map bin",
            () -> session.upsert(key("vecnest_map_" + t)).bin("m").setTo(Map.of("k", v)).execute());

        // Mixed with scalars.
        final List<Object> mixed = new ArrayList<>();
        mixed.add(42L);
        mixed.add("hello");
        mixed.add(v);
        assertNestedVectorRejected("mixed list bin",
            () -> session.upsert(key("vecnest_mixed_" + t)).bin("l").setTo(mixed).execute());

        // Deeper nesting.
        assertNestedVectorRejected("list in list",
            () -> session.upsert(key("vecnest_ll_" + t)).bin("l").setTo(List.of(List.of(v))).execute());
        assertNestedVectorRejected("map in list",
            () -> session.upsert(key("vecnest_ml_" + t)).bin("l").setTo(List.of(Map.of("k", v))).execute());
        assertNestedVectorRejected("list in map",
            () -> session.upsert(key("vecnest_lm_" + t)).bin("m").setTo(Map.of("k", List.of(v))).execute());

        // CDT operations.
        assertNestedVectorRejected("cdt listAppend",
            () -> session.upsert(key("vecnest_cdtlist_" + t)).bin("l").listAppend(v).execute());
        assertNestedVectorRejected("cdt map setTo",
            () -> session.upsert(key("vecnest_cdtmap_" + t)).bin("m").onMapKey("k").setTo(v).execute());
    }

    private static void assertNestedVectorRejected(final String what, final Runnable write) {
        final AerospikeException e = assertThrows(AerospikeException.class, write::run, what);
        assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode(), what);
        assertTrue(e.getMessage().contains("list/map cannot hold a vector"),
            what + ": unexpected message: " + e.getMessage());
    }

    // ------------------------------------------------------------------
    // Read-path parity
    // ------------------------------------------------------------------

    @Test
    public void readPathParityAcrossApis() {
        final Vector v = Vector.ofFloat64(new double[] {1.1, 2.2, 3.3});

        // Single key written and read four ways.
        final Key key = key("vecreadpath");
        session.upsert(key).bin(binName).setTo(v).execute();

        // (a) query all bins
        assertEquals(v, session.query(key).execute().getFirstRecord().getVector(binName));

        // (b) reading only the vector bin
        assertEquals(v, session.query(key).readingOnlyBins(binName)
            .execute().getFirstRecord().getVector(binName));

        // (c) operate get
        final Record opRec = session.upsert(key).bin(binName).get().execute().next().recordOrThrow();
        assertEquals(v, opRec.operationResult(0).getVector());

        // (d) multi-key batch read
        final List<Key> keys = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final Key k = key("vecreadpath_batch" + i);
            session.upsert(k).bin(binName).setTo(v).execute();
            keys.add(k);
        }

        final RecordStream rs = session.query(keys).execute();
        int count = 0;
        while (rs.hasNext()) {
            final Record rec = rs.next().recordOrThrow();
            assertEquals(v, rec.getVector(binName));
            count++;
        }
        assertEquals(keys.size(), count);
    }

    @Disabled("Generic VECTOR expression reads are not supported")
    @Test
    public void expressionReadOfVectorBinMustNotCrashServer() {
        final Key key = key("vecexpread");
        session.upsert(key).bin(binName).setTo(Vector.ofFloat32(new float[] {0.5f, -1.5f, 2.0f})).execute();

        // A plain expression read of a vector bin.
        final Record rec = session.query(key)
            .bin("out").selectFrom(Exp.vectorBin(binName))
            .execute()
            .getFirstRecord();

        assertNotNull(rec.getValue("out"),
            "expression read of a vector bin should return the projected bin");
    }

    @Disabled("Generic VECTOR expression filters are not supported")
    @Test
    public void filterExpressionOverVectorBinMustNotCrashServer() {
        final Key key = key("vecexpfilter");
        final Vector v = Vector.ofFloat32(new float[] {0.5f, -1.5f, 2.0f});
        session.upsert(key).bin(binName).setTo(v).execute();

        final Record rec = session.query(key)
            .where(Exp.binExists(binName))
            .execute()
            .getFirstRecord();

        assertEquals(v, rec.getVector(binName));
    }

    // ------------------------------------------------------------------
    // Overwrite / type transitions
    // ------------------------------------------------------------------

    @Test
    public void overwriteWithDifferentDimensions() {
        final Key key = key("vecoverwrite");
        session.upsert(key).bin(binName).setTo(Vector.ofFloat32(new float[] {1.0f, 2.0f})).execute();

        final Vector second = Vector.ofFloat32(new float[] {9.0f, 8.0f, 7.0f, 6.0f});
        session.upsert(key).bin(binName).setTo(second).execute();

        assertEquals(second, readVector(key, binName));
    }

    @Test
    public void overwriteVectorWithScalarAndBack() {
        final Key key = key("vectransition");

        final Vector v1 = Vector.ofInt32(new int[] {1, 2, 3});
        session.upsert(key).bin(binName).setTo(v1).execute();
        assertEquals(v1, readVector(key, binName));

        // Overwrite the vector bin with a scalar.
        session.upsert(key).bin(binName).setTo(999L).execute();
        assertEquals(999L, readRecord(key).getLong(binName));

        // Overwrite the scalar bin with a different vector type.
        final Vector v2 = Vector.ofFloat64(new double[] {1.5, 2.5});
        session.upsert(key).bin(binName).setTo(v2).execute();
        assertEquals(v2, readVector(key, binName));
    }

    @Test
    public void sameValuesDifferentElementTypesAreDistinct() {
        final Key key = key("vecelemtype");

        session.upsert(key).bin(binName).setTo(Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f})).execute();
        final Vector asFloat32 = readVector(key, binName);
        assertSame(ElementType.FLOAT32, asFloat32.elementType);

        session.upsert(key).bin(binName).setTo(Vector.ofFloat64(new double[] {1.0, 2.0, 3.0})).execute();
        final Vector asFloat64 = readVector(key, binName);
        assertSame(ElementType.FLOAT64, asFloat64.elementType);

        // Numerically similar but different element type: must not be equal.
        assertNotEquals(asFloat32, asFloat64);
    }

    // ------------------------------------------------------------------
    // operate + batch
    // ------------------------------------------------------------------

    @Test
    public void operateVectorRoundTrip() {
        final Key key = key("vecoperate");
        final Vector v = Vector.ofFloat64(new double[] {1.1, 2.2, 3.3});

        final RecordStream rs = session.upsert(key)
            .bin(binName).setTo(v)
            .bin(binName).get()
            .execute();

        final Record record = rs.next().recordOrThrow();
        assertEquals(v, record.operationResult(1).getVector());
    }

    @Test
    public void batchWriteReadVectors() {
        final int count = 5;
        final List<Key> keys = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            final Key k = key("vecbatch" + i);
            // Vary the element type across the batch.
            final ElementType t = ElementType.values()[i % ElementType.values().length];
            session.upsert(k).bin(binName).setTo(indexed(t, i)).execute();
            keys.add(k);
        }

        for (int i = 0; i < count; i++) {
            final ElementType t = ElementType.values()[i % ElementType.values().length];
            final Vector got = readVector(keys.get(i), binName);
            assertEquals(indexed(t, i), got);
            assertSame(t, got.elementType);
        }
    }

    private static Vector indexed(final ElementType t, final int i) {
        switch (t) {
            case FLOAT16: return Vector.ofFloat16(new short[] {(short)i, (short)(i + 1)});
            case INT32:   return Vector.ofInt32(new int[] {i, i + 1, i + 2});
            case FLOAT32: return Vector.ofFloat32(new float[] {i, i + 0.5f, i + 1.0f});
            case FLOAT64: return Vector.ofFloat64(new double[] {i, i + 0.5, i + 1.0});
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private Key key(final String id) {
        final Key k = args.set.id(id);
        session.delete(k).execute();
        return k;
    }

    private Record readRecord(final Key key) {
        return session.query(key).execute().getFirstRecord();
    }

    private Vector readVector(final Key key, final String bin) {
        final Record rec = readRecord(key);
        assertNotNull(rec, "record should exist");
        return rec.getVector(bin);
    }
}
