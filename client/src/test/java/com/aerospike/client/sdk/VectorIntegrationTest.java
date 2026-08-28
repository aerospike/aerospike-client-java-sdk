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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.Vector.ElementType;

/**
 * Server round-trip tests for the vector particle type (wire type 16), covering the element-type
 * matrix, value edge cases (special floats, boundary integers), dimension boundaries, vectors nested
 * in list/map bins and written through the typed CDT operation overloads, CDT read-back, read-path
 * parity, bin-type reflection, and type transitions.
 * <p>
 * Requires a server build that supports the vector particle type.
 */
public class VectorIntegrationTest extends ClusterTest {
    private static final String binName = "vecbin";

    // ------------------------------------------------------------------
    // Sample builders
    // ------------------------------------------------------------------

    /** A representative "normal" vector of the given element type (dims = 4). */
    private static Vector sample(final ElementType t) {
        switch (t) {
            case FLOAT16: return Vector.ofFloat16(new short[] {0x3c00, (short)0xbc00, 0x4000, 0x0000});
            case INT32:   return Vector.ofInt32(new int[] {-5, 0, 7, 12345});
            case FLOAT32: return Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.14159f, 0.0f});
            case FLOAT64: return Vector.ofFloat64(new double[] {1.5, -2.25, 3.14159, 0.0});
            default: throw new IllegalArgumentException(t.toString());
        }
    }

    /** A vector exercising special/boundary values for the given element type. */
    private static Vector special(final ElementType t) {
        switch (t) {
            case FLOAT16:
                // 0, -0, 1.0, -1.0, +Inf, -Inf, NaN, smallest subnormal.
                return Vector.ofFloat16(new short[] {
                    0x0000, (short)0x8000, 0x3c00, (short)0xbc00,
                    0x7c00, (short)0xfc00, 0x7e00, 0x0001});
            case INT32:
                return Vector.ofInt32(new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE});
            case FLOAT32:
                return Vector.ofFloat32(new float[] {
                    0.0f, -0.0f, 1.5f, -2.25f, Float.MIN_VALUE, Float.MAX_VALUE,
                    Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY});
            case FLOAT64:
                return Vector.ofFloat64(new double[] {
                    0.0, -0.0, 1.5, -2.25, Double.MIN_VALUE, Double.MAX_VALUE,
                    Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY});
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
        // Bit-exact comparison catches NaN payloads and negative zero.
        assertArrayEquals(v.getElementBytes(), got.getElementBytes());
        assertEquals(v, got);
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
     * Round-trips a 9000-element fp64 vector (72,008 bytes), crossing the 16-bit msgpack length
     * boundary. Covers both a top-level bin and a list element.
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

        final Key listKey = key("vecbig_list");
        session.upsert(listKey).bin("l").setTo(List.of(v)).execute();
        final List<?> list = readRecord(listKey).getList("l");
        assertEquals(1, list.size());
        assertEquals(v, list.get(0));
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
    // element-type x container matrix
    // ------------------------------------------------------------------

    @Test
    public void elementTypeTimesContainerMatrix() {
        for (final ElementType t : ElementType.values()) {
            final Vector v = sample(t);

            // 1. Stored directly as a list bin element.
            final Key listKey = key("vecmx_list_" + t);
            session.upsert(listKey).bin("l").setTo(List.of(v)).execute();
            final List<?> list = readRecord(listKey).getList("l");
            assertEquals(1, list.size(), "list bin size for " + t);
            assertEquals(v, list.get(0), "list bin element for " + t);

            // 2. Stored directly as a map bin value.
            final Key mapKey = key("vecmx_map_" + t);
            session.upsert(mapKey).bin("m").setTo(Map.of("k", v)).execute();
            final Map<?, ?> map = readRecord(mapKey).getMap("m");
            assertEquals(v, map.get("k"), "map bin value for " + t);

            // 3. Appended via the typed CDT list overload.
            final Key cdtListKey = key("vecmx_cdtlist_" + t);
            session.upsert(cdtListKey).bin("l").listAppend(v).execute();
            assertEquals(v, readRecord(cdtListKey).getList("l").get(0),
                "cdt listAppend element for " + t);

            // 4. Set via the typed CDT map-setter overload.
            final Key cdtMapKey = key("vecmx_cdtmap_" + t);
            session.upsert(cdtMapKey).bin("m").onMapKey("k").setTo(v).execute();
            assertEquals(v, readRecord(cdtMapKey).getMap("m").get("k"),
                "cdt map setTo value for " + t);
        }
    }

    @Test
    public void mixedTypeListPreservesVectorsAndScalars() {
        final Key key = key("vecmixedlist");
        final Vector vf = Vector.ofFloat32(new float[] {1.0f, 2.0f});
        final Vector vi = Vector.ofInt32(new int[] {3, 4, 5});

        final List<Object> input = new ArrayList<>();
        input.add(42L);
        input.add("hello");
        input.add(vf);
        input.add(vi);

        session.upsert(key).bin("l").setTo(input).execute();

        final List<?> got = readRecord(key).getList("l");
        assertEquals(4, got.size());
        assertEquals(42L, got.get(0));
        assertEquals("hello", got.get(1));
        assertEquals(vf, got.get(2));
        assertEquals(vi, got.get(3));
    }

    @Test
    public void allElementTypesInOneList() {
        final Key key = key("vecalltypeslist");
        final List<Object> input = new ArrayList<>();
        for (final ElementType t : ElementType.values()) {
            input.add(sample(t));
        }

        session.upsert(key).bin("l").setTo(input).execute();

        final List<?> got = readRecord(key).getList("l");
        assertEquals(ElementType.values().length, got.size());

        int i = 0;
        for (final ElementType t : ElementType.values()) {
            final Object element = got.get(i++);
            assertEquals(sample(t), element, "element " + t);
            assertSame(t, ((Vector)element).elementType);
        }
    }

    // ------------------------------------------------------------------
    // CDT read-back via server operations
    // ------------------------------------------------------------------

    @Test
    public void cdtReadBackViaListAndMapOps() {
        final Vector va = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
        final Vector vb = Vector.ofInt32(new int[] {4, 5, 6});

        // List: read a single element back via listGet.
        final Key listKey = key("veccdtread_list");
        session.upsert(listKey).bin("l").setTo(List.of(va, vb)).execute();

        final RecordStream rs = session.upsert(listKey)
            .bin("l").listGet(1)
            .execute();
        final Record rec = rs.next().recordOrThrow();
        assertEquals(vb, rec.operationResult(0).getVector());

        // Map: read a single value back via a CDT map-key projection.
        final Key mapKey = key("veccdtread_map");
        session.upsert(mapKey).bin("m").setTo(Map.of("k1", va, "k2", vb)).execute();

        final Record mapRec = session.query(mapKey)
            .bin("m").onMapKey("k2").getValues()
            .execute()
            .getFirstRecord();
        assertEquals(vb, mapRec.getVector("m"));
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

    // ------------------------------------------------------------------
    // Server bug: reading a vector bin through the expression engine crashes the node.
    //
    // Root cause is server-side, in rt_bin_translate() (aerospike-server
    // as/src/exp/exp_rt.c, ~line 3725): its particle-type switch has no
    // AS_PARTICLE_TYPE_VECTOR arm and falls through to `cf_crash(AS_EXP, "unexpected")`
    // (exp_rt.c:3748). Every expression that loads a vector bin routes through
    // rt_load_bin -> rt_bin_translate, so a filter, binExists, binType, or a plain
    // read expression over a vector bin all abort asd. Note that Exp.vectorBin()
    // requests the bin as BLOB, yet it still crashes, because rt_bin_translate
    // switches on the bin's *stored* particle type (VECTOR), not the requested type.
    //
    // Treat VECTOR as BLOB in rt_bin_translate(), as rt_value_translate() already does.
    //
    // The repros remain disabled until the server handles VECTOR on the expression read path.
    // ------------------------------------------------------------------

    @Disabled("Server bug: an expression read of a vector bin crashes the node (exp_rt.c rt_bin_translate cf_crash on AS_PARTICLE_TYPE_VECTOR). See section comment.")
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

    @Disabled("Server bug: a filter expression over a vector bin crashes the node (exp_rt.c rt_bin_translate cf_crash on AS_PARTICLE_TYPE_VECTOR). See section comment.")
    @Test
    public void filterExpressionOverVectorBinMustNotCrashServer() {
        final Key key = key("vecexpfilter");
        final Vector v = Vector.ofFloat32(new float[] {0.5f, -1.5f, 2.0f});
        session.upsert(key).bin(binName).setTo(v).execute();

        // binExists evaluates through rt_load_bin -> rt_bin_translate.
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
