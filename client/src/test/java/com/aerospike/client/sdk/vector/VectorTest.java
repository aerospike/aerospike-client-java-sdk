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
package com.aerospike.client.sdk.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Bin;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.Value.VectorValue;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.util.Packer;
import com.aerospike.client.sdk.util.Unpacker;
import com.aerospike.client.sdk.vector.Vector.ElementType;

class VectorTest {
    //-------------------------------------------------------
    // ElementType
    //-------------------------------------------------------

    @Test
    void elementTypeCodes() {
        assertEquals(0x01, ElementType.FLOAT16.getCode());
        assertEquals(0x02, ElementType.INT32.getCode());
        assertEquals(0x03, ElementType.FLOAT32.getCode());
        assertEquals(0x04, ElementType.FLOAT64.getCode());
    }

    @Test
    void elementTypeByteSizes() {
        assertEquals(2, ElementType.FLOAT16.getByteSize());
        assertEquals(4, ElementType.INT32.getByteSize());
        assertEquals(4, ElementType.FLOAT32.getByteSize());
        assertEquals(8, ElementType.FLOAT64.getByteSize());
    }

    @Test
    void elementTypeFromCode() {
        for (final ElementType type : ElementType.values()) {
            assertSame(type, ElementType.fromCode(type.getCode()));
        }
    }

    @Test
    void elementTypeFromInvalidCode() {
        assertThrows(IllegalArgumentException.class, () -> ElementType.fromCode((byte)0x7f));
    }

    //-------------------------------------------------------
    // Vector construction and accessors
    //-------------------------------------------------------

    @Test
    void constructFloat16() {
        final short[] data = new short[] {0x3c00, (short)0xbc00, 0x4000};
        final Vector v = Vector.ofFloat16(data);

        assertEquals(Vector.VERSION, v.version);
        assertSame(ElementType.FLOAT16, v.elementType);
        assertEquals(3, v.dimensions);
        assertArrayEquals(data, v.getFloat16Data());
    }

    @Test
    void constructInt32() {
        final int[] data = new int[] {-1, 0, 1, Integer.MAX_VALUE};
        final Vector v = Vector.ofInt32(data);

        assertEquals(Vector.VERSION, v.version);
        assertSame(ElementType.INT32, v.elementType);
        assertEquals(4, v.dimensions);
        assertArrayEquals(data, v.getInt32Data());
    }

    @Test
    void constructFloat32() {
        final float[] data = new float[] {1.5f, -2.25f, 0.0f, 3.14159f, Float.MAX_VALUE};
        final Vector v = Vector.ofFloat32(data);

        assertEquals(Vector.VERSION, v.version);
        assertSame(ElementType.FLOAT32, v.elementType);
        assertEquals(5, v.dimensions);
        assertArrayEquals(data, v.getFloat32Data(), 0.0f);
    }

    @Test
    void constructFloat64() {
        final double[] data = new double[] {1.5, -2.25, Double.MAX_VALUE};
        final Vector v = Vector.ofFloat64(data);

        assertEquals(Vector.VERSION, v.version);
        assertSame(ElementType.FLOAT64, v.elementType);
        assertEquals(3, v.dimensions);
        assertArrayEquals(data, v.getFloat64Data(), 0.0);
    }

    //-------------------------------------------------------
    // Immutability (defensive copies)
    //-------------------------------------------------------

    @Test
    void constructorCopiesInput() {
        final float[] data = new float[] {1.0f, 2.0f, 3.0f};
        final Vector v = Vector.ofFloat32(data);

        data[0] = 99.0f;

        assertEquals(1.0f, v.getFloat32Data()[0], 0.0f);
    }

    @Test
    void getterReturnsCopy() {
        final Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

        final float[] first = v.getFloat32Data();
        first[0] = 99.0f;

        assertEquals(1.0f, v.getFloat32Data()[0], 0.0f);
    }

    @Test
    void wrongTypeGetterThrows() {
        final Vector v = Vector.ofFloat32(new float[] {1.0f});
        assertThrows(IllegalStateException.class, v::getInt32Data);
    }

    //-------------------------------------------------------
    // Wire size
    //-------------------------------------------------------

    @Test
    void wireSize() {
        assertEquals(8 + 3 * 2, Vector.ofFloat16(new short[3]).getWireSize());
        assertEquals(8 + 4 * 4, Vector.ofInt32(new int[4]).getWireSize());
        assertEquals(8 + 5 * 4, Vector.ofFloat32(new float[5]).getWireSize());
        assertEquals(8 + 2 * 8, Vector.ofFloat64(new double[2]).getWireSize());
    }

    //-------------------------------------------------------
    // equals / hashCode / toString
    //-------------------------------------------------------

    @Test
    void equalsAndHashCode() {
        final Vector a = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
        final Vector b = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void notEqualsDifferentData() {
        final Vector a = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
        final Vector b = Vector.ofFloat32(new float[] {1.0f, 2.0f, 4.0f});

        assertNotEquals(a, b);
    }

    @Test
    void notEqualsDifferentType() {
        final Vector a = Vector.ofInt32(new int[] {1, 2, 3});
        final Vector b = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

        assertNotEquals(a, b);
    }

    @Test
    void toStringContainsData() {
        final Vector v = Vector.ofInt32(new int[] {1, 2, 3});
        assertEquals("[1, 2, 3]", v.toString());
    }

    //-------------------------------------------------------
    // writeTo (wire format)
    //-------------------------------------------------------

    @Test
    void writeToFloat32() {
        final float[] data = new float[] {1.5f, -2.25f, 3.14159f};
        final Vector v = Vector.ofFloat32(data);

        final byte[] buffer = new byte[v.getWireSize()];
        final int written = v.writeTo(buffer, 0);

        assertEquals(v.getWireSize(), written);
        assertHeader(buffer, ElementType.FLOAT32, data.length);

        for (int i = 0; i < data.length; i++) {
            final int bits = decodeIntLE(buffer, 8 + i * 4);
            assertEquals(data[i], Float.intBitsToFloat(bits), 0.0f);
        }
    }

    @Test
    void writeToFloat64() {
        final double[] data = new double[] {1.5, -2.25, 3.14159};
        final Vector v = Vector.ofFloat64(data);

        final byte[] buffer = new byte[v.getWireSize()];
        final int written = v.writeTo(buffer, 0);

        assertEquals(v.getWireSize(), written);
        assertHeader(buffer, ElementType.FLOAT64, data.length);

        for (int i = 0; i < data.length; i++) {
            final long bits = decodeLongLE(buffer, 8 + i * 8);
            assertEquals(data[i], Double.longBitsToDouble(bits), 0.0);
        }
    }

    @Test
    void writeToInt32() {
        final int[] data = new int[] {-1, 0, 1, 12345};
        final Vector v = Vector.ofInt32(data);

        final byte[] buffer = new byte[v.getWireSize()];
        final int written = v.writeTo(buffer, 0);

        assertEquals(v.getWireSize(), written);
        assertHeader(buffer, ElementType.INT32, data.length);

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], decodeIntLE(buffer, 8 + i * 4));
        }
    }

    @Test
    void writeToFloat16() {
        final short[] data = new short[] {0x3c00, (short)0xbc00, 0x4000};
        final Vector v = Vector.ofFloat16(data);

        final byte[] buffer = new byte[v.getWireSize()];
        final int written = v.writeTo(buffer, 0);

        assertEquals(v.getWireSize(), written);
        assertHeader(buffer, ElementType.FLOAT16, data.length);

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], decodeShortLE(buffer, 8 + i * 2));
        }
    }

    @Test
    void writeToRespectsOffset() {
        final Vector v = Vector.ofInt32(new int[] {7, 8});

        final byte[] buffer = new byte[4 + v.getWireSize()];
        final int written = v.writeTo(buffer, 4);

        assertEquals(v.getWireSize(), written);
        // Leading bytes untouched.
        assertEquals(0, buffer[0]);
        assertEquals(0, buffer[1]);
        assertEquals(0, buffer[2]);
        assertEquals(0, buffer[3]);
        assertEquals(Vector.VERSION, buffer[4]);
    }

    //-------------------------------------------------------
    // from (deserialization)
    //-------------------------------------------------------

    @Test
    void fromRoundTripsFloat16() {
        assertRoundTrips(Vector.ofFloat16(new short[] {0x3c00, (short)0xbc00, 0x4000}));
    }

    @Test
    void fromRoundTripsInt32() {
        assertRoundTrips(Vector.ofInt32(new int[] {-1, 0, 1, Integer.MAX_VALUE}));
    }

    @Test
    void fromRoundTripsFloat32() {
        assertRoundTrips(Vector.ofFloat32(new float[] {1.5f, -2.25f, 0.0f, 3.14159f, Float.MAX_VALUE}));
    }

    @Test
    void fromRoundTripsFloat64() {
        assertRoundTrips(Vector.ofFloat64(new double[] {1.5, -2.25, Double.MAX_VALUE}));
    }

    @Test
    void fromRespectsOffset() {
        final Vector v = Vector.ofInt32(new int[] {7, 8, 9});

        final byte[] buffer = new byte[4 + v.getWireSize()];
        v.writeTo(buffer, 4);

        final Vector parsed = Vector.from(buffer, 4, buffer.length - 4);
        assertEquals(v, parsed);
    }

    @Test
    void fromIgnoresTrailingBytes() {
        final Vector v = Vector.ofInt32(new int[] {1, 2, 3});

        final byte[] buffer = new byte[v.getWireSize() + 10];
        v.writeTo(buffer, 0);

        final Vector parsed = Vector.from(buffer, 0, buffer.length);
        assertEquals(v, parsed);
    }

    @Test
    void fromTooShortThrows() {
        final byte[] buffer = new byte[Vector.HEADER_SIZE - 1];
        assertThrows(IllegalArgumentException.class, () -> Vector.from(buffer, 0, buffer.length));
    }

    @Test
    void fromTruncatedDataThrows() {
        final Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});

        final byte[] buffer = new byte[v.getWireSize()];
        v.writeTo(buffer, 0);

        // Claim there's one fewer byte than the data actually requires.
        assertThrows(IllegalArgumentException.class, () -> Vector.from(buffer, 0, buffer.length - 1));
    }

    @Test
    void fromInvalidElementTypeThrows() {
        final byte[] buffer = new byte[Vector.HEADER_SIZE];
        buffer[0] = Vector.VERSION;
        buffer[1] = (byte)0x7f; // invalid element type code

        assertThrows(IllegalArgumentException.class, () -> Vector.from(buffer, 0, buffer.length));
    }

    //-------------------------------------------------------
    // Buffer.bytesToParticle (record deserialization)
    //-------------------------------------------------------

    @Test
    void bytesToParticleDeserializesVector() {
        final Vector v = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});

        final byte[] buffer = new byte[v.getWireSize()];
        v.writeTo(buffer, 0);

        final Object parsed = Buffer.bytesToParticle(ParticleType.VECTOR, buffer, 0, buffer.length);

        assertEquals(Vector.class, parsed.getClass());
        assertEquals(v, parsed);
    }

    //-------------------------------------------------------
    // Unpacker (vector nested in list/map)
    //-------------------------------------------------------

    @Test
    void unpackerDeserializesNestedVector() {
        final Vector v = Vector.ofInt32(new int[] {1, 2, 3});
        final List<Value> list = List.of(Value.get(v));

        // Two-pass pack: size estimate then write.
        final Packer packer = new Packer();
        packer.packValueList(list);
        packer.createBuffer();
        packer.packValueList(list);
        final byte[] packed = packer.getBuffer();

        final Object unpacked = Unpacker.unpackObjectList(packed, 0, packed.length);

        final List<?> unpackedList = (List<?>)unpacked;
        assertEquals(1, unpackedList.size());
        assertEquals(Vector.class, unpackedList.get(0).getClass());
        assertEquals(v, unpackedList.get(0));
    }

    @Test
    void unpackerRoundTripsRawVectorInList() {
        // A list built from raw (unwrapped) Vector elements, as opposed to
        // Value.get(Vector) elements, must still pack and unpack correctly.
        // This exercises Packer.packObject()/packList() -> Packer.packVector().
        final Vector v = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
        final List<Object> list = List.of(v);

        final byte[] packed = Packer.pack(list);
        final Object unpacked = Unpacker.unpackObjectList(packed, 0, packed.length);

        final List<?> unpackedList = (List<?>)unpacked;
        assertEquals(1, unpackedList.size());
        assertEquals(Vector.class, unpackedList.get(0).getClass());
        assertEquals(v, unpackedList.get(0));
    }

    @Test
    void binWithRawVectorInListPacksSuccessfully() {
        // Regression: Bin(String, List<?>) packs raw list elements via
        // Packer.packObject(), which must have a branch for Vector.
        final Vector v = Vector.ofInt32(new int[] {1, 2, 3});
        final Bin bin = new Bin("veclist", List.of(v));

        final byte[] buffer = new byte[bin.value.estimateSize()];
        final int written = bin.value.write(buffer, 0);

        assertEquals(buffer.length, written);
    }

    //-------------------------------------------------------
    // VectorValue
    //-------------------------------------------------------

    @Test
    void getVectorValue() {
        final Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f});
        final Value value = Value.get(v);

        assertEquals(ParticleType.VECTOR, value.getType());
        assertSame(v, value.getObject());
        assertSame(v, ((VectorValue)value).getVector());
    }

    @Test
    void getVectorNull() {
        assertSame(Value.getAsNull(), Value.get((Vector)null));
    }

    @Test
    void getObjectWrapsNativeVector() {
        // Deserialization yields a native Vector; Value.get(Object) must re-wrap
        // it as a VectorValue so a read-then-write round trip preserves the type.
        final Vector v = Vector.ofInt32(new int[] {1, 2, 3});
        final Value value = Value.get((Object)v);

        assertEquals(VectorValue.class, value.getClass());
        assertEquals(ParticleType.VECTOR, value.getType());
        assertSame(v, ((VectorValue)value).getVector());
    }

    @Test
    void valueEstimateSizeMatchesWireSize() {
        final Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
        final Value value = Value.get(v);

        assertEquals(v.getWireSize(), value.estimateSize());
    }

    @Test
    void valueWriteMatchesVectorWriteTo() {
        final Vector v = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
        final Value value = Value.get(v);

        final byte[] expected = new byte[v.getWireSize()];
        v.writeTo(expected, 0);

        final byte[] actual = new byte[value.estimateSize()];
        final int written = value.write(actual, 0);

        assertEquals(expected.length, written);
        assertArrayEquals(expected, actual);
    }

    @Test
    void valueEquals() {
        final Value a = Value.get(Vector.ofInt32(new int[] {1, 2, 3}));
        final Value b = Value.get(Vector.ofInt32(new int[] {1, 2, 3}));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void validateKeyTypeThrows() {
        final Value value = Value.get(Vector.ofInt32(new int[] {1, 2, 3}));

        final AerospikeException e = assertThrows(AerospikeException.class, value::validateKeyType);
        assertEquals(ResultCode.PARAMETER_ERROR, e.getResultCode());
    }

    @Test
    void packProducesParticleBytes() {
        final Vector v = Vector.ofInt32(new int[] {1, 2, 3});
        final Value value = Value.get(v);

        // Two-pass pack: size estimate then write.
        final Packer packer = new Packer();
        value.pack(packer);
        packer.createBuffer();
        value.pack(packer);
        final byte[] packed = packer.getBuffer();

        // Packed blob = msgpack byte-array header + particle type byte + wire bytes.
        final byte[] wire = new byte[v.getWireSize()];
        v.writeTo(wire, 0);

        final int payloadStart = packed.length - wire.length;
        assertEquals(ParticleType.VECTOR, packed[payloadStart - 1] & 0xff);

        final byte[] payload = new byte[wire.length];
        System.arraycopy(packed, payloadStart, payload, 0, wire.length);
        assertArrayEquals(wire, payload);
    }

    //-------------------------------------------------------
    // Bin
    //-------------------------------------------------------

    @Test
    void binConstructorWrapsVector() {
        final Vector v = Vector.ofFloat32(new float[] {1.0f, 2.0f, 3.0f});
        final Bin bin = new Bin("vecbin", v);

        assertEquals("vecbin", bin.name);
        assertEquals(VectorValue.class, bin.value.getClass());
        assertEquals(ParticleType.VECTOR, bin.value.getType());
        assertSame(v, ((VectorValue)bin.value).getVector());
    }

    @Test
    void binEqualsUsesVectorEquality() {
        final Bin a = new Bin("vecbin", Vector.ofInt32(new int[] {1, 2, 3}));
        final Bin b = new Bin("vecbin", Vector.ofInt32(new int[] {1, 2, 3}));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    //-------------------------------------------------------
    // Record
    //-------------------------------------------------------

    @Test
    void recordGetVectorReturnsVector() {
        final Vector v = Vector.ofFloat64(new double[] {1.1, 2.2, 3.3});
        final Map<String,Object> bins = new HashMap<>();
        bins.put("vecbin", v);
        final Record record = new Record(bins, null, 0, 0);

        assertSame(v, record.getVector("vecbin"));
    }

    //-------------------------------------------------------
    // End-to-end round trip through pack + unpack of a bin value
    //-------------------------------------------------------

    @Test
    void vectorBinValueRoundTripsThroughParticle() {
        final Vector v = Vector.ofFloat32(new float[] {1.5f, -2.25f, 3.0f});
        final Value value = Value.get(v);

        final byte[] wire = new byte[value.estimateSize()];
        value.write(wire, 0);

        final Object parsed = Buffer.bytesToParticle(ParticleType.VECTOR, wire, 0, wire.length);
        assertTrue(parsed instanceof Vector);
        assertEquals(v, parsed);
    }

    //-------------------------------------------------------
    // Helpers
    //-------------------------------------------------------

    private static void assertRoundTrips(final Vector v) {
        final byte[] buffer = new byte[v.getWireSize()];
        v.writeTo(buffer, 0);
        assertEquals(v, Vector.from(buffer, 0, buffer.length));
    }

    private static void assertHeader(final byte[] buffer, final ElementType type, final int dimensions) {
        assertEquals(Vector.VERSION, buffer[0]);
        assertEquals(type.getCode(), buffer[1]);
        assertEquals(dimensions, decodeIntLE(buffer, 2));
        assertEquals(0, buffer[6]);
        assertEquals(0, buffer[7]);
    }

    // Vector wire format is little-endian to match the server.
    private static int decodeIntLE(final byte[] b, final int offset) {
        return (b[offset] & 0xff) |
            ((b[offset + 1] & 0xff) << 8) |
            ((b[offset + 2] & 0xff) << 16) |
            ((b[offset + 3] & 0xff) << 24);
    }

    private static long decodeLongLE(final byte[] b, final int offset) {
        long result = 0;
        for (int i = 7; i >= 0; i--) {
            result = (result << 8) | (b[offset + i] & 0xff);
        }
        return result;
    }

    private static short decodeShortLE(final byte[] b, final int offset) {
        return (short)((b[offset] & 0xff) | ((b[offset + 1] & 0xff) << 8));
    }
}
