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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.util.Packer;

/**
 * A {@link ByteBuffer} value is its remaining bytes, from position to limit. The client used to
 * call {@link ByteBuffer#array()}, which returns the whole backing array and throws for direct
 * and read-only buffers.
 */
public class ByteBufferValueTest {
    private static final byte[] EXPECTED = {1, 2};

    /** Buffers whose remaining bytes are {1, 2}, each in a shape that array() gets wrong. */
    static Stream<Arguments> buffers() {
        return Stream.of(
            buffer("wrap with offset and length", () -> ByteBuffer.wrap(new byte[] {9, 9, 1, 2, 9}, 2, 2)),
            buffer("slice with array offset", () -> ByteBuffer.wrap(new byte[] {9, 9, 1, 2, 9}, 2, 2).slice()),
            buffer("read-only", () -> ByteBuffer.wrap(new byte[] {9, 1, 2}, 1, 2).asReadOnlyBuffer()),
            buffer("direct", () -> {
                ByteBuffer bb = ByteBuffer.allocateDirect(4);
                bb.put(new byte[] {9, 1, 2, 9});
                return bb.position(1).limit(3);
            }),
            buffer("whole array", () -> ByteBuffer.wrap(new byte[] {1, 2})));
    }

    private static Arguments buffer(String name, Supplier<ByteBuffer> supplier) {
        return Arguments.of(name, supplier);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("buffers")
    public void valueHoldsRemainingBytes(String name, Supplier<ByteBuffer> buffer) {
        assertArrayEquals(EXPECTED, (byte[])Value.get(buffer.get()).getObject());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("buffers")
    public void objectValueHoldsRemainingBytes(String name, Supplier<ByteBuffer> buffer) {
        // BinBuilder.setTo(Object) takes this path.
        assertArrayEquals(EXPECTED, (byte[])Value.get((Object)buffer.get()).getObject());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("buffers")
    public void keyDigestMatchesByteArrayKey(String name, Supplier<ByteBuffer> buffer) {
        Key fromBuffer = new Key("test", "test", Value.get(buffer.get()));
        Key fromBytes = new Key("test", "test", EXPECTED);

        assertArrayEquals(fromBytes.digest, fromBuffer.digest);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("buffers")
    public void packsAsByteArray(String name, Supplier<ByteBuffer> buffer) {
        assertArrayEquals(Packer.pack(List.of(EXPECTED)), Packer.pack(List.of(buffer.get())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("buffers")
    public void bufferPositionIsUnchanged(String name, Supplier<ByteBuffer> buffer) {
        ByteBuffer bb = buffer.get();
        int position = bb.position();

        Value.get(bb);
        Packer.pack(List.of(bb));

        assertEquals(position, bb.position());
        assertEquals(2, bb.remaining());
    }
}
