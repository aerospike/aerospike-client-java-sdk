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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.util.Packer;

/**
 * Pure unit tests for {@link CTX} msgpack serialization and deserialization (no server).
 */
class CtxSerdeTest {

    @Test
    void andFilterRoundTripsAsId0x204WithExpression() {
        Expression filter = Exp.build(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)));
        CTX[] original = new CTX[] {
            CTX.mapKeysIn("a", "b", "c"),
            CTX.andFilter(filter),
        };

        CTX[] restored = CTX.fromBytes(CTX.toBytes(original));

        assertThat(restored).hasSize(2);
        assertThat(restored[0].id).isEqualTo(0x2a);
        assertThat(restored[1].id).isEqualTo(0x204);
        assertNull(restored[1].value);
        assertNotNull(restored[1].exp);
        assertArrayEquals(filter.getBytes(), restored[1].exp.getBytes());
    }

    @Test
    void allChildrenWithFilterRoundTripsAsExpressionContext() {
        Expression filter = Exp.build(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0)));
        CTX[] original = new CTX[] {CTX.allChildrenWithFilter(filter)};

        CTX[] restored = CTX.fromBytes(CTX.toBytes(original));

        assertThat(restored).hasSize(1);
        assertThat(restored[0].id).isEqualTo(Exp.CTX_EXP);
        assertNotNull(restored[0].exp);
    }

    @Test
    void toBase64RoundTripPreservesAndFilterChain() {
        Expression compiled = Exp.build(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)));
        CTX[] original = new CTX[] {
            CTX.mapKeysIn("a", "b", "c"),
            CTX.andFilter(compiled),
        };

        CTX[] restored = CTX.fromBase64(CTX.toBase64(original));

        assertThat(restored).hasSize(2);
        assertThat(restored[0].id).isEqualTo(0x2a);
        assertThat(restored[1].id).isEqualTo(0x204);
        assertNotNull(restored[1].exp);
        assertArrayEquals(compiled.getBytes(), restored[1].exp.getBytes());
    }

    @Test
    void toStringFormatsValueAndExpressionContexts() {
        Expression compiled = Exp.build(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)));

        assertThat(CTX.mapKeysIn("a", "b").toString())
            .contains("CTX{id=0x2a")
            .contains("value=[a, b]");

        assertThat(CTX.andFilter(compiled).toString())
            .contains("CTX{id=0x204")
            .contains("exp=" + compiled.getBase64());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mapKeysInRoundTripCases")
    void mapKeysInPrimitiveKeyListsRoundTrip(CTX original, List<?> expectedKeys) {
        CTX[] restored = CTX.fromBytes(CTX.toBytes(new CTX[] {original}));
        assertThat(restored).hasSize(1);
        assertThat(restored[0].id).isEqualTo(original.id);
        assertThat(mapKeysInKeyList(restored[0])).isEqualTo(expectedKeys);
    }

    static Stream<Arguments> mapKeysInRoundTripCases() {
        return Stream.of(
            Arguments.of(CTX.mapKeysIn((byte) 1, (byte) 2), List.of(1L, 2L)),
            Arguments.of(CTX.mapKeysIn((short) 3, (short) 4), List.of(3L, 4L)),
            Arguments.of(CTX.mapKeysIn(5, 6), List.of(5L, 6L)),
            Arguments.of(CTX.mapKeysIn(7L, 8L), List.of(7L, 8L)),
            Arguments.of(CTX.mapKeysIn(1.5, 2.5), List.of(1.5, 2.5)),
            Arguments.of(CTX.mapKeysIn(3.5f, 4.5f), List.of(3.5d, 4.5d))
        );
    }

    @Test
    void fromBytesEmptyPayloadReturnsEmptyContextArray() {
        CTX[] restored = CTX.fromBytes(new byte[0]);
        assertThat(restored).isEmpty();
    }

    @Test
    void fromBytesRejectsTruncatedContextPayload() {
        Packer packer = new Packer();
        for (int pass = 0; pass < 2; pass++) {
            packer.packArrayBegin(1);
            packer.packInt(0x20);
            if (pass == 0) {
                packer.createBuffer();
            }
        }

        assertThrows(AerospikeException.Parse.class, () -> CTX.fromBytes(packer.getBuffer()));
    }

    @Test
    void fromBytesRejectsNonIntegerContextId() {
        byte[] malformed = packContextIdAndValue("bad-id", 1);

        AerospikeException.Parse ae = assertThrows(AerospikeException.Parse.class,
            () -> CTX.fromBytes(malformed));
        assertTrue(ae.getMessage().contains("Context id must be an integer"), ae.getMessage());
    }

    private static byte[] packContextIdAndValue(String id, int value) {
        Packer packer = new Packer();
        for (int pass = 0; pass < 2; pass++) {
            packer.packArrayBegin(2);
            packer.packString(id);
            packer.packInt(value);
            if (pass == 0) {
                packer.createBuffer();
            }
        }
        return packer.getBuffer();
    }

    @SuppressWarnings("unchecked")
    private static List<Object> mapKeysInKeyList(CTX ctx) {
        assertNotNull(ctx.value);
        return (List<Object>) ctx.value.getObject();
    }
}
