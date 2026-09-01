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
package com.aerospike.client.sdk.query.plan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class QueryWhereWireTest {

    private static final String SIMPLE_AEL = "$.age > 30";
    private static final String COMPOUND_AEL = "$.age > 30 and $.country == 'US'";

    @Test
    void forExplainEncodesExplainFlagAndAel() {
        byte[] payload = QueryWhereWire.forExplain(SIMPLE_AEL);

        assertEquals(QueryWhereWire.FLAG_EXPLAIN, QueryWhereWire.flags(payload));
        assertEquals(SIMPLE_AEL, QueryWhereWire.ael(payload));
        assertArrayEquals(expectedPayload(QueryWhereWire.FLAG_EXPLAIN, SIMPLE_AEL), payload);
    }

    @Test
    void forExplainWithFlagsRequireIndex() {
        int flags = QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX;
        byte[] payload = QueryWhereWire.forExplain(flags, SIMPLE_AEL);

        assertEquals(flags, QueryWhereWire.flags(payload));
    }

    @Test
    void forExplainWithFlagsHardHint() {
        int flags = QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT;
        byte[] payload = QueryWhereWire.forExplain(flags, SIMPLE_AEL);

        assertEquals(flags, QueryWhereWire.flags(payload));
    }

    @Test
    void forExplainWithFlagsRequireIndexAndHardHint() {
        int flags = QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX
            | QueryWhereWire.FLAG_HARD_HINT;
        byte[] payload = QueryWhereWire.forExplain(flags, SIMPLE_AEL);

        assertEquals(flags, QueryWhereWire.flags(payload));
    }

    @Test
    void forExplainWithFlagsRequiresExplainBit() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryWhereWire.forExplain(QueryWhereWire.FLAG_REQUIRE_INDEX, SIMPLE_AEL));
    }

    @Test
    void forExecuteClearsExplainFlag() {
        byte[] payload = QueryWhereWire.forExecute(SIMPLE_AEL);

        assertEquals(0, QueryWhereWire.flags(payload));
        assertEquals(SIMPLE_AEL, QueryWhereWire.ael(payload));
        assertArrayEquals(expectedPayload(0, SIMPLE_AEL), payload);
    }

    @Test
    void encodeSupportsCompoundAel() {
        byte[] payload = QueryWhereWire.forExplain(COMPOUND_AEL);

        assertEquals(COMPOUND_AEL, QueryWhereWire.ael(payload));
    }

    @Test
    void clearExplainClearsAllExplainOnlyFlags() {
        byte[] explain = QueryWhereWire.forExplain(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
            COMPOUND_AEL
        );
        byte[] execute = QueryWhereWire.clearExplain(explain);

        assertEquals(0, QueryWhereWire.flags(execute));
        assertEquals(COMPOUND_AEL, QueryWhereWire.ael(execute));
        assertArrayEquals(QueryWhereWire.forExecute(COMPOUND_AEL), execute);
    }

    @Test
    void clearExplainOnDefaultExplainPayload() {
        byte[] explain = QueryWhereWire.forExplain(SIMPLE_AEL);
        byte[] execute = QueryWhereWire.clearExplain(explain);

        assertArrayEquals(QueryWhereWire.forExecute(SIMPLE_AEL), execute);
    }

    @Test
    void clearExplainIdempotentOnExecuteShape() {
        byte[] execute = QueryWhereWire.forExecute(SIMPLE_AEL);
        assertArrayEquals(execute, QueryWhereWire.clearExplain(execute));
    }

    @Test
    void requireAelRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.requireAel(null));
    }

    @Test
    void requireAelRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.requireAel(""));
    }

    @Test
    void requireAelRejectsBlank() {
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.requireAel("   "));
    }

    @Test
    void rejectsUnknownFlags() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryWhereWire.encode(1 << 4, SIMPLE_AEL));
    }

    @Test
    void rejectsContinuationBitInSemanticFlags() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryWhereWire.encode(QueryWhereWire.FLAG_ENC_VARINT, SIMPLE_AEL));
    }

    @Test
    void decodeSingleBytePrefixMatchesV1() {
        byte[] payload = expectedPayload(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
            SIMPLE_AEL
        );

        assertEquals(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
            QueryWhereWire.flags(payload)
        );
        assertEquals(SIMPLE_AEL, QueryWhereWire.ael(payload));
    }

    /**
     * The i-th prefix byte carries flag positions [1 + 7i .. 7 + 7i], so a payload bit set
     * in the second byte is flag position 8 — not a re-OR of the first byte's positions.
     * Mirrors server {@code where_parse_flags} in {@code query_where.c}.
     */
    @Test
    void decodeMultiBytePrefixShiftsPayloadIntoFlagPositions() {
        byte[] prefix = {
            (byte) (QueryWhereWire.FLAG_ENC_VARINT | QueryWhereWire.FLAG_EXPLAIN),
            (byte) (1 << 1)
        };
        byte[] payload = multiBytePrefixPayload(prefix, SIMPLE_AEL);

        assertEquals(
            QueryWhereWire.FLAG_EXPLAIN | (1 << 8),
            QueryWhereWire.flags(payload)
        );
        assertEquals(SIMPLE_AEL, QueryWhereWire.ael(payload));
    }

    /**
     * A single-byte prefix decodes to itself, so client and server agree by construction
     * on every flag combination the current set can express.
     */
    @Test
    void singleBytePrefixDecodesToItself() {
        for (int flags = 0; flags <= QueryWhereWire.FLAG_KNOWN; flags++) {
            if ((flags & QueryWhereWire.FLAG_ENC_VARINT) != 0) {
                continue;
            }

            byte[] payload = QueryWhereWire.encode(flags, SIMPLE_AEL);

            assertEquals(1, payload.length - SIMPLE_AEL.length(), "flags 0x" + Integer.toHexString(flags));
            assertEquals(flags, payload[0] & 0xFF, "flags 0x" + Integer.toHexString(flags));
            assertEquals(flags, QueryWhereWire.flags(payload));
        }
    }

    @Test
    void clearExplainCollapsesMultiBytePrefixToSingleByte() {
        byte[] prefix = {
            (byte) (QueryWhereWire.FLAG_ENC_VARINT | QueryWhereWire.FLAG_EXPLAIN
                | QueryWhereWire.FLAG_REQUIRE_INDEX),
            0
        };
        byte[] explain = multiBytePrefixPayload(prefix, COMPOUND_AEL);
        byte[] execute = QueryWhereWire.clearExplain(explain);

        assertArrayEquals(QueryWhereWire.forExecute(COMPOUND_AEL), execute);
    }

    @Test
    void clearExplainPreservesFlagsBeyondTheFirstPrefixByte() {
        byte[] prefix = {
            (byte) (QueryWhereWire.FLAG_ENC_VARINT | QueryWhereWire.FLAG_EXPLAIN),
            (byte) (1 << 1)
        };
        byte[] explain = multiBytePrefixPayload(prefix, COMPOUND_AEL);
        byte[] execute = QueryWhereWire.clearExplain(explain);

        assertEquals(1 << 8, QueryWhereWire.flags(execute));
        assertEquals(COMPOUND_AEL, QueryWhereWire.ael(execute));
    }

    @Test
    void rejectsTruncatedMultiBytePrefix() {
        byte[] payload = new byte[] {
            (byte) (QueryWhereWire.FLAG_ENC_VARINT | QueryWhereWire.FLAG_EXPLAIN)
        };

        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.flags(payload));
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.ael(payload));
    }

    /** Server caps the prefix at AS_QUERY_WHERE_FLAGS_MAX_BYTES (9). */
    @Test
    void rejectsOverlongFlagPrefix() {
        byte[] prefix = new byte[10];
        Arrays.fill(prefix, (byte) QueryWhereWire.FLAG_ENC_VARINT);

        byte[] payload = multiBytePrefixPayload(prefix, SIMPLE_AEL);

        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.flags(payload));
    }

    /** Flag positions above 31 cannot round-trip through the int flag domain. */
    @Test
    void rejectsFlagPositionBeyondIntRange() {
        byte[] prefix = new byte[6];
        Arrays.fill(prefix, (byte) QueryWhereWire.FLAG_ENC_VARINT);
        prefix[5] = (byte) (1 << 1);

        byte[] payload = multiBytePrefixPayload(prefix, SIMPLE_AEL);

        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.flags(payload));
    }

    private static byte[] expectedPayload(int flags, String ael) {
        byte[] aelBytes = ael.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + aelBytes.length];
        payload[0] = (byte) flags;
        System.arraycopy(aelBytes, 0, payload, 1, aelBytes.length);
        return payload;
    }

    private static byte[] multiBytePrefixPayload(byte[] prefix, String ael) {
        byte[] aelBytes = ael.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[prefix.length + aelBytes.length];
        System.arraycopy(prefix, 0, payload, 0, prefix.length);
        System.arraycopy(aelBytes, 0, payload, prefix.length, aelBytes.length);
        return payload;
    }
}
