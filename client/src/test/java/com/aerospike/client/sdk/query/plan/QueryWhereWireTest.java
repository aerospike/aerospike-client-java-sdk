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

import org.junit.jupiter.api.Test;

class QueryWhereWireTest {

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
    void clearExplainRebuildsExecutePayload() {
        byte[] explain = QueryWhereWire.encode(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_REQUIRE_INDEX,
            COMPOUND_AEL
        );
        byte[] execute = QueryWhereWire.clearExplain(explain);

        assertEquals(QueryWhereWire.FLAG_REQUIRE_INDEX, QueryWhereWire.flags(execute));
        assertEquals(COMPOUND_AEL, QueryWhereWire.ael(execute));
    }

    @Test
    void rejectsNullAel() {
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.forExplain(null));
    }

    @Test
    void rejectsEmptyAel() {
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.forExplain(""));
    }

    @Test
    void rejectsUnknownFlags() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryWhereWire.encode(1 << 4, SIMPLE_AEL));
    }

    @Test
    void rejectsVarIntEncodingFlag() {
        assertThrows(IllegalArgumentException.class,
            () -> QueryWhereWire.encode(QueryWhereWire.FLAG_ENC_VARINT, SIMPLE_AEL));
    }

    @Test
    void encodeAcceptsHardHintBit() {
        byte[] payload = QueryWhereWire.encode(
            QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT,
            SIMPLE_AEL
        );
        assertEquals(QueryWhereWire.FLAG_EXPLAIN | QueryWhereWire.FLAG_HARD_HINT,
            QueryWhereWire.flags(payload));
    }

    @Test
    void clearExplainRequiresExplainFlag() {
        byte[] executeShape = QueryWhereWire.forExecute(SIMPLE_AEL);
        assertThrows(IllegalArgumentException.class, () -> QueryWhereWire.clearExplain(executeShape));
    }

    private static byte[] expectedPayload(int flags, String ael) {
        byte[] aelBytes = ael.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + aelBytes.length];
        payload[0] = (byte) flags;
        System.arraycopy(aelBytes, 0, payload, 1, aelBytes.length);
        return payload;
    }
}
