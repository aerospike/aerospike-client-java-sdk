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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;

/**
 * Validates MessagePack layout for server-side DSL compile wrapper
 */
class ServerCompiledFilterWireTest {

    @Test
    void fromServerCompiledFilter_encodes_two_element_root_array_with_opcode_128_then_utf8() {
        Expression e = Expression.fromServerCompiledFilter("$.bin==1");
        byte[] b = e.getBytes();
        assertThat(b.length).isGreaterThan(4);
        assertThat(b[0]).isEqualTo((byte) 0x92);
        assertThat(b[1]).isEqualTo((byte) 0xcc);
        assertThat(b[2]).isEqualTo((byte) Expression.SERVER_COMPILED_DSL_EXPRESSION_OP);
        assertThat(b[3]).isEqualTo((byte) (0xa0 + "$.bin==1".length()));
    }

    @Test
    void fromServerCompiledFilter_nullThrows() {
        assertThrows(AerospikeException.class, () -> Expression.fromServerCompiledFilter(null));
    }

    @Test
    void fromServerCompiledFilter_emptyString_stillProducesTwoElementRoot() {
        Expression e = Expression.fromServerCompiledFilter("");
        byte[] b = e.getBytes();
        assertThat(b[0]).isEqualTo((byte) 0x92);
        assertThat(b[1]).isEqualTo((byte) 0xcc);
        assertThat(b[2]).isEqualTo((byte) Expression.SERVER_COMPILED_DSL_EXPRESSION_OP);
        assertThat(b[3]).isEqualTo((byte) 0xa0);
    }

    @Test
    void fromServerCompiledFilter_utf8_payloadCopiedVerbatim_afterStringHeader() {
        String src = "$.café=='é'";
        byte[] utf8 = src.getBytes(StandardCharsets.UTF_8);
        Expression e = Expression.fromServerCompiledFilter(src);
        byte[] payload = e.getBytes();
        assertThat(Byte.toUnsignedInt(payload[3])).isEqualTo(0xa0 + utf8.length);
        assertThat(Arrays.copyOfRange(payload, 4, 4 + utf8.length)).isEqualTo(utf8);
    }

    @Test
    void fromServerCompiledFilter_asciiLength32UsesStr8NotFixstr() {
        String padded = "a".repeat(32);
        byte[] b = Expression.fromServerCompiledFilter(padded).getBytes();
        assertThat(b[0]).isEqualTo((byte) 0x92);
        assertThat(b[1]).isEqualTo((byte) 0xcc);
        assertThat(b[2]).isEqualTo((byte) Expression.SERVER_COMPILED_DSL_EXPRESSION_OP);
        assertThat(Byte.toUnsignedInt(b[3])).isEqualTo(0xd9);
        assertThat(Byte.toUnsignedInt(b[4])).isEqualTo(32);
        assertThat(Arrays.copyOfRange(b, 5, 37)).isEqualTo(padded.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void expr_embed_roundTripMatchesDirectWirePayload() {
        Expression direct = Expression.fromServerCompiledFilter("$.rank in (1,2,3)");
        Expression rebuilt = Exp.build(Exp.expr(direct));
        assertThat(rebuilt.getBytes()).isEqualTo(direct.getBytes());
    }
}
