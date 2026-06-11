/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.
 */
package com.aerospike.client.sdk.exp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Demo: same logical filter as Rust {@code eq(string_bin("a"), string_val("b".repeat(31)))}.
 * <p>
 * String literal uses {@link com.aerospike.client.sdk.util.Packer#packParticleString} →
 * msgpack string length = UTF-8 bytes + 1 (STRING particle). For 31 ASCII {@code b}, length
 * is 32 → Java uses MessagePack <strong>str8</strong> ({@code 0xd9 0x20}) before {@code 0x03}
 * and the payload. Rust {@code aerospike-core} {@code pack_string} / {@code pack_string_begin}
 * uses <strong>str16</strong> ({@code 0xda 0x00 0x20}) for that same total length, so wire bytes
 * differ (see sibling Rust test {@code pack_string_header_demo.rs}).
 */
class PackStringHeaderParticleMismatchDemoTest {

    private static String hexSpace(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(String.format("%02x", data[i]));
        }
        return sb.toString();
    }

    @Test
    void eq_stringBin_stringVal_len31_prints_msgpack_hex_java_uses_str8() {
        String literal = "b".repeat(31);
        assertThat(literal.length()).isEqualTo(31);

        Expression e = Exp.build(Exp.eq(Exp.stringBin("a"), Exp.val(literal)));
        byte[] b = e.getBytes();

        System.out.println();
        System.out.println("Java: Exp.build(Exp.eq(Exp.stringBin(\"a\"), Exp.val(\"b\"*31))) MessagePack ("
            + b.length + " bytes, hex):");
        System.out.println(hexSpace(b));
        System.out.println();

        // str8 (0xd9) + length 32 (0x20) + STRING particle (0x03) + 31×'b'
        assertThat(b).containsSequence((byte) 0xd9, (byte) 0x20, (byte) 0x03);
        //assertThat(b).doesNotContainSequence((byte) 0xda, (byte) 0x00, (byte) 0x20, (byte) 0x03);
    }
}
