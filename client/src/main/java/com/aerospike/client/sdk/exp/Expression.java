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

import java.io.Serializable;
import java.util.Arrays;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.util.Crypto;
import com.aerospike.client.sdk.util.Packer;

/**
 * Packed expression byte instructions.
 */
public final class Expression implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int SERVER_COMPILED_DSL_EXPRESSION_OP = 128;

    private final byte[] bytes;

    /**
     * Expression constructor used by {@link Exp#build(Exp)}
     */
    Expression(Exp exp) {
        Packer packer = new Packer();
        exp.pack(packer);
        packer.createBuffer();
        exp.pack(packer);
        bytes = packer.getBuffer();
    }

    /**
     * Expression constructor for packed expression instructions.
     */
    Expression(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Return a new expression from packed expression instructions in bytes.
     */
    public static Expression fromBytes(byte[] bytes) {
        return new Expression(bytes);
    }

    /**
     * Return a new expression from packed expression instructions in base64 encoded bytes.
     */
    public static Expression fromBase64(byte[] bytes) {
        return Expression.fromBytes(Crypto.decodeBase64(bytes, 0, bytes.length));
    }

    /**
     * Return a new expression from packed expression instructions in base64 encoded string.
     */
    public static Expression fromBase64(String s) {
        return Expression.fromBase64(s.getBytes());
    }

    /**
     * Build filter-expression bytes for {@linkplain com.aerospike.client.sdk.command.FieldType#FILTER_EXP field 43} when the server
     * should parse/compile textual DSL/AEL. Layout matches the C client ({@code dsl} branch): MessagePack array of length
     * {@code 2} — integer {@code 128} ({@link #SERVER_COMPILED_DSL_EXPRESSION_OP}) and UTF-8 source string.
     *
     * @see com.aerospike.client.sdk.Cluster#supportsServerCompiledFilterExpression()
     *
     * @param dslSourceUtf8 DSL/AEL source (UTF-8)
     */
    public static Expression fromServerCompiledFilter(String dslSourceUtf8) {
        if (dslSourceUtf8 == null) {
            throw new AerospikeException("Server-compiled DSL/AEL source must not be null");
        }
        return new Expression(encodeServerCompiledFilterPayload(dslSourceUtf8));
    }

    private static byte[] encodeServerCompiledFilterPayload(String dslSourceUtf8) {
        try {
            Packer packer = new Packer();
            packPayload(packer, dslSourceUtf8);
            packer.createBuffer();
            packPayload(packer, dslSourceUtf8);
            return packer.getBuffer();
        }
        catch (Throwable t) {
            throw new AerospikeException.Serialize(t);
        }
    }

    private static void packPayload(Packer packer, String dslSourceUtf8) {
        packer.packArrayBegin(2);
        packer.packInt(SERVER_COMPILED_DSL_EXPRESSION_OP);
        packer.packString(dslSourceUtf8);
    }

    /**
     * Return packed byte instructions.
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Return byte instructions in base64 encoding.
     */
    public String getBase64() {
        return Crypto.encodeBase64(bytes);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Expression other = (Expression) obj;
        return Arrays.equals(bytes, other.bytes);
    }
}
