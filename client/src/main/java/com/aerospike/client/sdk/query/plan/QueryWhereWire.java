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

import java.nio.charset.StandardCharsets;

/**
 * Encodes field {@code 44} ({@code WHERE}) payloads for server query explain/execute.
 *
 * <p>Wire shape: {@code [flags: u8][AEL source UTF-8...]}. Flags match server
 * {@code AS_QUERY_WHERE_FLAG_*} in {@code query_where.h}.</p>
 */
public final class QueryWhereWire {

    /**
     * Bit 0 encoding selector — {@code 0} = single-byte flags (v1); {@code 1} = varInt (future).
     * Must remain clear until varInt encoding is supported.
     */
    public static final int FLAG_ENC_VARINT = 1 << 0;

    /** Explain phase — server runs index planner only. */
    public static final int FLAG_EXPLAIN = 1 << 1;

    /** Optional: reject PI fallback on explain when set with {@link #FLAG_EXPLAIN}. */
    public static final int FLAG_REQUIRE_INDEX = 1 << 2;

    /** Explain-only: require field {@code 21} index name hint; fail if hint missing or not selected. */
    public static final int FLAG_HARD_HINT = 1 << 3;

    public static final int FLAG_KNOWN =
        FLAG_ENC_VARINT | FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT;

    private QueryWhereWire() {
    }

    /**
     * Field {@code 44} body for phase 1 (explain).
     */
    public static byte[] forExplain(String ael) {
        return encode(FLAG_EXPLAIN, ael);
    }

    /**
     * Field {@code 44} body for phase 2 (execute) — same AEL, EXPLAIN cleared.
     */
    public static byte[] forExecute(String ael) {
        return encode(0, ael);
    }

    /**
     * Encodes a WHERE field value: {@code [flags][AEL UTF-8]}.
     *
     * @param flags bit mask of {@link #FLAG_EXPLAIN}, {@link #FLAG_REQUIRE_INDEX}, etc.
     * @param ael   raw AEL source text (not packed predexp, not msgpack {@code [128,"…"]})
     */
    public static byte[] encode(int flags, String ael) {
        validateFlags(flags);
        byte[] aelBytes = aelBytes(ael);
        byte[] payload = new byte[1 + aelBytes.length];
        payload[0] = (byte) flags;
        System.arraycopy(aelBytes, 0, payload, 1, aelBytes.length);
        return payload;
    }

    /**
     * Rebuilds execute payload from an explain payload (clears {@link #FLAG_EXPLAIN}).
     */
    public static byte[] clearExplain(byte[] explainPayload) {
        if (explainPayload == null || explainPayload.length < 2) {
            throw new IllegalArgumentException("explain WHERE payload must include flags and AEL");
        }
        int flags = explainPayload[0] & 0xFF;
        validateFlags(flags);
        if ((flags & FLAG_EXPLAIN) == 0) {
            throw new IllegalArgumentException("explain WHERE payload must have EXPLAIN flag set");
        }
        int executeFlags = flags & ~FLAG_EXPLAIN;
        byte[] aelBytes = new byte[explainPayload.length - 1];
        System.arraycopy(explainPayload, 1, aelBytes, 0, aelBytes.length);
        byte[] payload = new byte[explainPayload.length];
        payload[0] = (byte) executeFlags;
        System.arraycopy(aelBytes, 0, payload, 1, aelBytes.length);
        return payload;
    }

    public static int flags(byte[] payload) {
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("WHERE payload must not be null or empty");
        }
        return payload[0] & 0xFF;
    }

    public static String ael(byte[] payload) {
        if (payload == null || payload.length < 2) {
            throw new IllegalArgumentException("WHERE payload must include flags and AEL");
        }
        return new String(payload, 1, payload.length - 1, StandardCharsets.UTF_8);
    }

    private static void validateFlags(int flags) {
        if ((flags & ~FLAG_KNOWN) != 0) {
            throw new IllegalArgumentException("unknown WHERE flags 0x" + Integer.toHexString(flags));
        }
        if ((flags & FLAG_ENC_VARINT) != 0) {
            throw new IllegalArgumentException("varInt WHERE flags encoding is not supported");
        }
    }

    private static byte[] aelBytes(String ael) {
        if (ael == null) {
            throw new IllegalArgumentException("WHERE AEL must not be null");
        }
        byte[] bytes = ael.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0) {
            throw new IllegalArgumentException("WHERE AEL must not be empty");
        }
        return bytes;
    }
}
