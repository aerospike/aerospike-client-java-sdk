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
import java.util.Arrays;

/**
 * Encodes field {@code 44} ({@code WHERE}) payloads for server query explain/execute.
 *
 * <p>Wire shape: {@code [flag-byte 0][flag-byte 1]…[flag-byte N][AEL source UTF-8…]}.
 * Each flag byte uses varInt-style continuation: bit {@code 0} = more flag bytes follow;
 * bits {@code 1–7} carry flag positions {@code [1 + 7i .. 7 + 7i]} for the {@code i}-th
 * byte. When all semantic flags fit in one byte, bit {@code 0} is clear and the byte
 * decodes to itself (v1-compatible single-byte prefix).</p>
 *
 * <p>Flags match server {@code AS_QUERY_WHERE_FLAG_*} in {@code query_where.h}.</p>
 *
 * <p>Non-blank AEL is validated at the explain probe entry
 * ({@link com.aerospike.client.sdk.query.IndexProbePlanner} /
 * {@link com.aerospike.client.sdk.command.IndexProbeCommand}) and in {@link #encode}.
 * {@link #clearExplain}, {@link #flags}, and {@link #ael} operate on client-authored
 * explain payloads stored on {@link QueryPlan}.</p>
 */
public final class QueryWhereWire {

    /**
     * Bit 0 continuation bit — {@code 1} = more flag bytes follow; {@code 0} = last flag byte.
     *
     * <p>On a single-byte prefix with bit 0 clear, wire layout matches v1 encoding.</p>
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

    /** Explain-only flags cleared when building field {@code 44} for execute. */
    static final int EXPLAIN_ONLY_FLAGS = FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT;

    /** Semantic flag bits carried in bits 1–7 of each prefix byte. */
    private static final int FLAG_SEMANTIC_MASK = 0xFE;

    /** Payload bits per prefix byte (bits 1–7; bit 0 is the continuation bit). */
    private static final int FLAG_PAYLOAD_BITS = 7;

    private static final int FLAG_PAYLOAD_MASK = 0x7F;

    /**
     * Maximum varInt-style flag prefix length (guards malformed payloads). Matches
     * server {@code AS_QUERY_WHERE_FLAGS_MAX_BYTES}: 9 bytes × 7 payload bits fills
     * flag positions 1–63, which is all a {@code uint64_t} decoded value can hold.
     */
    private static final int MAX_FLAG_PREFIX_LEN = 9;

    /**
     * Highest flag position this client can represent, since {@link #flags} is an
     * {@code int}. A prefix setting a position above this decodes to a value that
     * cannot round-trip, so it is rejected rather than silently truncated.
     */
    private static final int MAX_FLAG_POSITION = 31;

    private QueryWhereWire() {
    }

    /**
     * Field {@code 44} body for phase 1 (explain) with default flags ({@link #FLAG_EXPLAIN} only).
     */
    public static byte[] forExplain(String ael) {
        return encode(FLAG_EXPLAIN, ael);
    }

    /**
     * Field {@code 44} body for phase 1 (explain) with the given flag mask (must include
     * {@link #FLAG_EXPLAIN}).
     */
    public static byte[] forExplain(int flags, String ael) {
        if ((flags & FLAG_EXPLAIN) == 0) {
            throw new IllegalArgumentException("explain WHERE flags must include EXPLAIN");
        }
        return encode(flags, ael);
    }

    /**
     * Field {@code 44} body for phase 2 (execute) — same AEL, explain-only flags cleared.
     */
    public static byte[] forExecute(String ael) {
        return encode(0, ael);
    }

    /**
     * Validates AEL before field {@code 44} encoding.
     */
    public static void requireAel(String ael) {
        if (ael == null || ael.isBlank()) {
            throw new IllegalArgumentException("WHERE AEL must not be null or blank");
        }
    }

    /**
     * Encodes a WHERE field value: varInt-style flag prefix + AEL UTF-8.
     *
     * @param flags bit mask of {@link #FLAG_EXPLAIN}, {@link #FLAG_REQUIRE_INDEX}, etc.
     * @param ael   raw AEL source text (not packed predexp, not msgpack {@code [128,"…"]})
     */
    public static byte[] encode(int flags, String ael) {
        requireAel(ael);
        validateFlags(flags);
        byte[] prefix = encodeFlagPrefix(flags);
        byte[] aelBytes = ael.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[prefix.length + aelBytes.length];
        System.arraycopy(prefix, 0, payload, 0, prefix.length);
        System.arraycopy(aelBytes, 0, payload, prefix.length, aelBytes.length);
        return payload;
    }

    /**
     * Rebuilds execute payload from an explain payload (clears explain-only flags).
     *
     * <p>A multi-byte prefix may collapse to a single byte after clearing explain flags.</p>
     */
    public static byte[] clearExplain(byte[] explainPayload) {
        if (explainPayload.length == 0) {
            return explainPayload;
        }

        FlagPrefix parsed = decodeFlagPrefix(explainPayload);
        int executeFlags = parsed.flags & ~EXPLAIN_ONLY_FLAGS;

        if (parsed.aelOffset == 1 && executeFlags == 0) {
            byte[] execute = Arrays.copyOf(explainPayload, explainPayload.length);
            execute[0] = 0;
            return execute;
        }

        byte[] prefix = encodeFlagPrefix(executeFlags);
        int aelLength = explainPayload.length - parsed.aelOffset;
        byte[] payload = new byte[prefix.length + aelLength];
        System.arraycopy(prefix, 0, payload, 0, prefix.length);
        System.arraycopy(explainPayload, parsed.aelOffset, payload, prefix.length, aelLength);
        return payload;
    }

    /**
     * Returns the decoded semantic flags from a WHERE payload.
     */
    public static int flags(byte[] payload) {
        return decodeFlagPrefix(payload).flags;
    }

    /**
     * Returns the AEL source text from a WHERE payload.
     */
    public static String ael(byte[] payload) {
        FlagPrefix parsed = decodeFlagPrefix(payload);
        if (parsed.aelOffset >= payload.length) {
            throw new IllegalArgumentException("missing WHERE AEL");
        }
        return new String(payload, parsed.aelOffset, payload.length - parsed.aelOffset,
            StandardCharsets.UTF_8);
    }

    private static void validateFlags(int flags) {
        if ((flags & ~FLAG_KNOWN) != 0) {
            throw new IllegalArgumentException("unknown WHERE flags 0x" + Integer.toHexString(flags));
        }
        if ((flags & FLAG_ENC_VARINT) != 0) {
            throw new IllegalArgumentException(
                "WHERE flag bit 0 is reserved for wire continuation, not a semantic flag");
        }
    }

    /**
     * Decodes a varInt-style flag prefix and returns semantic flags plus the AEL byte offset.
     *
     * <p>Mirrors server {@code where_parse_flags} in {@code query_where.c}: the {@code i}-th
     * prefix byte carries flag positions {@code [1 + 7i .. 7 + 7i]} in its bits 1–7, so each
     * byte's payload is shifted into place rather than OR'd flat. The two agree by
     * construction on a single-byte prefix, which is the only shape the current flag set
     * needs.</p>
     */
    private static FlagPrefix decodeFlagPrefix(byte[] payload) {
        if (payload.length == 0) {
            throw new IllegalArgumentException("missing WHERE payload");
        }

        int offset = 0;
        long decoded = 0;

        while (true) {
            if (offset >= payload.length) {
                throw new IllegalArgumentException("truncated WHERE flag prefix");
            }
            if (offset >= MAX_FLAG_PREFIX_LEN) {
                throw new IllegalArgumentException("WHERE flag prefix too long");
            }

            int b = payload[offset] & 0xFF;
            long flagPayload = (b & FLAG_SEMANTIC_MASK) >>> 1;

            decoded |= flagPayload << (1 + FLAG_PAYLOAD_BITS * offset);
            offset++;

            if ((b & FLAG_ENC_VARINT) == 0) {
                break;
            }
        }

        if ((decoded >>> (MAX_FLAG_POSITION + 1)) != 0) {
            throw new IllegalArgumentException(
                "WHERE flags 0x" + Long.toHexString(decoded) + " exceed flag position "
                    + MAX_FLAG_POSITION);
        }

        return new FlagPrefix((int) decoded, offset);
    }

    /**
     * Encodes semantic flags into a varInt-style prefix — the exact inverse of
     * {@link #decodeFlagPrefix}.
     *
     * <p>Current flags all sit in positions 1–3, so this emits a single byte with the
     * continuation bit clear, matching the v1 encoding.</p>
     */
    private static byte[] encodeFlagPrefix(int semantic) {
        byte[] prefix = new byte[flagPrefixLength(semantic)];

        for (int i = 0; i < prefix.length; i++) {
            int flagPayload = (semantic >>> (1 + FLAG_PAYLOAD_BITS * i)) & FLAG_PAYLOAD_MASK;
            int cont = (i + 1 < prefix.length) ? FLAG_ENC_VARINT : 0;
            prefix[i] = (byte) ((flagPayload << 1) | cont);
        }

        return prefix;
    }

    /**
     * Number of prefix bytes needed to carry {@code semantic}: the byte holding its
     * highest set flag position, since position {@code p} lives in byte {@code (p - 1) / 7}.
     */
    private static int flagPrefixLength(int semantic) {
        int highestPosition = MAX_FLAG_POSITION - Integer.numberOfLeadingZeros(semantic);

        return (highestPosition < 1) ? 1 : ((highestPosition - 1) / FLAG_PAYLOAD_BITS) + 1;
    }

    private record FlagPrefix(int flags, int aelOffset) {
    }
}
