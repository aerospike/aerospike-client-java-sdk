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

import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.ParticleType;

/**
 * Transforms probe {@code INDEX_RANGE} (field {@code 22}) bytes for execute when field
 * {@code 21} ({@code INDEX_NAME}) is also sent.
 *
 * <p>Probe responses may include a bin name in the range body; execute rejects
 * {@code INDEX_NAME} together with a bin name in {@code INDEX_RANGE}. Execute therefore
 * sends {@code bin_name_len = 0} and only the ktype/range tail.</p>
 */
public final class IndexRangeWire {

    private static final int GEO_REGION_DESCRIBE_MAX = 64;

    private IndexRangeWire() {
    }

    /**
     * Converts explain field-{@code 22} bytes to the execute shape used with field {@code 21}.
     *
     * <p>Internal to the server-led two-phase query path ({@code QueryCommand.forPlan} only).
     * Structural validation of explain {@code INDEX_RANGE} is in
     * {@link QueryPlan#fromExplainResponse}.</p>
     *
     * @param probeRangeBytes opaque {@code INDEX_RANGE} body from explain field {@code 22}
     * @return execute field-{@code 22} body ({@code bin_name_len = 0} when bin name was stripped)
     */
    public static byte[] forExecuteWithIndexName(byte[] probeRangeBytes) {
        int binNameLen = probeRangeBytes[1] & 0xFF;
        if (binNameLen == 0) {
            return probeRangeBytes;
        }

        int offset = 2 + binNameLen;
        int tailLen = probeRangeBytes.length - offset;
        byte[] execute = new byte[2 + tailLen];
        execute[0] = 1;
        execute[1] = 0;
        System.arraycopy(probeRangeBytes, offset, execute, 2, tailLen);
        return execute;
    }

    /**
     * Human-readable explain {@code INDEX_RANGE} for debug logs
     *
     * @param probeRangeBytes opaque explain field-{@code 22} body, or {@code null}
     * @return decoded range summary, or {@code null} when {@code probeRangeBytes} is {@code null}
     */
    public static String describeProbeRange(byte[] probeRangeBytes) {
        if (probeRangeBytes == null) {
            return null;
        }
        if (probeRangeBytes.length < 2) {
            return "invalid(truncated)";
        }

        int binNameLen = probeRangeBytes[1] & 0xFF;
        int offset = 2;
        if (probeRangeBytes.length < offset + binNameLen + 1) {
            return "invalid(truncated)";
        }

        String binName = binNameLen > 0
            ? new String(probeRangeBytes, offset, binNameLen, StandardCharsets.UTF_8)
            : "";
        offset += binNameLen;
        int ktype = probeRangeBytes[offset++] & 0xFF;

        return switch (ktype) {
            case ParticleType.INTEGER -> {
                BoundLong begin = readIntegerBound(probeRangeBytes, offset);
                if (begin == null) {
                    yield "invalid(integer-begin)";
                }
                BoundLong end = readIntegerBound(probeRangeBytes, begin.nextOffset);
                if (end == null) {
                    yield "invalid(integer-end)";
                }
                yield "bin=" + binName + " range=[" + begin.value + "," + end.value + "]";
            }
            case ParticleType.STRING -> {
                BoundBytes value = readBytesBound(probeRangeBytes, offset);
                if (value == null) {
                    yield "invalid(string)";
                }
                String text = new String(value.bytes, StandardCharsets.UTF_8);
                yield "bin=" + binName + " value=" + text + " len=" + value.bytes.length;
            }
            case ParticleType.BLOB -> {
                BoundBytes value = readBytesBound(probeRangeBytes, offset);
                if (value == null) {
                    yield "invalid(blob)";
                }
                yield "bin=" + binName + " value=x'" + Buffer.bytesToHexString(value.bytes)
                    + "' len=" + value.bytes.length;
            }
            case ParticleType.GEOJSON -> {
                BoundBytes value = readBytesBound(probeRangeBytes, offset);
                if (value == null) {
                    yield "invalid(geojson)";
                }
                yield "bin=" + binName + " region=" + abbreviateRegion(value.bytes)
                    + " len=" + value.bytes.length;
            }
            default -> "bin=" + binName + " ktype=" + ktype + " hex="
                + Buffer.bytesToHexString(probeRangeBytes);
        };
    }

    /**
     * Region text is abbreviated because a geo bound may be up to 1 MiB, unlike STRING/BLOB
     * bounds, which the server caps at 2048.
     */
    private static String abbreviateRegion(byte[] region) {
        String text = new String(region, StandardCharsets.UTF_8);
        if (text.length() <= GEO_REGION_DESCRIBE_MAX) {
            return text;
        }
        return text.substring(0, GEO_REGION_DESCRIBE_MAX) + "...";
    }

    private static BoundLong readIntegerBound(byte[] bytes, int offset) {
        BoundBytes raw = readBytesBound(bytes, offset);
        if (raw == null) {
            return null;
        }
        if (raw.bytes.length != 8) {
            return null;
        }
        return new BoundLong(Buffer.bytesToLong(raw.bytes, 0), raw.nextOffset);
    }

    private static BoundBytes readBytesBound(byte[] bytes, int offset) {
        if (bytes.length < offset + 4) {
            return null;
        }
        int len = Buffer.bytesToInt(bytes, offset);
        offset += 4;
        if (len < 0 || bytes.length < offset + len) {
            return null;
        }
        byte[] value = new byte[len];
        System.arraycopy(bytes, offset, value, 0, len);
        return new BoundBytes(value, offset + len);
    }

    private record BoundLong(long value, int nextOffset) {}

    private record BoundBytes(byte[] bytes, int nextOffset) {}
}
