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

/**
 * Transforms probe {@code INDEX_RANGE} (field {@code 22}) bytes for execute when field
 * {@code 21} ({@code INDEX_NAME}) is also sent.
 *
 * <p>Probe responses may include a bin name in the range body; execute rejects
 * {@code INDEX_NAME} together with a bin name in {@code INDEX_RANGE}. Execute therefore
 * sends {@code bin_name_len = 0} and only the ktype/range tail.</p>
 */
public final class IndexRangeWire {

    private IndexRangeWire() {
    }

    /**
     * Converts explain field-{@code 22} bytes to the execute shape used with field {@code 21}.
     *
     * <p>Internal to the server-led two-phase query path ({@code QueryCommand.forPlan} only).
     * Non-empty {@code INDEX_RANGE} on SI explain is validated in
     * {@link QueryPlan#fromExplainResponse}.</p>
     *
     * @param probeRangeBytes opaque {@code INDEX_RANGE} body from explain field {@code 22}
     * @return execute field-{@code 22} body ({@code bin_name_len = 0} when bin name was stripped)
     */
    public static byte[] forExecuteWithIndexName(byte[] probeRangeBytes) {
        int offset = 0;
        int nRanges = probeRangeBytes[offset++] & 0xFF;
        if (nRanges != 1) {
            throw new IllegalArgumentException(
                "INDEX_RANGE probe body must have n_ranges=1, found " + nRanges);
        }
        if (offset >= probeRangeBytes.length) {
            throw new IllegalArgumentException("INDEX_RANGE probe body truncated after n_ranges");
        }

        int binNameLen = probeRangeBytes[offset++] & 0xFF;
        if (binNameLen == 0) {
            return probeRangeBytes;
        }
        if (offset + binNameLen > probeRangeBytes.length) {
            throw new IllegalArgumentException("INDEX_RANGE probe body truncated in bin name");
        }

        offset += binNameLen;
        int tailLen = probeRangeBytes.length - offset;
        if (tailLen <= 0) {
            throw new IllegalArgumentException("INDEX_RANGE probe body missing ktype/range tail");
        }

        byte[] execute = new byte[2 + tailLen];
        execute[0] = 1;
        execute[1] = 0;
        System.arraycopy(probeRangeBytes, offset, execute, 2, tailLen);
        return execute;
    }
}
