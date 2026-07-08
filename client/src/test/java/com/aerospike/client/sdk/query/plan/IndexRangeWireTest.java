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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.query.Filter;

class IndexRangeWireTest {

    @Test
    void stripsBinNameForExecuteWithIndexName() {
        byte[] probe = probeRangeWithBinName("age", 30L);
        byte[] execute = IndexRangeWire.forExecuteWithIndexName(probe);

        assertArrayEquals(new byte[] {1, 0}, slice(execute, 0, 2));
        assertArrayEquals(slice(probe, 1 + 1 + 3, probe.length), slice(execute, 2, execute.length));
    }

    @Test
    void noOpWhenBinNameLenAlreadyZero() {
        byte[] probe = new byte[] {1, 0, 3, 0, 0, 0, 0, 4};
        byte[] execute = IndexRangeWire.forExecuteWithIndexName(probe);
        assertSame(probe, execute);
    }

    @Test
    void rejectsMultipleRanges() {
        byte[] probe = new byte[] {2, 3, 'a', 'g', 'e'};
        assertThrows(IllegalArgumentException.class,
            () -> IndexRangeWire.forExecuteWithIndexName(probe));
    }

    @Test
    void rejectsTruncatedBinName() {
        byte[] probe = new byte[] {1, 3, 'a', 'g'};
        assertThrows(IllegalArgumentException.class,
            () -> IndexRangeWire.forExecuteWithIndexName(probe));
    }

    private static byte[] probeRangeWithBinName(String binName, long value) {
        Filter structured = Filter.equal(binName, value);
        byte[] wireBody = new byte[1 + structured.estimateSize()];
        wireBody[0] = 1;
        structured.write(wireBody, 1);
        return wireBody;
    }

    private static byte[] slice(byte[] bytes, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(bytes, from, out, 0, out.length);
        return out;
    }
}
