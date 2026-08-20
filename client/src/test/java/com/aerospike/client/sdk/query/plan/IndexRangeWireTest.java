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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.query.Filter;

class IndexRangeWireTest {

    @Test
    void describeIntegerRange() {
        byte[] probe = probeRangeWithBinName("age", 101L, Long.MAX_VALUE);
        assertEquals("bin=age range=[101,9223372036854775807]", IndexRangeWire.describeProbeRange(probe));
    }

    @Test
    void describeIntegerEquality() {
        byte[] probe = probeRangeWithBinName("age", 30L);
        assertEquals("bin=age range=[30,30]", IndexRangeWire.describeProbeRange(probe));
    }

    @Test
    void describeStringEquality() {
        byte[] probe = probeRangeWithBinName("ka", "k1");
        assertEquals("bin=ka value=k1 len=2", IndexRangeWire.describeProbeRange(probe));
    }

    @Test
    void describeGeoRegion() {
        String region = "{\"type\":\"Point\",\"coordinates\":[-122.0986857,37.4214209]}";
        byte[] probe = geoProbeRangeWithBinName("loc", region);

        assertEquals("bin=loc region=" + region + " len=" + region.length(),
            IndexRangeWire.describeProbeRange(probe));
    }

    @Test
    void describeNullRange() {
        assertNull(IndexRangeWire.describeProbeRange(null));
    }

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

    private static byte[] probeRangeWithBinName(String binName, long value) {
        Filter structured = Filter.equal(binName, value);
        byte[] wireBody = new byte[1 + structured.estimateSize()];
        wireBody[0] = 1;
        structured.write(wireBody, 1);
        return wireBody;
    }

    private static byte[] probeRangeWithBinName(String binName, long begin, long end) {
        Filter structured = Filter.range(binName, begin, end);
        byte[] wireBody = new byte[1 + structured.estimateSize()];
        wireBody[0] = 1;
        structured.write(wireBody, 1);
        return wireBody;
    }

    private static byte[] probeRangeWithBinName(String binName, String value) {
        Filter structured = Filter.equal(binName, value);
        byte[] wireBody = new byte[1 + structured.estimateSize()];
        wireBody[0] = 1;
        structured.write(wireBody, 1);
        return wireBody;
    }

    private static byte[] geoProbeRangeWithBinName(String binName, String region) {
        Filter structured = Filter.geoWithinRegion(binName, region);
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
