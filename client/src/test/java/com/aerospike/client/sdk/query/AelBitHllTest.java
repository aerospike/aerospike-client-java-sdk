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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.HllConfig;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Value;

/**
 * Server-backed AEL integration tests for the BIT builtin family (BLOB receiver) and the
 * HLL builtin family (HLL receiver).
 *
 * <p>Both families were only reachable through the msgpack {@code Exp} builders before;
 * this suite covers their AEL source spelling.
 *
 * <p>Seeded blob is {@code 0x01020304} — bit offsets are counted from the most significant
 * bit of byte 0, so {@code 0x01} has its only set bit at offset 7.
 */
public class AelBitHllTest extends ClusterTest {
    private static final String KEY = "ael_bit_hll";
    private static final String BIN_BLOB = "b";
    private static final String BIN_HLL = "h";
    private static final String BIN_HLL2 = "h2";

    private static final String B = "$." + BIN_BLOB + ":BLOB";
    private static final String H = "$." + BIN_HLL + ":HLL";

    private Key key;

    @BeforeAll
    public static void requireAelServer() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        session.upsert(key)
            .bin(BIN_BLOB).setTo(new byte[] { 0x01, 0x02, 0x03, 0x04 })
            .execute();

        session.upsert(key)
            .bin(BIN_HLL).hllAdd(List.of(Value.get(1), Value.get(2), Value.get(3)), HllConfig.of(8))
            .bin(BIN_HLL2).hllAdd(List.of(Value.get(3), Value.get(4)), HllConfig.of(8))
            .execute();
    }

    // --- bit reads ---

    @Test
    public void bitCountCountsSetBits() {
        // 0x01|0x02|0x03|0x04 -> 1 + 1 + 2 + 1 set bits.
        assertEquals(5L, selectLong(B + ".bitCount(offset: 0, size: 32)"));
    }

    @Test
    public void bitCountHonoursOffsetAndSize() {
        assertEquals(1L, selectLong(B + ".bitCount(offset: 0, size: 8)"));
        assertEquals(2L, selectLong(B + ".bitCount(offset: 16, size: 8)"));
    }

    @Test
    public void bitGetReturnsSelectedBytes() {
        assertBlob("01", B + ".bitGet(offset: 0, size: 8)");
        assertBlob("0102", B + ".bitGet(offset: 0, size: 16)");
    }

    @Test
    public void bitGetIntReturnsInteger() {
        assertEquals(1L, selectLong(B + ".bitGetInt(offset: 0, size: 8)"));
        assertEquals(258L, selectLong(B + ".bitGetInt(offset: 0, size: 16)"));
    }

    @Test
    public void bitLscanFindsFirstSetBitFromLeft() {
        assertEquals(7L, selectLong(B + ".bitLscan(offset: 0, size: 32, value: true)"));
    }

    @Test
    public void bitRscanFindsLastSetBitFromRight() {
        assertEquals(29L, selectLong(B + ".bitRscan(offset: 0, size: 32, value: true)"));
    }

    @Test
    public void bitScanForClearBits() {
        assertEquals(0L, selectLong(B + ".bitLscan(offset: 0, size: 32, value: false)"));
    }

    @Test
    public void b64EncodeRendersBlobAsText() {
        assertEquals("AQIDBA==", selectValue(B + ".b64Encode()"));
    }

    // --- bit modifies (local: produce a new blob, bin untouched) ---

    @Test
    public void bitSetWritesGivenBits() {
        assertBlob("ff020304", B + ".bitSet(offset: 0, size: 8, value: x'FF')");
    }

    @Test
    public void bitSetIntWritesInteger() {
        assertBlob("ff020304", B + ".bitSetInt(offset: 0, size: 8, value: 255)");
    }

    @Test
    public void bitwiseOrAndXorAgainstMask() {
        assertBlob("f1020304", B + ".bitOr(offset: 0, size: 8, value: x'F0')");
        assertBlob("00020304", B + ".bitAnd(offset: 0, size: 8, value: x'F0')");
        assertBlob("fe020304", B + ".bitXor(offset: 0, size: 8, value: x'FF')");
    }

    @Test
    public void bitNotInvertsSelectedBits() {
        assertBlob("fe020304", B + ".bitNot(offset: 0, size: 8)");
    }

    @Test
    public void bitShiftsMoveBitsWithinSelection() {
        assertBlob("02020304", B + ".bitLshift(offset: 0, size: 8, shift: 1)");
        assertBlob("00020304", B + ".bitRshift(offset: 0, size: 8, shift: 1)");
    }

    @Test
    public void bitArithmeticOnSelection() {
        assertBlob("02020304", B + ".bitAdd(offset: 0, size: 8, value: 1)");
        assertBlob("00020304", B + ".bitSubtract(offset: 0, size: 8, value: 1)");
    }

    @Test
    public void bitResizeGrowsWithZeroPadding() {
        assertBlob("0102030400000000", B + ".bitResize(byteSize: 8)");
    }

    @Test
    public void bitInsertAddsBytes() {
        assertBlob("aa01020304", B + ".bitInsert(byteOffset: 0, value: x'AA')");
    }

    @Test
    public void bitRemoveDropsBytes() {
        assertBlob("020304", B + ".bitRemove(byteOffset: 0, byteSize: 1)");
    }

    @Test
    public void bitOpsLeaveSourceBinUnchanged() {
        selectValue(B + ".bitSet(offset: 0, size: 8, value: x'FF')");
        try (RecordStream rs = session.query(key).execute()) {
            Record rec = rs.next().recordOrThrow();
            assertEquals("01020304", HexFormat.of().formatHex((byte[]) rec.getValue(BIN_BLOB)));
        }
    }

    // --- bit results in filter position ---

    @Test
    public void bitCountUsableInWhereClause() {
        assertTrue(matchesWhere(B + ".bitCount(offset: 0, size: 32) == 5"));
        assertTrue(matchesWhere(B + ".bitGetInt(offset: 0, size: 8) == 1"));
    }

    // --- HLL ---

    @Test
    public void hllCountEstimatesCardinality() {
        assertEquals(3L, selectLong(H + ".hllCount()"));
    }

    @Test
    public void hllDescribeReturnsIndexAndMinhashBits() {
        assertEquals(List.of(8L, 0L), selectLongs(H + ".hllDescribe()"));
    }

    @Test
    public void hllMayContainTestsMembership() {
        assertEquals(1L, selectLong(H + ".hllMayContain([1])"));
        assertEquals(0L, selectLong(H + ".hllMayContain([9999])"));
    }

    @Test
    public void hllCountUsableInWhereClause() {
        assertTrue(matchesWhere(H + ".hllCount() == 3"));
        assertTrue(matchesWhere(H + ".hllCount() > 0"));
    }

    // --- helpers ---

    private void assertBlob(String expectedHex, String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(byte[].class, value, () -> "expected blob for AEL: " + ael);
        assertEquals(expectedHex, HexFormat.of().formatHex((byte[]) value),
            () -> "unexpected blob for AEL: " + ael);
    }

    private long selectLong(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Number.class, value, () -> "expected number for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private List<Long> selectLongs(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(List.class, value, () -> "expected list for AEL: " + ael);
        return ((List<?>) value).stream().map(v -> ((Number) v).longValue()).toList();
    }

    private Object selectValue(String ael) {
        try (RecordStream rs = session.query(key)
            .bin("out")
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            Object value = rec.getValue("out");
            assertNotNull(value, () -> "null result for AEL: " + ael);
            return value;
        }
    }

    private boolean matchesWhere(String whereAel) {
        try (RecordStream rs = session.query(key).where(whereAel).execute()) {
            return rs.hasNext();
        }
    }
}
