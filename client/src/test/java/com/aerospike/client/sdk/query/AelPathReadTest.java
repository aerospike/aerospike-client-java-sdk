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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

/**
 * Server-backed AEL integration tests for CDT path reads and {@code .count()}
 */
public class AelPathReadTest extends ClusterTest {
    private static final String KEY = "ael_path_read";
    private static final String BIN_MAP = "m";
    private static final String BIN_LIST = "l";
    private static final String BIN_INT = "num";
    private static final String BIN_STR = "s";
    private static final String BIN_BLOB = "b";

    private Key key;

    @BeforeAll
    public static void requireAelServer() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);
        map.put("gamma", 30);

        session.upsert(key)
            .bin(BIN_MAP).setTo(map)
            .bin(BIN_LIST).setTo(List.of(100, 200, 300, 400, 500))
            .bin(BIN_INT).setTo(42)
            .bin(BIN_STR).setTo("  trim-me  ")
            .bin(BIN_BLOB).setTo(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF})
            .execute();
    }

    // --- implicit terminal read vs named getters ---

    @Test
    public void implicitMapKeyRead() {
        // Single-select implicit get needs a resolved leaf type; bare $.m:MAP.alpha
        // leaves the value AUTO and fails server AEL compile (parameter error).
        assertEquals(10L, selectLong("implicit", "$." + BIN_MAP + ":MAP.alpha:INT"));
    }

    @Test
    public void implicitListIndexRead() {
        assertEquals(200L, selectLong("implicit", "$." + BIN_LIST + ":LIST.[1]:INT"));
    }

    @Test
    public void implicitMultiSelectValuesDifferFromGetKeys() {
        List<?> values = selectList("values", "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}");
        List<?> keys = selectList("keys", "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}.getKeys()");

        assertThat(values)
            .extracting(v -> ((Number) v).longValue())
            .containsExactlyInAnyOrder(10L, 20L, 30L);
        assertEquals(Set.of("alpha", "beta", "gamma"), new HashSet<>(keys));
    }

    @Test
    public void getKeyValuesReturnsFlatKeyValueList() {
        List<?> flat = selectList("kv", "$." + BIN_MAP + ":MAP.{alpha,beta}.getKeyValues()");
        assertEquals(4, flat.size());
        assertEquals("alpha", flat.get(0));
        assertEquals(10L, ((Number) flat.get(1)).longValue());
        assertEquals("beta", flat.get(2));
        assertEquals(20L, ((Number) flat.get(3)).longValue());
    }

    @Test
    public void getTreePreservesMapStructure() {
        Object tree = selectValue("tree", "$." + BIN_MAP + ":MAP.{alpha,beta}.getTree()");
        assertInstanceOf(Map.class, tree);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) tree;
        assertEquals(10L, ((Number) map.get("alpha")).longValue());
        assertEquals(20L, ((Number) map.get("beta")).longValue());
        assertEquals(2, map.size());
    }

    @Test
    public void getTreeOnFullMapKeyList() {
        Object tree = selectValue("tree", "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}.getTree()");
        assertInstanceOf(Map.class, tree);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) tree;
        assertEquals(3, map.size());
        assertEquals(30L, ((Number) map.get("gamma")).longValue());
    }

    // --- .count() on single-select vs multi-select ---

    @Test
    public void countOnWholeMapIsElementSize() {
        assertEquals(3L, selectLong("size", "$." + BIN_MAP + ":MAP.count()"));
    }

    @Test
    public void countOnWholeListIsElementSize() {
        assertEquals(5L, selectLong("size", "$." + BIN_LIST + ":LIST.count()"));
    }

    @Test
    public void countOnMultiSelectMapKeyListIsMatchCount() {
        assertEquals(3L, selectLong("count", "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}.count()"));
    }

    @Test
    public void countOnMultiSelectListRangeIsMatchCount() {
        assertEquals(3L, selectLong("count", "$." + BIN_LIST + ":LIST.[0:3].count()"));
    }

    @Test
    public void countOnSingleMapKeyIsNotApplicable() {
        // .count() on a navigated scalar (INT at key alpha) is OP_NOT_APPLICABLE —
        // SIZE only applies to LIST/MAP containers, not leaf values.
        AerospikeException ex = assertThrows(AerospikeException.BinOpInvalidException.class,
            () -> selectLong("count", "$." + BIN_MAP + ":MAP.alpha:INT.count()"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode());
    }

    // --- rank / positional getters / rel-range ---

    @Test
    public void listRankSelectorRead() {
        assertEquals(200L, selectLong("rank", "$." + BIN_LIST + ":LIST.[{#1}]:INT"));
    }

    @Test
    public void mapRankSelectorRead() {
        assertEquals(10L, selectLong("rank", "$." + BIN_MAP + ":MAP.{#0}:INT"));
    }

    @Test
    public void getIndexesForListValue() {
        List<?> indexes = selectList("idx", "$." + BIN_LIST + ":LIST.[=200:200].getIndexes()");
        assertThat(indexes)
            .extracting(v -> ((Number) v).longValue())
            .containsExactly(1L);
    }

    @Test
    public void getRanksForListValue() {
        List<?> ranks = selectList("rank", "$." + BIN_LIST + ":LIST.[=200:200].getRanks()");
        assertThat(ranks)
            .extracting(v -> ((Number) v).longValue())
            .containsExactly(1L);
    }

    @Test
    public void listRelRankRangeCount() {
        assertEquals(2L, selectLong("n", "$." + BIN_LIST + ":LIST.[#0:1~200].count()"));
    }

    // --- string / blob path methods ---

    @Test
    public void stringTrimPathMethod() {
        assertEquals("trim-me", selectString("trimmed", "$." + BIN_STR + ":STRING.trim()"));
    }

    @Test
    public void blobBitGetPathMethod() {
        assertEquals(0xDEL, selectLong("bits", "$." + BIN_BLOB + ":BLOB.bitGet(offset: 0, size: 8)"));
    }

    @Test
    public void missingPathSegmentWithNoFailReturnsNull() {
        Object value = selectValueAllowNull("v", "$." + BIN_MAP + ":MAP.missing:INT:NO_FAIL");
        assertThat(value).isNull();
    }

    // --- path writes (mutations via AEL strings) ---

    @Test
    public void mapSetToOnPath() {
        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.delta:INT.setTo(99)");
        assertEquals(99L, mapLong("delta"));
    }

    @Test
    public void mapAddOnPath() {
        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.alpha.add(5)");
        assertEquals(15L, mapLong("alpha"));
    }

    @Test
    public void mapRemoveOnPath() {
        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.beta.remove()");
        assertThat(mapLong("beta")).isNull();
        assertEquals(2L, selectLong("size", "$." + BIN_MAP + ":MAP.count()"));
    }

    @Test
    public void listAppendOnPath() {
        upsertPath(BIN_LIST, "$." + BIN_LIST + ":LIST.append(600)");
        assertEquals(600L, selectLong("last", "$." + BIN_LIST + ":LIST.[5]:INT"));
    }

    @Test
    public void modifyIncrementsMatchingMapValues() {
        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.*[?(@ > 15)].modify(@ + 1)");
        assertEquals(10L, mapLong("alpha"));
        assertEquals(21L, mapLong("beta"));
        assertEquals(31L, mapLong("gamma"));
    }

    @Test
    public void createOrderedNestedMapOnMissingKey() {
        upsertPath(BIN_MAP, "$." + BIN_MAP + ":MAP.nested:KEY_ORDERED.inner:INT.setTo(7)");
        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) mapValue("nested");
        assertNotNull(nested);
        assertEquals(7L, ((Number) nested.get("inner")).longValue());
    }

    // --- typed bins / path tails ---

    @Test
    public void typedScalarBinRead() {
        assertEquals(42L, selectLong("typed", "$." + BIN_INT + ":INT"));
    }

    @Test
    public void typedMapValueRead() {
        assertEquals(20L, selectLong("typed", "$." + BIN_MAP + ":MAP.beta:INT"));
    }

    @Test
    public void typedPathUsedInFilter() {
        try (RecordStream rs = session.query(key)
            .bin("hit")
            .selectFrom("$." + BIN_MAP + ":MAP.beta:INT")
            .where("$." + BIN_MAP + ":MAP.beta:INT == 20")
            .execute()) {
            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(20L, rec.getLong("hit"));
        }
    }

    // --- helpers ---

    private long selectLong(String resultBin, String ael) {
        Object value = selectValue(resultBin, ael);
        assertNotNull(value, () -> "null result for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private List<?> selectList(String resultBin, String ael) {
        Object value = selectValue(resultBin, ael);
        assertNotNull(value, () -> "null result for AEL: " + ael);
        assertInstanceOf(List.class, value, () -> "expected list for AEL: " + ael);
        return (List<?>) value;
    }

    private Object selectValue(String resultBin, String ael) {
        try (RecordStream rs = session.query(key)
            .bin(resultBin)
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            return rec.getValue(resultBin);
        }
    }

    private Object selectValueAllowNull(String resultBin, String ael) {
        try (RecordStream rs = session.query(key)
            .bin(resultBin)
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            return rec.getValue(resultBin);
        }
    }

    private String selectString(String resultBin, String ael) {
        Object value = selectValue(resultBin, ael);
        assertInstanceOf(String.class, value, () -> "expected string for AEL: " + ael);
        return (String) value;
    }

    private void upsertPath(String binName, String ael) {
        session.update(key)
            .bin(binName).upsertFrom(ael)
            .execute();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapBin() {
        Record rec = session.query(key).execute().getFirstRecord();
        assertNotNull(rec);
        return (Map<String, Object>) rec.getValue(BIN_MAP);
    }

    private Long mapLong(String mapKey) {
        Object value = mapBin().get(mapKey);
        return value == null ? null : ((Number) value).longValue();
    }

    private Object mapValue(String mapKey) {
        return mapBin().get(mapKey);
    }
}
