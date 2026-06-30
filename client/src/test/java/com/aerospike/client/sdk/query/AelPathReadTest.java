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
}
