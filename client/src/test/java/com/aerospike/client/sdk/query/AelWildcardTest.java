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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;

/**
 * Server-backed AEL integration tests for wildcard filters, loop variables,
 * and {@code &[?(...)]} AND post-filters.
 */
public class AelWildcardTest extends ClusterTest {
    private static final String KEY = "ael_wildcard";
    private static final String BIN_MAP = "m";
    private static final String BIN_LIST = "l";
    private static final String BIN_INT_MAP = "im";
    private static final String BIN_BLOB_MAP = "bm";

    private Key key;

    @BeforeAll
    public static void requireAelServer() {
        assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);
        map.put("gamma", 30);

        Map<Integer, Object> intKeyMap = new LinkedHashMap<>();
        intKeyMap.put(1, "one");
        intKeyMap.put(2, "two");
        intKeyMap.put(10, "ten");

        byte[] blobKey = new byte[] {0x42};
        Map<byte[], String> blobMap = new LinkedHashMap<>();
        blobMap.put(blobKey, "blob-val");

        session.upsert(key)
            .bin(BIN_MAP).setTo(map)
            .bin(BIN_LIST).setTo(List.of(100, 200, 300, 400, 500))
            .bin(BIN_INT_MAP).setTo(intKeyMap)
            .bin(BIN_BLOB_MAP).setTo(blobMap)
            .execute();
    }

    // --- wildcard filters: @, @key, @index ---

    @Test
    public void listWildcardFilterOnTypedValue() {
        assertEquals(3L, selectLong("n", "$." + BIN_LIST + ":LIST.*[?(@:INT > 250)].count()"));
    }

    @Test
    public void listWildcardFilterOnValueNoMatch() {
        assertEquals(0L, selectLong("n", "$." + BIN_LIST + ":LIST.*[?(@:INT > 900)].count()"));
    }

    @Test
    public void mapWildcardFilterOnValue() {
        assertEquals(2L, selectLong("n", "$." + BIN_MAP + ":MAP.*[?(@ > 15)].count()"));
    }

    @Test
    public void mapWildcardFilterOnKey() {
        assertEquals(1L, selectLong("n", "$." + BIN_MAP + ":MAP.*[?(@key == 'beta')].count()"));
    }

    @Test
    public void listWildcardFilterOnIndex() {
        assertEquals(2L, selectLong("n", "$." + BIN_LIST + ":LIST.*[?(@index >= 3)].count()"));
    }

    @Test
    public void wildcardFilterSelectsMatchingValues() {
        List<?> values = selectList("vals", "$." + BIN_MAP + ":MAP.*[?(@ > 15)]");
        assertThat(values)
            .extracting(v -> ((Number) v).longValue())
            .containsExactlyInAnyOrder(20L, 30L);
    }

    @Test
    public void whereMatchesWhenWildcardPredicateTrue() {
        assertTrue(matchesWhere("$." + BIN_LIST + ":LIST.*[?(@:INT > 400)].count() >= 1"));
    }

    @Test
    public void whereExcludesWhenWildcardPredicateFalse() {
        assertFalse(matchesWhere("$." + BIN_LIST + ":LIST.*[?(@:INT > 500)].count() >= 1"));
    }

    // --- typed loop vars (@:T, @key:T) ---

    @Test
    public void typedValuePinNarrowsListWildcard() {
        // Exact INT match; wrong inference (e.g. STRING lexicographic) would not isolate 200.
        assertEquals(1L, selectLong("n", "$." + BIN_LIST + ":LIST.*[?(@:INT == 200)].count()"));
    }

    @Test
    public void typedStringKeyPinOnMapWildcard() {
        assertEquals(1L, selectLong("n", "$." + BIN_MAP + ":MAP.*[?(@key:STRING == 'gamma')].count()"));
    }

    @Test
    public void typedIntKeyPinOnIntMapWildcard() {
        assertEquals(1L, selectLong("n", "$." + BIN_INT_MAP + ":MAP.*[?(@key:INT == 2)].count()"));
    }

    @Test
    public void typedBlobKeyPinOnBlobMapWildcard() {
        assertEquals(1L, selectLong("n", "$." + BIN_BLOB_MAP + ":MAP.*[?(@key:BLOB == X'42')].count()"));
    }

    // --- &[?(...) AND post-filters ---

    @Test
    public void andFilterAfterIndexRange() {
        assertEquals(2L, selectLong("n", "$." + BIN_LIST + ":LIST.[1:4]&[?(@:INT >= 300)].count()"));
    }

    @Test
    public void andFilterAfterKeyList() {
        assertEquals(2L, selectLong("n", "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}&[?(@ > 15)].count()"));
    }

    @Test
    public void andFilterAfterLoopVarKeyList() {
        assertEquals(2L, selectLong("n",
            "$." + BIN_MAP + ":MAP.{@\"alpha\", \"beta\", \"gamma\"}&[?(@ > 15)].count()"));
    }

    @Test
    public void andFilterAfterValueInterval() {
        assertEquals(1L, selectLong("n", "$." + BIN_MAP + ":MAP.{=10:31}&[?(@key == 'gamma')].count()"));
    }

    @Test
    public void andFilterNarrowsKeyListSelection() {
        List<?> keys = selectList("keys", "$." + BIN_MAP + ":MAP.{alpha,beta,gamma}&[?(@ > 15)].getKeys()");
        List<String> keyNames = keys.stream().map(String.class::cast).toList();
        assertThat(keyNames).containsExactlyInAnyOrder("beta", "gamma");
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

    private boolean matchesWhere(String whereAel) {
        try (RecordStream rs = session.query(key).where(whereAel).execute()) {
            return rs.hasNext();
        }
    }
}
