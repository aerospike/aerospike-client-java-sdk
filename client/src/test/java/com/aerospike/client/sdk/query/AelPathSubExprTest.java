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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;

/**
 * Server-backed AEL integration tests for {@code (expr)} in CDT path parameter
 * slots.
 */
public class AelPathSubExprTest extends ClusterTest {
    private static final String KEY = "ael_path_subexpr";
    private static final String BIN_MAP = "m";
    private static final String BIN_LIST = "l";
    private static final String BIN_KEY = "keyName";
    private static final String BIN_IDX = "idx";
    private static final String BIN_START = "startIdx";
    private static final String BIN_END = "endIdx";
    private static final String BIN_LO = "lo";
    private static final String BIN_HI = "hi";
    private static final String BIN_KEYS = "keys";

    private static Boolean pathSubExprSupported;

    private Key key;

    @BeforeAll
    public static void requirePathSubExprInPathSlots() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
        Assumptions.assumeTrue(
            probePathSubExprSupport(),
            "server AEL does not support (expr) in path parameter slots (§2.3 blocked on ael_parser.y @ 93301ab26)");
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
            .bin(BIN_KEY).setTo("beta")
            .bin(BIN_IDX).setTo(2)
            .bin(BIN_START).setTo(1)
            .bin(BIN_END).setTo(4)
            .bin(BIN_LO).setTo(15)
            .bin(BIN_HI).setTo(31)
            .bin(BIN_KEYS).setTo(List.of("alpha", "gamma"))
            .execute();
    }

    // --- keys, indices, ranks, range bounds via (expr) ---

    @Test
    public void listIndexFromBin() {
        assertEquals(300L, selectLong("hit", "$." + BIN_LIST + ":LIST.[($" + BIN_IDX + ":INT)]:INT"));
    }

    @Test
    public void mapKeyFromBin() {
        assertEquals(20L, selectLong("hit", "$." + BIN_MAP + ":MAP.{($" + BIN_KEY + ":STRING)}:INT"));
    }

    @Test
    public void indexRangeBoundsFromBins() {
        assertEquals(3L, selectLong("n",
            "$." + BIN_LIST + ":LIST.[($" + BIN_START + ":INT):($" + BIN_END + ":INT)].count()"));
    }

    @Test
    public void valueIntervalBoundsFromBins() {
        assertEquals(2L, selectLong("n",
            "$." + BIN_MAP + ":MAP.{=($" + BIN_LO + ":INT):($" + BIN_HI + ":INT)}.count()"));
    }

    @Test
    public void keyListFromBin() {
        assertEquals(2L, selectLong("n", "$." + BIN_MAP + ":MAP.{($" + BIN_KEYS + ":LIST)}.count()"));
    }

    @Test
    public void dynamicPathSelectsExpectedValues() {
        List<?> values = selectList("vals",
            "$." + BIN_MAP + ":MAP.{($" + BIN_KEYS + ":LIST)}");
        assertThat(values)
            .extracting(v -> ((Number) v).longValue())
            .containsExactlyInAnyOrder(10L, 30L);
    }

    @Test
    public void whereMatchesWhenDynamicIndexPredicateTrue() {
        assertTrue(matchesWhere(
            "$." + BIN_LIST + ":LIST.[($" + BIN_IDX + ":INT)]:INT == 300"));
    }

    @Test
    public void whereExcludesWhenDynamicIndexPredicateFalse() {
        assertFalse(matchesWhere(
            "$." + BIN_LIST + ":LIST.[($" + BIN_IDX + ":INT)]:INT == 200"));
    }

    // --- probe + helpers ---

    private static boolean probePathSubExprSupport() {
        if (pathSubExprSupported != null) {
            return pathSubExprSupported;
        }
        Key probeKey = args.set.id("ael_path_subexpr_probe");
        try {
            session.delete(probeKey).execute();
            session.upsert(probeKey)
                .bin(BIN_LIST).setTo(List.of(100, 200, 300))
                .bin(BIN_IDX).setTo(1)
                .execute();

            try (RecordStream rs = session.query(probeKey)
                .bin("hit")
                .selectFrom("$." + BIN_LIST + ":LIST.[($" + BIN_IDX + ":INT)]:INT")
                .execute()) {
                if (!rs.hasNext()) {
                    pathSubExprSupported = false;
                    return false;
                }
                Record rec = rs.next().recordOrThrow();
                Object value = rec.getValue("hit");
                pathSubExprSupported = value instanceof Number
                    && ((Number) value).longValue() == 200L;
                return pathSubExprSupported;
            }
        }
        catch (AerospikeException ex) {
            pathSubExprSupported = false;
            return false;
        }
        finally {
            session.delete(probeKey).execute();
        }
    }

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
