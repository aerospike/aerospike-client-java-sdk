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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.aerospike.client.sdk.ResultCode;

/**
 * Server-backed AEL integration tests for CDT path selectors — the brace / bracket
 * vocabulary that picks elements by index, key, value or rank.
 *
 * <p>Complements {@code AelPathReadTest} (bare key / index reads and the multi-select
 * getters) and {@code AelWildcardTest} (filtered wildcards). This suite covers the
 * selector forms themselves, including inversion and value-relative ranges.
 *
 * <p>Seeded state: list {@code [100, 200, 300, 400, 500]} and key-ordered map
 * {@code {alpha: 10, beta: 20, gamma: 30}}.
 */
public class AelPathSelectorTest extends ClusterTest {
    private static final String KEY = "ael_path_selector";
    private static final String BIN_LIST = "l";
    private static final String BIN_MAP = "m";
    private static final String BIN_STRINGS = "sl";

    private static final String L = "$." + BIN_LIST + ":LIST";
    private static final String M = "$." + BIN_MAP + ":MAP";

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
            .bin(BIN_LIST).setTo(List.of(100, 200, 300, 400, 500))
            .bin(BIN_MAP).setTo(map)
            .bin(BIN_STRINGS).setTo(List.of("delta", "alpha", "charlie", "bravo"))
            .execute();
    }

    // --- list: index and index ranges ---

    @Test
    public void listIndexRange() {
        assertEquals(List.of(100L, 200L), selectLongs(L + ".[0:2]"));
    }

    @Test
    public void listIndexRangeOpenEnd() {
        assertEquals(List.of(400L, 500L), selectLongs(L + ".[3:]"));
    }

    @Test
    public void listIndexRangeOpenStart() {
        assertEquals(List.of(100L, 200L), selectLongs(L + ".[:2]"));
    }

    @Test
    public void listIndexRangeInverted() {
        assertEquals(List.of(300L, 400L, 500L), selectLongs(L + ".[!0:2]"));
    }

    // --- list: by value ---

    @Test
    public void listByValueSingular() {
        assertEquals(200L, selectLong(L + ".[=200]:INT"));
    }

    @Test
    public void listByValueRange() {
        assertEquals(List.of(100L, 200L), selectLongs(L + ".[=100:300]"));
    }

    @Test
    public void listByValueOnStringsNeedsQuotedLiteral() {
        assertEquals("alpha", selectValue("$." + BIN_STRINGS + ":LIST.[='alpha']:STRING"));
    }

    @Test
    public void listByValueRejectsUnquotedStringLiteral() {
        assertParameterError("$." + BIN_STRINGS + ":LIST.[=alpha]");
    }

    // --- list: by rank ---

    @Test
    public void listByRankLowest() {
        assertEquals(100L, selectLong(L + ".[#0]:INT"));
    }

    @Test
    public void listByRankHighestUsesNegativeRank() {
        assertEquals(500L, selectLong(L + ".[#-1]:INT"));
    }

    /**
     * A rank range returns elements in rank order, which need not be storage order — an
     * unordered list ranks by value, so {@code [#0:2]} on {@code [200, 100, ...]} yields
     * the two smallest.
     */
    @Test
    public void listByRankRange() {
        assertEquals(List.of(100L, 200L), sortedLongs(L + ".[#0:2]"));
    }

    /**
     * Value-relative rank range: {@code [#0:2~300]} starts at the rank of value 300 and
     * takes elements from there.
     */
    @Test
    public void listByValueRelativeRankRange() {
        assertEquals(List.of(300L, 400L, 500L), sortedLongs(L + ".[#0:2~300]"));
    }

    // --- map: by key ---

    @Test
    public void mapByExplicitKey() {
        assertEquals(10L, selectLong(M + ".{@alpha}:INT"));
    }

    @Test
    public void mapByKeyList() {
        assertEquals(List.of(10L, 30L), selectLongs(M + ".{@alpha,gamma}"));
    }

    @Test
    public void mapByKeyListInverted() {
        assertEquals(List.of(20L), selectLongs(M + ".{!@alpha,gamma}"));
    }

    @Test
    public void mapByKeyRange() {
        assertEquals(List.of(10L, 20L), selectLongs(M + ".{@alpha:gamma}"));
    }

    // --- map: by index, value, rank ---

    @Test
    public void mapByIndexNeedsTypePin() {
        assertEquals(10L, selectLong(M + ".{0}:INT"));
    }

    @Test
    public void mapByIndexRange() {
        assertEquals(List.of(10L, 20L), selectLongs(M + ".{0:2}"));
    }

    @Test
    public void mapByValueSingular() {
        assertEquals(10L, selectLong(M + ".{=10}:INT"));
    }

    @Test
    public void mapByValueRange() {
        assertEquals(List.of(10L, 20L), selectLongs(M + ".{=10:30}"));
    }

    @Test
    public void mapByRankSingular() {
        assertEquals(10L, selectLong(M + ".{#0}:INT"));
    }

    @Test
    public void mapByRankRange() {
        assertEquals(List.of(10L, 20L), selectLongs(M + ".{#0:2}"));
    }

    // --- multi-select getters ---

    @Test
    public void getKeysOnRankRange() {
        assertEquals(List.of("alpha", "beta"), selectList(M + ".{#0:2}.getKeys()"));
    }

    @Test
    public void getIndexesOnListRange() {
        assertEquals(List.of(0L, 1L, 2L), selectLongs(L + ".[0:3].getIndexes()"));
    }

    @Test
    public void getRanksOnListRange() {
        assertEquals(List.of(0L, 1L, 2L), selectLongs(L + ".[0:3].getRanks()"));
    }

    @Test
    public void getMapsRebuildsMapFromKeySelection() {
        Object value = selectValue(M + ".{@alpha,beta}.getMaps()");
        assertInstanceOf(Map.class, value);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) value;
        assertEquals(2, map.size());
        assertEquals(10L, ((Number) map.get("alpha")).longValue());
        assertEquals(20L, ((Number) map.get("beta")).longValue());
    }

    @Test
    public void countOnSelectorIsMatchCount() {
        assertEquals(3L, selectLong(L + ".[0:3].count()"));
        assertEquals(2L, selectLong(M + ".{@alpha,gamma}.count()"));
    }

    // --- selectors in filter position ---

    @Test
    public void selectorUsableInWhereClause() {
        assertTrue(matchesWhere(L + ".[#-1]:INT == 500"));
        assertTrue(matchesWhere(M + ".{@beta}:INT == 20"));
    }

    // --- rejected forms ---

    /**
     * Inversion needs a set to complement, so the singular selects reject it. The
     * plural forms ({@code {!@a,b}}, {@code [!a:b]}) are covered above.
     */
    @Test
    public void invertRejectedOnSingularKeySelect() {
        assertParameterError(M + ".{!@alpha}");
    }

    @Test
    public void invertRejectedOnSingularValueSelect() {
        assertParameterError(L + ".[!=200]");
    }

    @Test
    public void invertRejectedOnSingularRankSelect() {
        assertParameterError(L + ".[!#0]");
    }

    /** getKeys needs a selection to enumerate; a singular select is not one. */
    @Test
    public void getKeysRejectedOnSingularSelect() {
        assertParameterError(M + ".{#0}.getKeys()");
    }

    /** join consumes a whole list, not a multi-select. */
    @Test
    public void joinRejectedOnMultiSelect() {
        assertParameterError("$." + BIN_STRINGS + ":LIST.[0:4].join(',')");
    }

    @Test
    public void joinAcceptedOnWholeList() {
        assertEquals("delta,alpha,charlie,bravo",
            selectValue("$." + BIN_STRINGS + ":LIST.join(',')"));
    }

    // --- helpers ---

    private void assertParameterError(String ael) {
        AerospikeException ex = assertThrows(AerospikeException.class, () -> selectValue(ael),
            () -> "expected server to reject AEL: " + ael);
        assertEquals(ResultCode.PARAMETER_ERROR, ex.getResultCode(),
            () -> "unexpected result code for AEL: " + ael);
    }

    private long selectLong(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Number.class, value, () -> "expected number for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private List<Long> selectLongs(String ael) {
        return selectList(ael).stream().map(v -> ((Number) v).longValue()).toList();
    }

    private List<Long> sortedLongs(String ael) {
        return selectLongs(ael).stream().sorted().toList();
    }

    private List<?> selectList(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(List.class, value, () -> "expected list for AEL: " + ael);
        return (List<?>) value;
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
