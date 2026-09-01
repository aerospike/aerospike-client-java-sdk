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
 * Server-backed AEL integration tests for the write-intent verbs, {@code modify(...)} and
 * the {@code :PROPERTY} flags.
 *
 * <p>These run through {@code upsertFrom}, which evaluates the AEL against the record and
 * stores the <em>result</em> in bin {@code out}. The source bin is not mutated, so each
 * test asserts on the computed output and (where it matters) that the source is intact.
 *
 * <p>Fixture — one record with these bins:
 * <pre>
 *   l   [100, 200, 300, 400, 500]
 *   m   {alpha: 10, beta: 20, gamma: 30}    key-ordered
 * </pre>
 */
public class AelPathWriteTest extends ClusterTest {
    private static final String KEY = "ael_path_write";

    private static final List<Long> SEEDED_LIST = List.of(100L, 200L, 300L, 400L, 500L);

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
            .bin("l").setTo(List.of(100, 200, 300, 400, 500))
            .bin("m").setTo(map)
            .execute();
    }

    // --- element writes ---

    @Test
    public void setToReplacesListElement() {
        assertEquals(List.of(999L, 200L, 300L, 400L, 500L),
            writeLongs("$.l:LIST.[0]:INT.setTo(999)"));
    }

    @Test
    public void addIncrementsListElement() {
        assertEquals(List.of(105L, 200L, 300L, 400L, 500L),
            writeLongs("$.l:LIST.[0]:INT.add(5)"));
    }

    @Test
    public void setToReplacesMapValue() {
        assertEquals(99L, writeMap("$.m:MAP.alpha:INT.setTo(99)").get("alpha"));
    }

    @Test
    public void addIncrementsMapValue() {
        assertEquals(11L, writeMap("$.m:MAP.alpha:INT.add(1)").get("alpha"));
    }

    @Test
    public void setToCreatesAbsentMapKey() {
        Map<String, Long> result = writeMap("$.m:MAP.delta:INT.setTo(40)");
        assertEquals(4, result.size());
        assertEquals(40L, result.get("delta"));
    }

    @Test
    public void updateReplacesExistingMapKey() {
        assertEquals(11L, writeMap("$.m:MAP.alpha:INT.update(11)").get("alpha"));
    }

    /**
     * The write verb already presets its own create/update flag, so restating it as a
     * property is a parse error rather than a redundant no-op.
     */
    @Test
    public void createOnlyPropertyRejectedOnSetTo() {
        assertWriteFails("$.m:MAP.delta:INT.setTo(40):CREATE_ONLY", ResultCode.PARAMETER_ERROR);
    }

    // --- bulk collection writes ---

    @Test
    public void appendAddsSingleListElement() {
        assertEquals(List.of(100L, 200L, 300L, 400L, 500L, 600L),
            writeLongs("$.l:LIST.append(600)"));
    }

    @Test
    public void appendItemsAddsCollection() {
        assertEquals(List.of(100L, 200L, 300L, 400L, 500L, 700L, 800L),
            writeLongs("$.l:LIST.appendItems([700, 800])"));
    }

    @Test
    public void putItemsMergesIntoMap() {
        Map<String, Long> result = writeMap("$.m:MAP.putItems({delta: 40})");
        assertEquals(4, result.size());
        assertEquals(40L, result.get("delta"));
    }

    @Test
    public void updateItemsRewritesExistingKeys() {
        assertEquals(11L, writeMap("$.m:MAP.updateItems({alpha: 11})").get("alpha"));
    }

    // --- structural writes ---

    @Test
    public void removeDropsSelectedElement() {
        assertEquals(List.of(200L, 300L, 400L, 500L), writeLongs("$.l:LIST.[0]:INT.remove()"));
    }

    @Test
    public void clearEmptiesCollection() {
        assertEquals(List.of(), writeLongs("$.l:LIST.clear()"));
    }

    @Test
    public void sortOrdersList() {
        assertEquals(SEEDED_LIST, writeLongs("$.l:LIST.sort()"));
    }

    // --- modify() over a filtered selection ---

    @Test
    public void modifyAppliesExpressionToMatchedListElements() {
        assertEquals(List.of(100L, 200L, 301L, 401L, 501L),
            writeLongs("$.l:LIST.*[?(@:INT > 200)].modify(@:INT + 1)"));
    }

    @Test
    public void modifyAppliesExpressionToMatchedMapValues() {
        Map<String, Long> result =
            writeMap("$.m:MAP.*[?(@key:STRING == 'beta')].modify(@:INT * 2)");
        assertEquals(40L, result.get("beta"));
        assertEquals(10L, result.get("alpha"));
        assertEquals(30L, result.get("gamma"));
    }

    @Test
    public void modifyLeavesSourceBinUnchanged() {
        writeLongs("$.l:LIST.*[?(@:INT > 200)].modify(@:INT + 1)");
        assertEquals(SEEDED_LIST, readLongs("l"));
    }

    // --- property flags ---

    @Test
    public void addUniqueAcceptsDistinctItems() {
        assertEquals(List.of(100L, 200L, 300L, 400L, 500L, 600L, 700L),
            writeLongs("$.l:LIST.appendItems([600, 700]):ADD_UNIQUE"));
    }

    /**
     * ADD_UNIQUE fails the op when the input carries a duplicate — here {@code 1} appears
     * twice within the appended list itself, independently of the list being appended to.
     */
    @Test
    public void addUniqueRejectsDuplicateWithinInput() {
        assertWriteFails("$.l:LIST.appendItems([1, 1, 2]):ADD_UNIQUE", ResultCode.OP_NOT_APPLICABLE);
    }

    /**
     * NO_FAIL suppresses that failure by discarding the whole op — the list is returned
     * unchanged rather than partially appended.
     */
    @Test
    public void noFailSuppressesAddUniqueFailureWholesale() {
        assertEquals(SEEDED_LIST, writeLongs("$.l:LIST.appendItems([1, 1, 2]):ADD_UNIQUE:NO_FAIL"));
    }

    @Test
    public void noFailAcceptedOnModify() {
        assertEquals(List.of(100L, 200L, 301L, 401L, 501L),
            writeLongs("$.l:LIST.*[?(@:INT > 200)].modify(@:INT + 1):NO_FAIL"));
    }

    // --- absent-bin behaviour ---

    @Test
    public void writeToAbsentBinIsNotApplicable() {
        assertWriteFails("$.nosuchbin:MAP.k:INT.setTo(1)", ResultCode.OP_NOT_APPLICABLE);
    }

    // --- helpers ---

    private void assertWriteFails(String ael, int expectedResultCode) {
        AerospikeException ex = assertThrows(AerospikeException.class, () -> write(ael),
            () -> "expected server to reject AEL: " + ael);
        assertEquals(expectedResultCode, ex.getResultCode(),
            () -> "unexpected result code for AEL: " + ael);
    }

    private List<Long> writeLongs(String ael) {
        Object value = write(ael);
        assertInstanceOf(List.class, value, () -> "expected list for AEL: " + ael);
        return ((List<?>) value).stream().map(v -> ((Number) v).longValue()).toList();
    }

    private Map<String, Long> writeMap(String ael) {
        Object value = write(ael);
        assertInstanceOf(Map.class, value, () -> "expected map for AEL: " + ael);
        Map<String, Long> out = new LinkedHashMap<>();
        ((Map<?, ?>) value).forEach((k, v) -> out.put((String) k, ((Number) v).longValue()));
        return out;
    }

    /** Evaluates a write-intent AEL into bin {@code out} and returns the stored result. */
    private Object write(String ael) {
        try (RecordStream rs = session.query(key)
            .upsert(key)
            .bin("out")
            .upsertFrom(ael)
            .execute()) {
            while (rs.hasNext()) {
                rs.next().recordOrThrow();
            }
        }

        try (RecordStream rs = session.query(key).execute()) {
            assertTrue(rs.hasNext(), () -> "no record after AEL write: " + ael);
            Record rec = rs.next().recordOrThrow();
            Object value = rec.getValue("out");
            assertNotNull(value, () -> "no output bin after AEL write: " + ael);
            return value;
        }
    }

    private List<Long> readLongs(String binName) {
        try (RecordStream rs = session.query(key).execute()) {
            Record rec = rs.next().recordOrThrow();
            return ((List<?>) rec.getValue(binName)).stream()
                .map(v -> ((Number) v).longValue())
                .toList();
        }
    }
}
