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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.OperationResult;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.ResultCode;

/** Top-K merge tests. */
public class TopKMergeTest {
    private static final String NS = "test";
    private static final String SET = "topk";

    private static RecordResult okRecord(String userKey, String binName, Object value) {
        Key key = new Key(NS, SET, userKey);
        Map<String, Object> bins = new HashMap<>();
        bins.put(binName, value);
        Record rec = new Record(bins, new OperationResult[0], 1, 0);
        return new RecordResult(key, rec, 0);
    }

    private static RecordResult errorRecord(String userKey) {
        Key key = new Key(NS, SET, userKey);
        return new RecordResult(key, ResultCode.TIMEOUT, 0, "timeout", 0, false);
    }

    @Test
    void integerAscKeepsSmallest() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 30L),
            okRecord("b", "n", 10L),
            okRecord("c", "n", 20L)));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 2);

        assertEquals(2, result.size());
        assertEquals("b", result.get(0).getKey().userKey.toString());
        assertEquals("c", result.get(1).getKey().userKey.toString());
    }

    @Test
    void integerDescKeepsLargest() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 30L),
            okRecord("b", "n", 10L),
            okRecord("c", "n", 20L)));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.DESC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 2);

        assertEquals(2, result.size());
        assertEquals("a", result.get(0).getKey().userKey.toString());
        assertEquals("c", result.get(1).getKey().userKey.toString());
    }

    @Test
    void doubleNaNSortsGreaterThanAllFiniteValues() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "d", Double.NaN),
            okRecord("b", "d", 5.0),
            okRecord("c", "d", 100.0)));

        OrderBySpec ascSpec = new OrderBySpec("d", OrderByType.DOUBLE, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> ascResult = TopKMerge.mergeSortDedupeTruncate(candidates, ascSpec, 1);
        assertEquals("b", ascResult.get(0).getKey().userKey.toString());

        OrderBySpec descSpec = new OrderBySpec("d", OrderByType.DOUBLE, Order.DESC, OrderByFlags.NONE);
        List<RecordResult> descResult = TopKMerge.mergeSortDedupeTruncate(candidates, descSpec, 1);
        assertEquals("a", descResult.get(0).getKey().userKey.toString());
    }

    @Test
    void stringCaseInsensitiveFlagFoldsAsciiCase() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "s", "Banana"),
            okRecord("b", "s", "apple"),
            okRecord("c", "s", "Cherry")));

        OrderBySpec spec = new OrderBySpec("s", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        assertEquals("b", result.get(0).getKey().userKey.toString());
        assertEquals("a", result.get(1).getKey().userKey.toString());
        assertEquals("c", result.get(2).getKey().userKey.toString());
    }

    @Test
    void stringComparesByUtf8ByteOrderNotJavaUtf16() {
        // Verify UTF-8 byte ordering differs from UTF-16 ordering.
        String bmp = "\uFFFF";
        String supplementary = "\uD800\uDC00";
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("supp", "s", supplementary),
            okRecord("bmp", "s", bmp)));

        OrderBySpec spec = new OrderBySpec("s", OrderByType.STRING, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 2);

        assertEquals("bmp", result.get(0).getKey().userKey.toString());
        assertEquals("supp", result.get(1).getKey().userKey.toString());
    }

    @Test
    void bytesCompareByteOrderShorterWinsCommonPrefixTie() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "b", new byte[] {1, 2, 3}),
            okRecord("b", "b", new byte[] {1, 2}),
            okRecord("c", "b", new byte[] {1, 3})));

        OrderBySpec spec = new OrderBySpec("b", OrderByType.BYTES, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        assertEquals("b", result.get(0).getKey().userKey.toString());
        assertEquals("a", result.get(1).getKey().userKey.toString());
        assertEquals("c", result.get(2).getKey().userKey.toString());
    }

    @Test
    void nilRanksLastInAscendingOrder() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 10L),
            okRecord("missingBin", "other", 5L),
            okRecord("wrongType", "n", "not-an-integer")
        ));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        assertEquals("a", result.get(0).getKey().userKey.toString());
        assertEquals(3, result.size());
    }

    @Test
    void nilRanksLastInDescendingOrderToo() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 10L),
            okRecord("missingBin", "other", 5L)));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.DESC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 2);

        assertEquals("a", result.get(0).getKey().userKey.toString());
        assertEquals("missingBin", result.get(1).getKey().userKey.toString());
    }

    @Test
    void twoKeysAreLexicographicWithNilFirstKeyTies() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "first", 1L),
            okRecord("b", "first", 1L),
            okRecord("c", "first", 2L),
            okRecord("nil", "second", 1L)
        ));
        candidates.get(0).getRecord().bins.put("second", 2L);
        candidates.get(1).getRecord().bins.put("second", 3L);
        candidates.get(2).getRecord().bins.put("second", 0L);

        List<OrderBySpec> specs = List.of(
            new OrderBySpec("first", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE),
            new OrderBySpec("second", OrderByType.INTEGER, Order.DESC, OrderByFlags.NONE));
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, specs, 4);

        assertEquals("b", result.get(0).getKey().userKey.toString());
        assertEquals("a", result.get(1).getKey().userKey.toString());
        assertEquals("c", result.get(2).getKey().userKey.toString());
        assertEquals("nil", result.get(3).getKey().userKey.toString());
    }

    @Test
    void duplicateDigestsAreDedupedKeepingOneSurvivor() {
        Key sharedKey = new Key(NS, SET, "dup");
        Map<String, Object> bins = new HashMap<>();
        bins.put("n", 10L);
        Record rec = new Record(bins, new OperationResult[0], 1, 0);

        List<RecordResult> candidates = new ArrayList<>(List.of(
            new RecordResult(sharedKey, rec, 0),
            new RecordResult(sharedKey, rec, 0),
            okRecord("other", "n", 20L)));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 10);

        assertEquals(2, result.size());
    }

    @Test
    void truncatesToK() {
        List<RecordResult> candidates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            candidates.add(okRecord("k" + i, "n", (long)i));
        }

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        assertEquals(3, result.size());
        assertEquals("k0", result.get(0).getKey().userKey.toString());
        assertEquals("k1", result.get(1).getKey().userKey.toString());
        assertEquals("k2", result.get(2).getKey().userKey.toString());
    }

    @Test
    void errorResultTerminatesReduction() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 1L),
            okRecord("b", "n", 2L),
            errorRecord("errored")));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        assertThrows(AerospikeException.class,
            () -> TopKMerge.mergeSortDedupeTruncate(candidates, spec, 1));
    }

    @Test
    void duplicateDigestKeepsHighestGenerationRegardlessOfArrivalOrder() {
        Key key = new Key(NS, SET, "dup");
        Map<String, Object> lowBins = new HashMap<>();
        lowBins.put("n", 10L);
        RecordResult low = new RecordResult(key, new Record(lowBins, new OperationResult[0], 1, 0), 0);

        Map<String, Object> highBins = new HashMap<>();
        highBins.put("n", 99L);
        RecordResult high = new RecordResult(key, new Record(highBins, new OperationResult[0], 5, 0), 0);

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);

        List<RecordResult> lowFirst = TopKMerge.mergeSortDedupeTruncate(
            new ArrayList<>(List.of(low, high)), spec, 10);
        List<RecordResult> highFirst = TopKMerge.mergeSortDedupeTruncate(
            new ArrayList<>(List.of(high, low)), spec, 10);

        assertEquals(1, lowFirst.size());
        assertEquals(1, highFirst.size());
        assertEquals(99L, lowFirst.get(0).getRecord().getLong("n"));
        assertEquals(99L, highFirst.get(0).getRecord().getLong("n"));
    }

    @Test
    void heapEvictionBoundaryKeepsExactlyKBest() {
        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);

        for (int candidateCount : new int[] {2, 3, 4}) {
            List<RecordResult> candidates = new ArrayList<>();
            for (int i = candidateCount - 1; i >= 0; i--) {
                candidates.add(okRecord("k" + i, "n", (long)i));
            }
            List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

            int expected = Math.min(3, candidateCount);
            assertEquals(expected, result.size());
            for (int i = 0; i < expected; i++) {
                assertEquals((long)i, result.get(i).getRecord().getLong("n"));
            }
        }
    }

    @Test
    void listAndMapOrderKeysRankAsNilLast() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("scalar", "n", 10L),
            okRecord("list", "n", List.of(1L, 2L)),
            okRecord("map", "n", Map.of("k", 1L))));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        assertEquals(3, result.size());
        assertEquals("scalar", result.get(0).getKey().userKey.toString());
    }
}
