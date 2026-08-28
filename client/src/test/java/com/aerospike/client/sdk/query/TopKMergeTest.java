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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.OperationResult;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.ResultCode;

/**
 * Pure unit tests for {@link TopKMerge}: no cluster/session needed, since this is client-side
 * buffer/sort/dedupe/truncate logic over already-constructed {@link RecordResult} objects.
 */
class TopKMergeTest {
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

    // -- INTEGER / DOUBLE / STRING / BYTES ordering, both directions -----------

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

        // ASC keeps the smallest -- NaN should never win ASC over a finite value.
        OrderBySpec ascSpec = new OrderBySpec("d", OrderByType.DOUBLE, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> ascResult = TopKMerge.mergeSortDedupeTruncate(candidates, ascSpec, 1);
        assertEquals("b", ascResult.get(0).getKey().userKey.toString());

        // DESC keeps the largest -- NaN sorts greater than all finite values, so it wins DESC.
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

        assertEquals("b", result.get(0).getKey().userKey.toString()); // apple
        assertEquals("a", result.get(1).getKey().userKey.toString()); // Banana
        assertEquals("c", result.get(2).getKey().userKey.toString()); // Cherry
    }

    @Test
    void bytesCompareByteOrderShorterWinsCommonPrefixTie() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "b", new byte[] {1, 2, 3}),
            okRecord("b", "b", new byte[] {1, 2}),
            okRecord("c", "b", new byte[] {1, 3})));

        OrderBySpec spec = new OrderBySpec("b", OrderByType.BYTES, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        // {1,2} < {1,2,3} (shorter wins the common-prefix tie) < {1,3}
        assertEquals("b", result.get(0).getKey().userKey.toString());
        assertEquals("a", result.get(1).getKey().userKey.toString());
        assertEquals("c", result.get(2).getKey().userKey.toString());
    }

    // -- NIL handling: missing bin, wrong type, collection ---------------------

    @Test
    void nilRanksLastInAscendingOrder() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 10L),
            okRecord("missingBin", "other", 5L),        // bin absent -> NIL
            okRecord("wrongType", "n", "not-an-integer") // type mismatch -> NIL
        ));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 3);

        assertEquals("a", result.get(0).getKey().userKey.toString());
        // Both NIL records follow, tie-broken by digest -- order between them is unspecified
        // here, but neither may precede the non-NIL record.
        assertEquals(3, result.size());
    }

    @Test
    void nilRanksLastInDescendingOrderToo() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 10L),
            okRecord("missingBin", "other", 5L)));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.DESC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 2);

        // NIL ranks last even in DESC, not first.
        assertEquals("a", result.get(0).getKey().userKey.toString());
        assertEquals("missingBin", result.get(1).getKey().userKey.toString());
    }

    // -- Digest dedup and truncation --------------------------------------------

    @Test
    void duplicateDigestsAreDedupedKeepingOneSurvivor() {
        Key sharedKey = new Key(NS, SET, "dup");
        Map<String, Object> bins = new HashMap<>();
        bins.put("n", 10L);
        Record rec = new Record(bins, new OperationResult[0], 1, 0);

        // Same digest arriving twice, as could happen across two nodes mid partition-migration.
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

    // -- Errors are never silently dropped --------------------------------------

    @Test
    void errorResultsPassThroughUnchangedAndDoNotCountAgainstK() {
        List<RecordResult> candidates = new ArrayList<>(List.of(
            okRecord("a", "n", 1L),
            okRecord("b", "n", 2L),
            errorRecord("errored")));

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        List<RecordResult> result = TopKMerge.mergeSortDedupeTruncate(candidates, spec, 1);

        // k=1 -> exactly one OK result, plus the error appended unconditionally.
        long okCount = result.stream().filter(RecordResult::isOk).count();
        long errorCount = result.stream().filter(rr -> !rr.isOk()).count();
        assertEquals(1, okCount);
        assertEquals(1, errorCount);
        assertTrue(result.stream().anyMatch(rr -> !rr.isOk() && rr.getResultCode() == ResultCode.TIMEOUT));
    }
}
