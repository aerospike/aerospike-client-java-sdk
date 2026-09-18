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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;

/** Client-side Top-K merge. */
final class TopKMerge {

    private TopKMerge() {
    }

    /** Merge, deduplicate, and order candidates. */
    static List<RecordResult> mergeSortDedupeTruncate(
        List<RecordResult> candidates, List<OrderBySpec> specs, int k
    ) {
        Reducer reducer = new Reducer(specs, k);
        candidates.forEach(reducer::accept);
        return reducer.finish();
    }

    static List<RecordResult> mergeSortDedupeTruncate(
        List<RecordResult> candidates, OrderBySpec spec, int k
    ) {
        return mergeSortDedupeTruncate(candidates, List.of(spec), k);
    }

    /** Thread-confined bounded reduction state. */
    static final class Reducer {
        private final Comparator<RecordResult> bestFirst;
        private final PriorityQueue<RecordResult> worstFirst;
        private final Map<ByteBuffer, RecordResult> byDigest = new HashMap<>();
        private final int k;

        Reducer(List<OrderBySpec> specs, int k) {
            if (specs == null || specs.isEmpty() || specs.size() > 2) {
                throw new IllegalArgumentException("Top-K requires one or two order-by specs");
            }
            this.bestFirst = comparator(specs);
            this.worstFirst = new PriorityQueue<>(k, bestFirst.reversed());
            this.k = k;
        }

        void accept(RecordResult candidate) {
            if (!candidate.isOk()) {
                candidate.orThrow();
            }

            ByteBuffer digest = ByteBuffer.wrap(candidate.getKey().digest);
            RecordResult existing = byDigest.get(digest);
            if (existing != null) {
                if (!shouldReplaceDuplicate(candidate, existing)) {
                    return;
                }
                worstFirst.remove(existing);
                byDigest.remove(digest);
            }

            if (worstFirst.size() == k && bestFirst.compare(candidate, worstFirst.peek()) >= 0) {
                return;
            }
            if (worstFirst.size() == k) {
                RecordResult evicted = worstFirst.remove();
                byDigest.remove(ByteBuffer.wrap(evicted.getKey().digest));
            }
            worstFirst.add(candidate);
            byDigest.put(digest, candidate);
        }

        private boolean shouldReplaceDuplicate(RecordResult candidate, RecordResult existing) {
            int candidateGeneration = candidate.getRecord().generation;
            int existingGeneration = existing.getRecord().generation;
            return candidateGeneration > existingGeneration ||
                (candidateGeneration == existingGeneration && bestFirst.compare(candidate, existing) < 0);
        }

        List<RecordResult> finish() {
            List<RecordResult> result = new ArrayList<>(worstFirst);
            result.sort(bestFirst);
            return result;
        }
    }

    /** Return the order key, or {@code null} for NIL. */
    static Object extractKey(Record record, OrderBySpec spec) {
        if (record == null) {
            return null;
        }
        Object raw = record.getValue(spec.getBinName());

        return switch (spec.getType()) {
            case INTEGER -> (raw instanceof Long) ? raw : null;
            case DOUBLE -> (raw instanceof Double) ? raw : null;
            case STRING -> extractStringKey(raw, spec);
            case BYTES -> (raw instanceof byte[]) ? raw : null;
        };
    }

    private static Object extractStringKey(Object raw, OrderBySpec spec) {
        if (!(raw instanceof String s)) {
            return null;
        }
        // Compare strings by unsigned UTF-8 bytes.
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if ((spec.getFlags() & OrderByFlags.CASE_INSENSITIVE) != 0) {
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] >= 'A' && bytes[i] <= 'Z') {
                    bytes[i] += 'a' - 'A';
                }
            }
        }
        return bytes;
    }

    private static Comparator<RecordResult> comparator(List<OrderBySpec> specs) {
        return (a, b) -> {
            for (OrderBySpec spec : specs) {
                Object keyA = extractKey(a.getRecord(), spec);
                Object keyB = extractKey(b.getRecord(), spec);
                boolean nilA = keyA == null;
                boolean nilB = keyB == null;

                if (nilA != nilB) {
                    return nilA ? 1 : -1;
                }
                if (!nilA) {
                    int cmp = compareNonNilKeys(keyA, keyB, spec.getType());
                    if (spec.getDirection() == Order.DESC) {
                        cmp = -cmp;
                    }
                    if (cmp != 0) {
                        return cmp;
                    }
                }
            }

            return compareDigests(a, b);
        };
    }

    @SuppressWarnings("unchecked")
    private static int compareNonNilKeys(Object keyA, Object keyB, OrderByType type) {
        if (type == OrderByType.DOUBLE) {
            double a = (Double)keyA;
            double b = (Double)keyB;
            if (Double.isNaN(a) || Double.isNaN(b)) {
                return Double.isNaN(a) == Double.isNaN(b) ? 0 : (Double.isNaN(a) ? 1 : -1);
            }
            return Double.compare(a, b);
        }
        if (type == OrderByType.BYTES || type == OrderByType.STRING) {
            return compareBytes((byte[])keyA, (byte[])keyB);
        }
        return ((Comparable<Object>)keyA).compareTo(keyB);
    }

    /** Byte order; shorter value wins a common-prefix tie. */
    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compareUnsigned(a[i], b[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    private static int compareDigests(RecordResult a, RecordResult b) {
        return Arrays.compareUnsigned(a.getKey().digest, b.getKey().digest);
    }
}
