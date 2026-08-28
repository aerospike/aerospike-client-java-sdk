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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;

/**
 * Client-side Top-K merge: bounded buffer-then-sort-once, since each server node already
 * bounds and sorts its own output to at most {@code k} records, so the total volume across
 * all nodes is tiny and known in advance ({@code k * nodeCount}).
 *
 * <p>The comparator replicates the server's total order: non-NIL before NIL, then the declared
 * per-type comparator (DOUBLE NaN sorts greater than all finite values), then digest-ascending
 * as the tie-break. A key is NIL when the bin is absent, wrong type, or a collection.</p>
 */
final class TopKMerge {

    private TopKMerge() {
    }

    /**
     * Sorts the successful candidates, dedupes by digest (a digest can appear twice if partition
     * ownership migrated mid-scan), truncates to the best {@code k}, and appends any non-OK
     * results unchanged.
     *
     * @param candidates all records collected from every node's already-bounded Top-K result
     * @param spec       the order-by clause driving both key extraction and comparator direction
     * @param k          the Top-K limit; the returned OK-result count never exceeds this
     * @return the merged, deduplicated, truncated result list, in final delivery order
     */
    static List<RecordResult> mergeSortDedupeTruncate(List<RecordResult> candidates, OrderBySpec spec, int k) {
        List<RecordResult> ok = new ArrayList<>();
        List<RecordResult> errors = new ArrayList<>();

        for (RecordResult rr : candidates) {
            if (rr.isOk()) {
                ok.add(rr);
            }
            else {
                errors.add(rr);
            }
        }

        ok.sort(comparator(spec));

        List<RecordResult> result = new ArrayList<>(Math.min(k, ok.size()) + errors.size());
        Set<ByteBuffer> seenDigests = new HashSet<>();

        for (RecordResult rr : ok) {
            if (result.size() >= k) {
                break;
            }
            if (seenDigests.add(ByteBuffer.wrap(rr.getKey().digest))) {
                result.add(rr);
            }
        }

        result.addAll(errors);
        return result;
    }

    /**
     * The order key for a record per the order-by spec, or {@code null} for NIL (bin absent,
     * wrong type, or a collection). Matches {@code order_key_from_bin} in the server design.
     */
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
        return ((spec.getFlags() & OrderByFlags.CASE_INSENSITIVE) != 0) ? s.toLowerCase(Locale.ROOT) : s;
    }

    private static Comparator<RecordResult> comparator(OrderBySpec spec) {
        return (a, b) -> {
            Object keyA = extractKey(a.getRecord(), spec);
            Object keyB = extractKey(b.getRecord(), spec);

            boolean nilA = keyA == null;
            boolean nilB = keyB == null;

            if (nilA != nilB) {
                // Non-NIL before NIL, in both ASC and DESC.
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

            // Tie-break: digest ascending, unconditionally (both NIL, or a genuine tie).
            return compareDigests(a, b);
        };
    }

    @SuppressWarnings("unchecked")
    private static int compareNonNilKeys(Object keyA, Object keyB, OrderByType type) {
        if (type == OrderByType.DOUBLE) {
            // Double.compare ranks NaN greater than all finite values.
            return Double.compare((Double)keyA, (Double)keyB);
        }
        if (type == OrderByType.BYTES) {
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
