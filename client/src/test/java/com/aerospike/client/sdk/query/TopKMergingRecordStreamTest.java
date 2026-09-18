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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.OperationResult;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;

/** Streaming Top-K merge tests. */
public class TopKMergingRecordStreamTest {
    private static final String NS = "test";
    private static final String SET = "topkstream";

    @Test
    void reducesUnboundedUnsortedFallbackStreamToGlobalTopK() {
        AsyncRecordStream source = new AsyncRecordStream(100);
        long[] values = {7, 2, 9, 4, 1, 8, 3, 6, 5, 0};
        for (long v : values) {
            source.publish(okRecord("k" + v, v));
        }
        source.complete();

        OrderBySpec spec = new OrderBySpec("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.NONE);
        RecordStream merged = TopKMergingRecordStream.merge(source, List.of(spec), 3);

        List<Long> got = new ArrayList<>();
        try (merged) {
            while (merged.hasNext()) {
                got.add(merged.next().recordOrThrow().getLong("n"));
            }
        }

        assertEquals(List.of(0L, 1L, 2L), got);
    }

    private static RecordResult okRecord(String userKey, long value) {
        Key key = new Key(NS, SET, userKey);
        Map<String, Object> bins = new HashMap<>();
        bins.put("n", value);
        Record rec = new Record(bins, new OperationResult[0], 1, 0);
        return new RecordResult(key, rec, 0);
    }
}
