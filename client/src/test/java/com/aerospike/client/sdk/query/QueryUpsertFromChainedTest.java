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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations
 * under the License.
 */
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;

/**
 * Mirrors the Python SDK intent: seed {@code value}, then in one fluent chain
 * {@code query(key).upsert(key).bin("computed").upsertFrom("$.value:INT + 1000")}
 * so the write sees the record read in the same multi-spec execution, and
 * {@code computed} should be {@code 1006} on a subsequent read.
 */
public class QueryUpsertFromChainedTest extends ClusterTest {

    private static final String TEST_KEY = "fluent_cb_exp_1";

    @Test
    public void chainedQueryThenUpsertFromAddsComputedBin() {
        Key key = args.set.id(TEST_KEY);

        session.delete(key).execute();

        session.upsert(key)
            .bins("value")
            .values(6)
            .execute();

        int batchResults = 0;
        try (RecordStream rs = session.query(key)
            .upsert(key)
            .bin("computed")
            .upsertFrom("$.\"vaLue\" + 1000")
            .execute()) {
            while (rs.hasNext()) {
                RecordResult rr = rs.next();
                assertTrue(rr.isOk(),
                    () -> "unexpected resultCode=" + rr.resultCode() + " index=" + rr.index());
                batchResults++;
            }
        }

        assertEquals(2, batchResults, "expected one query result and one upsert result");

        try (RecordStream rs = session.query(key).execute()) {
            Record rec = rs.next().recordOrThrow();
            assertTrue(rec.bins.containsKey("computed"),
                () -> "computed bin missing after chained batch; bins=" + rec.bins);
            assertEquals(1006, rec.getInt("computed"));
            assertFalse(rs.hasNext());
        }
    }
}
