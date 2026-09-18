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
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assumptions;

import com.aerospike.client.sdk.query.Order;
import com.aerospike.client.sdk.query.OrderByType;
import com.aerospike.client.sdk.util.Version;

class TopKIntegrationTest extends ClusterTest {
    @BeforeAll
    static void requireTopKServer() {
        Assumptions.assumeTrue(
            cluster.getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_3),
            "Top-K requires server version 8.1.3+");
    }

    @Test
    void twoKeyTopKOrdersLexicographicallyAndRanksNilLast() {
        DataSet dataSet = DataSet.of(args.namespace, "topk_integration");
        put(dataSet, "a", 1L, 1L);
        put(dataSet, "b", 1L, 3L);
        put(dataSet, "c", 2L, 0L);
        Key missing = dataSet.id("nil");
        session.delete(missing).execute();
        session.upsert(missing).bin("id").setTo("nil").bin("second").setTo(9L).execute();

        List<String> ids = new ArrayList<>();
        try (RecordStream stream = session.query(dataSet)
            .bin("id").get()
            .bin("first").get()
            .bin("second").get()
            .orderBy("first", OrderByType.INTEGER, Order.ASC)
            .thenOrderBy("second", OrderByType.INTEGER, Order.DESC)
            .topK(4)
            .execute()) {
            while (stream.hasNext()) {
                ids.add(stream.next().recordOrThrow().getString("id"));
            }
        }

        assertEquals(List.of("b", "a", "c", "nil"), ids);
    }

    private void put(DataSet dataSet, String id, long first, long second) {
        Key key = dataSet.id(id);
        session.delete(key).execute();
        session.upsert(key)
            .bin("id").setTo(id)
            .bin("first").setTo(first)
            .bin("second").setTo(second)
            .execute();
    }
}
