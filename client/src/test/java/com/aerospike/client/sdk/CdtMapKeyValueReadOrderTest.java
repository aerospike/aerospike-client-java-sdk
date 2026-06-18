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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.MapOrder;

/**
 * Regression for CDT map reads that return key–value pairs ({@code MapReturnType.KEY_VALUE} /
 * {@link com.aerospike.client.sdk.CdtActionNonInvertableBuilder#getKeysAndValues()}): the SDK
 * documents an {@link AerospikeMap} backed by {@link LinkedHashMap} so server key order is
 * preserved. If results are unpacked into a plain {@link java.util.HashMap} backing, iteration
 * order is undefined and key order from the server is lost.
 */
public class CdtMapKeyValueReadOrderTest extends ClusterTest {

    private static final String BIN = "m";

    @Test
    @DisplayName("query: getByKeyRange KEY_VALUE returns AerospikeMap with LinkedHashMap backing and stable key order")
    public void queryGetByKeyRangeKeysAndValuesPreservesOrder() {
        Key key = args.set.id("cdtKvOrderQuery");

        session.delete(key).execute();

        AerospikeMap<String, Long> stored = AerospikeMap.of(MapOrder.KEY_ORDERED, 8);
        stored.put("d", 4L);
        stored.put("b", 2L);
        stored.put("a", 1L);
        stored.put("c", 3L);
        stored.put("e", 5L);

        assertEquals(MapOrder.KEY_ORDERED, stored.getOrder());
        session.upsert(key).bin(BIN).setTo(stored).execute();

        try (RecordStream rs = session.query(key)
                .bin(BIN).onMapKeyRange("b", "e").getKeysAndValues()
                .execute()) {

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            AerospikeMap<?, ?> subset = rec.getMap(BIN);
            
            assertEquals(MapOrder.KEY_ORDERED, subset.getOrder());

            Map<?, ?> backing = subset.getMap();
            assertInstanceOf(
                    LinkedHashMap.class,
                    backing,
                    "KEY_VALUE read results should use LinkedHashMap backing (see CdtActionNonInvertableBuilder#getKeysAndValues) "
                            + "so server key order is preserved; got " + backing.getClass().getName());

            List<Object> keysInIterationOrder = new ArrayList<>();
            for (Map.Entry<?, ?> e : subset.entrySet()) {
                keysInIterationOrder.add(e.getKey());
            }
            assertEquals(List.of("b", "c", "d"), keysInIterationOrder,
                    "subset keys should follow ascending key order for [b, e) on a KEY_ORDERED map");
        }
    }

    @Test
    @DisplayName("operate batch: getByKeyRange KEY_VALUE returns AerospikeMap with LinkedHashMap backing")
    public void operateGetByKeyRangeKeysAndValuesPreservesOrder() {
        Key key = args.set.id("cdtKvOrderOperate");

        session.delete(key).execute();

        AerospikeMap<String, Long> stored = AerospikeMap.of(MapOrder.KEY_ORDERED, 8);
        stored.put("d", 4L);
        stored.put("b", 2L);
        stored.put("a", 1L);
        stored.put("c", 3L);
        stored.put("e", 5L);

        try (RecordStream rs = session.upsert(key)
                .bin(BIN).setTo(stored)
                .bin(BIN).onMapKeyRange("b", "e").getKeysAndValues()
                .execute()) {

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            AerospikeList<?> results = rec.getList(BIN);
            AerospikeMap<?, ?> subset = (AerospikeMap<?, ?>) results.get(results.size() - 1);

            Map<?, ?> backing = subset.getMap();
            assertInstanceOf(
                    LinkedHashMap.class,
                    backing,
                    "KEY_VALUE operate read should use LinkedHashMap backing; got " + backing.getClass().getName());

            List<Object> keysInIterationOrder = new ArrayList<>();
            for (Map.Entry<?, ?> e : subset.entrySet()) {
                keysInIterationOrder.add(e.getKey());
            }
            assertEquals(List.of("b", "c", "d"), keysInIterationOrder);
        }
    }
}
