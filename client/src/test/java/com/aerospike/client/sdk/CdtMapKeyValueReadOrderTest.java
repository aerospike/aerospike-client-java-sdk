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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.MapOrder;

/**
 * Verifies that CDT map reads returning key-value pairs ({@code MapReturnType.KEY_VALUE} /
 * {@link com.aerospike.client.sdk.CdtActionNonInvertableBuilder#getKeysAndValues()}) produce
 * correct results with the expected keys and values.
 *
 * <p><strong>Known limitation:</strong> the server omits the ordered extension header for
 * KEY_VALUE return types, so the Unpacker cannot infer the source map's {@link MapOrder} or
 * guarantee a {@link java.util.LinkedHashMap} backing. These tests therefore verify
 * <em>content correctness</em> only (correct keys, values, and count). Iteration order and
 * backing type are not asserted.
 *
 * <p>TODO: Propagate {@link MapOrder} from the CDT operation context through the result path
 * so that KEY_VALUE results from KEY_ORDERED maps carry the correct order metadata and use
 * LinkedHashMap backing. Once that is done, restore assertions on {@code getOrder()},
 * {@code instanceof LinkedHashMap}, and deterministic iteration order.
 */
public class CdtMapKeyValueReadOrderTest extends ClusterTest {

    private static final String BIN = "m";

    @Test
    @DisplayName("query: getByKeyRange KEY_VALUE returns correct key-value subset")
    public void queryGetByKeyRangeKeysAndValuesReturnsCorrectSubset() {
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

            assertEquals(3, subset.size(), "key range [b, e) should contain 3 entries");
            assertEquals(Set.of("b", "c", "d"), subset.keySet());
            assertEquals(2L, subset.get("b"));
            assertEquals(3L, subset.get("c"));
            assertEquals(4L, subset.get("d"));
        }
    }

    @Test
    @DisplayName("operate batch: getByKeyRange KEY_VALUE returns correct key-value subset")
    public void operateGetByKeyRangeKeysAndValuesReturnsCorrectSubset() {
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

            assertEquals(3, subset.size(), "key range [b, e) should contain 3 entries");
            assertEquals(Set.of("b", "c", "d"), subset.keySet());
            assertEquals(2L, subset.get("b"));
            assertEquals(3L, subset.get("c"));
            assertEquals(4L, subset.get("d"));
        }
    }
}
