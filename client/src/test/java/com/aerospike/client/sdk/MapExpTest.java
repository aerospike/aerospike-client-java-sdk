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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.MapExp;

/**
 * Integration tests for map expressions: client {@link Exp} API (always run) and
 * string AEL equivalents (run when server-side AEL supports them).
 */
public class MapExpTest extends ClusterTest {

    @Test
    public void sortedMapEquality() {
        TreeMap<String,String> map = new TreeMap<>();
        map.put("key1", "e");
        map.put("key2", "d");
        map.put("key3", "c");
        map.put("key4", "b");
        map.put("key5", "a");

        Key key = args.set.id("sortedMapEquality");
        String binName = "m";

        session.upsert(key)
            .bin(binName).setTo(map)
            .execute();

        Expression where = Exp.build(Exp.eq(Exp.mapBin(binName), Exp.val(map)));

        RecordStream rs = session.query(key)
            .readingOnlyBins(binName)
            .failOnFilteredOut()
            .where(where)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        AerospikeMap<?,?> m = rec.getMap(binName);

        // A sorted map is returned as a LinkedHashMap for performance.
        // The response is ordered, so the LinkedHashMap insertion order
        // will match the sort order.
        assertEquals(MapOrder.KEY_ORDERED, m.getOrder());
    }

    @Test
    public void invertedMapExp() {
        HashMap<String,Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 2);
        map.put("d", 3);

        Key key = args.set.id("ime");
        String binName = "m";

        session.upsert(key)
            .bin(binName).setTo(map)
            .execute();

        // INVERTED remove returns the map with entries removed where value != 2.
        Expression readExp = Exp.build(
            MapExp.removeByValue(MapReturnType.INVERTED, Exp.val(2), Exp.mapBin(binName)));

        RecordStream rs = session.query(key)
            .bin(binName).selectFrom(readExp)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        Map<?,?> m = rec.getMap(binName);
        assertEquals(2L, m.size());
        assertEquals(2L, m.get("b"));
        assertEquals(2L, m.get("c"));
    }

    /**
     * String AEL equivalents of the {@link Exp} tests above. Skipped on 8.1.3+ until
     * the server accepts these forms (currently Parameter error).
     */
    @Nested
    class StringAel {
        private static boolean serverSupportsSortedMapEqualityFilter;

        @BeforeAll
        public static void probeMapEqualityFilter() {
            assumeSupportsAel();
            Key probeKey = args.set.id("sortedMapEqualityProbe");
            TreeMap<String,String> map = new TreeMap<>();
            map.put("key1", "e");
            map.put("key2", "d");

            session.upsert(probeKey)
                .bin("m").setTo(map)
                .execute();

            String where = "$.m == {'key1': 'e', 'key2': 'd'}";
            try (RecordStream rs = session.query(probeKey)
                .readingOnlyBins("m")
                .failOnFilteredOut()
                .where(where)
                .execute()) {
                serverSupportsSortedMapEqualityFilter = rs.hasNext();
            }
            catch (com.aerospike.client.sdk.AerospikeException ex) {
                serverSupportsSortedMapEqualityFilter = false;
            }
            finally {
                session.delete(probeKey).execute();
            }
        }

        @Test
        public void sortedMapEquality() {
            assumeSupportsAel();
            Assumptions.assumeTrue(serverSupportsSortedMapEqualityFilter,
                "server-side string AEL map equality filter ($.m == {...}) not supported yet");

            TreeMap<String,String> map = new TreeMap<>();
            map.put("key1", "e");
            map.put("key2", "d");
            map.put("key3", "c");
            map.put("key4", "b");
            map.put("key5", "a");

            Key key = args.set.id("sortedMapEqualityAel");
            String binName = "m";

            session.upsert(key)
                .bin(binName).setTo(map)
                .execute();

            String where = "$." + binName + " == {'key1': 'e', 'key2': 'd', 'key3': 'c', 'key4': 'b', 'key5': 'a'}";

            RecordStream rs = session.query(key)
                .readingOnlyBins(binName)
                .failOnFilteredOut()
                .where(where)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            AerospikeMap<?,?> m = rec.getMap(binName);
            assertEquals(MapOrder.KEY_ORDERED, m.getOrder());
        }

        @Test
        public void invertedMapExp() {
            assumeSupportsAel();
            assumeFalse(supportsAel(),
                "server-side string AEL fails (Parameter error): "
                    + "$.m.{=n}.get(return: ORDERED_MAP) in selectFrom is not supported");

            HashMap<String,Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 2);
            map.put("d", 3);

            Key key = args.set.id("imeAel");
            String binName = "m";

            session.upsert(key)
                .bin(binName).setTo(map)
                .execute();

            String readExp = "$." + binName + ".{=2}.get(return: ORDERED_MAP)";

            RecordStream rs = session.query(key)
                .bin(binName).selectFrom(readExp)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            Map<?,?> m = rec.getMap(binName);
            assertEquals(2L, m.size());
            assertEquals(2L, m.get("b"));
            assertEquals(2L, m.get("c"));
        }
    }
}
