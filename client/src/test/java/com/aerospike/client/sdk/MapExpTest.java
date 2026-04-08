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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

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

		// Expression where = Exp.build(Exp.eq(Exp.mapBin(binName), Exp.val(map)));
		String where = "$." + binName + ".get(type: MAP) == {'key1': 'e', 'key2': 'd', 'key3': 'c', 'key4': 'b', 'key5': 'a'}";

		RecordStream rs = session.query(key)
			.readingOnlyBins(binName)
			.where(where)
			.execute();

		assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		AerospikeMap<?,?> m = rec.getMap(binName);

		// A sorted map is returned as a LinkedHashMap for performance.
		// The response is ordered, so the LinkedHashMap insertion order
		// will match the sort order.
		assertEquals(AerospikeMap.Type.LINKED, m.getType());
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

		//Expression readExp = Exp.build(
		//	MapExp.removeByValue(MapReturnType.INVERTED, Exp.val(2), Exp.mapBin(binName)));
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
