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
package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.cdt.MapReturnType;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.exp.MapExp;

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

		// TODO What is DSL equivalent string for this expression.
		Expression e = Exp.build(Exp.eq(Exp.mapBin(binName), Exp.val(map)));

		RecordStream rs = session.query(key)
			.readingOnlyBins(binName)
			.where(e)
			//.where("$.m.[] == [{})");
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

		// TODO What is DSL equivalent string for this expression.
		// Use INVERTED to return map with entries removed where value != 2.
		Expression e = Exp.build(
			MapExp.removeByValue(MapReturnType.INVERTED, Exp.val(2), Exp.mapBin(binName)));

		RecordStream rs = session.query(key)
	        .bin(binName).selectFrom(e)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		Map<?,?> m = rec.getMap(binName);
		assertEquals(2L, m.size());
		assertEquals(2L, m.get("b"));
		assertEquals(2L, m.get("c"));
	}
}
