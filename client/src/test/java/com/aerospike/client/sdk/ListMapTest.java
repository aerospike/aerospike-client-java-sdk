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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

public class ListMapTest extends ClusterTest {
	@Test
	public void aerospikeListBinsValues() {
		Key key = args.set.id("aerospikeListBinValue");
		String binName = "listbin";

		session.delete(key).execute();

		AerospikeList<String> list = new AerospikeList<>(16);
		list.add("e");
		list.add("d");
		list.add("c");
		list.add("b");
		list.add("a");
		list.sort();

		session.upsert(key)
			.bins(binName)
			.values(list)
	    	.execute();

		RecordStream rs = session.query(key)
			.readingOnlyBins(binName)
			.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> receivedList = rec.getList(binName);
		assertEquals(5, receivedList.size());
		assertEquals("a", receivedList.get(0));
		assertEquals("b", receivedList.get(1));
		assertEquals("c", receivedList.get(2));
		assertEquals("d", receivedList.get(3));
		assertEquals("e", receivedList.get(4));
	}

	@Test
	public void aerospikeListOps() {
		Key key = args.set.id("aerospikeListOps");
		String binName = "listbin";

		session.delete(key).execute();

		AerospikeList<String> list = new AerospikeList<>(16);
		list.add("e");
		list.add("d");
		list.add("c");
		list.add("b");
		list.add("a");
		list.sort();

		session.upsert(key)
	        .bin(binName).setTo(list)
	        .execute();

		RecordStream rs = session.query(key)
			.readingOnlyBins(binName)
			.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> receivedList = rec.getList(binName);
		assertEquals(5, receivedList.size());
		assertEquals("a", receivedList.get(0));
		assertEquals("b", receivedList.get(1));
		assertEquals("c", receivedList.get(2));
		assertEquals("d", receivedList.get(3));
		assertEquals("e", receivedList.get(4));
	}

	@Test
	public void aerospikeMapBinsValues() {
		Key key = args.set.id("aerospikeMapBinsValues");
		String binName = "listbin";

		session.delete(key).execute();

		AerospikeMap<String,Integer> map = new AerospikeMap<>(AerospikeMap.Type.UNORDERED, 16);
		map.put("joe", 90);
		map.put("jim", 76);
		map.put("charlie", 78);

		session.upsert(key)
			.bins(binName)
			.values(map)
	    	.execute();

		RecordStream rs = session.query(key)
			.readingOnlyBins(binName)
			.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        AerospikeMap<?,?> receivedMap = rec.getMap(binName);
		assertEquals(3, receivedMap.size());
		assertEquals(90L, receivedMap.get("joe"));
		assertEquals(76L, receivedMap.get("jim"));
		assertEquals(78L, receivedMap.get("charlie"));
		assertEquals(AerospikeMap.Type.UNORDERED, receivedMap.getType());
	}

	@Test
	public void aerospikeMapOps() {
		Key key = args.set.id("aerospikeMapOps");
		String binName = "listbin";

		session.delete(key).execute();

		AerospikeMap<String,Integer> map = new AerospikeMap<>(AerospikeMap.Type.LINKED, 16);
		map.put("charlie", 78);
		map.put("jim", 76);
		map.put("joe", 90);

		session.upsert(key)
	        .bin(binName).setTo(map)
	        .execute();

		RecordStream rs = session.query(key)
			.readingOnlyBins(binName)
			.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		AerospikeMap<?,?> receivedMap = rec.getMap(binName);
		assertEquals(3, receivedMap.size());
		assertEquals(90L, receivedMap.get("joe"));
		assertEquals(76L, receivedMap.get("jim"));
		assertEquals(78L, receivedMap.get("charlie"));
		assertEquals(AerospikeMap.Type.LINKED, receivedMap.getType());
	}

	@Test
	public void listStrings() {
		Key key = args.set.id("listStrings");
		String binName = "listbin1";

		session.delete(key).execute();

		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");

		session.upsert(key)
	        .bin(binName).setTo(list)
	        .execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> receivedList = rec.getList(binName);
		assertEquals(3, receivedList.size());
		assertEquals("string1", receivedList.get(0));
		assertEquals("string2", receivedList.get(1));
		assertEquals("string3", receivedList.get(2));
	}

	@Test
	public void listComplex() {
		Key key = args.set.id("listComplex");
		String binName = "listbin2";

		session.delete(key).execute();

		String geopoint =
			"{ \"type\": \"Point\", \"coordinates\": [0.00, 0.00] }";

		byte[] blob = new byte[] {3, 52, 125};
		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(2);
		list.add(blob);
		list.add(Value.getAsGeoJSON(geopoint));

		session.upsert(key)
	        .bin(binName).setTo(list)
	        .execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> receivedList = rec.getList(binName);

		assertEquals(4, receivedList.size());
		assertEquals("string1", receivedList.get(0));
		// Server convert numbers to long, so must expect long.
		assertEquals(2L, receivedList.get(1));
		assertArrayEquals(blob, (byte[])receivedList.get(2));
		assertEquals(Value.getAsGeoJSON(geopoint), receivedList.get(3));
	}

	@Test
	public void mapStrings() {
		Key key = args.set.id("mapStrings");
		String binName = "mapbin1";

		session.delete(key).execute();

		HashMap<String,String> map = new HashMap<String,String>();
		map.put("key1", "string1");
		map.put("key2", "loooooooooooooooooooooooooongerstring2");
		map.put("key3", "string3");

		session.upsert(key)
	        .bin(binName).setTo(map)
	        .execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        Map<?,?> receivedMap = rec.getMap(binName);

		assertEquals(3, receivedMap.size());
		assertEquals("string1", receivedMap.get("key1"));
		assertEquals("loooooooooooooooooooooooooongerstring2", receivedMap.get("key2"));
		assertEquals("string3", receivedMap.get("key3"));
	}

	@Test
	public void mapComplex() {
		Key key = args.set.id("mapComplex");
		String binName = "mapbin2";

		session.delete(key).execute();

		byte[] blob = new byte[] {3, 52, 125};
		List<Integer> list = new ArrayList<Integer>();
		list.add(100034);
		list.add(12384955);
		list.add(3);
		list.add(512);

		HashMap<Object,Object> map = new HashMap<Object,Object>();
		map.put("key1", "string1");
		map.put("key2", 2);
		map.put("key3", blob);
		map.put("key4", list);  // map.put("key4", Value.getAsList(list)) works too
		map.put("key5", true);
		map.put("key6", false);

		session.upsert(key)
	        .bin(binName).setTo(map)
	        .execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        Map<?,?> receivedMap = rec.getMap(binName);

		assertEquals(6, receivedMap.size());
		assertEquals("string1", receivedMap.get("key1"));
		// Server convert numbers to long, so must expect long.
		assertEquals(2L, receivedMap.get("key2"));
		assertArrayEquals(blob, (byte[])receivedMap.get("key3"));

		List<?> receivedInner = (List<?>)receivedMap.get("key4");
		assertEquals(4, receivedInner.size());
		assertEquals(100034L, receivedInner.get(0));
		assertEquals(12384955L, receivedInner.get(1));
		assertEquals(3L, receivedInner.get(2));
		assertEquals(512L, receivedInner.get(3));

		assertEquals(true, receivedMap.get("key5"));
		assertEquals(false, receivedMap.get("key6"));
	}

	@Test
	public void listMapCombined() {
		Key key = args.set.id("listMapCombined");
		String binName = "listmapbin";

		session.delete(key).execute();

		byte[] blob = new byte[] {3, 52, 125};
		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(5);

		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1);
		innerMap.put(2, "b");
		innerMap.put(3, blob);
		innerMap.put("list", inner);

		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(8);
		list.add(inner);
		list.add(innerMap);

		session.upsert(key)
	        .bin(binName).setTo(list)
	        .execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        List<?> received = rec.getList(binName);

		assertEquals(4, received.size());
		assertEquals("string1", received.get(0));
		// Server convert numbers to long, so must expect long.
		assertEquals(8L, received.get(1));

		List<?> receivedInner = (List<?>)received.get(2);
		assertEquals(2, receivedInner.size());
		assertEquals("string2", receivedInner.get(0));
		assertEquals(5L, receivedInner.get(1));

		Map<?,?> receivedMap = (Map<?,?>)received.get(3);
		assertEquals(4, receivedMap.size());
		assertEquals(1L, receivedMap.get("a"));
		assertEquals("b", receivedMap.get(2L));
		assertArrayEquals(blob, (byte[])receivedMap.get(3L));

		List<?> receivedInner2 = (List<?>)receivedMap.get("list");
		assertEquals(2, receivedInner2.size());
		assertEquals("string2", receivedInner2.get(0));
		assertEquals(5L, receivedInner2.get(1));
	}

	@Test
	public void keyOrderedMapWithVariousKeyTypes() {
		Key key = args.set.id("keyOrderedMapTypes");
		String binName = "komap";

		session.delete(key).execute();

		TreeMap<String, Object> map = new TreeMap<>();
		map.put("alpha", "value1");
		map.put("beta", 42);
		map.put("gamma", "value3");


		// TreeMap implements SortedMap, which is automatically serialized as KEY_ORDERED.
		session.upsert(key)
			.bin(binName).setTo(map)
			.execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		Map<?, ?> result = rec.getMap(binName);
		assertNotNull(result);
		assertEquals(List.of("alpha", "beta", "gamma"),
				new ArrayList<>(result.keySet()));
	}

	@Test
	public void keyOrderedMapNonScalarKeyCausesParameterError() {
		Key key = args.set.id("keyOrderedMapNonScalarKey");
		String binName = "badMapKO";

		session.delete(key).execute();

		Map<String, Object> inner = new HashMap<>();
		inner.put("inner", 1);
		TreeMap<Object, String> outer = new TreeMap<>(Comparator.comparingInt(System::identityHashCode));
		outer.put(inner, "v");

		AerospikeException ae = assertThrows(AerospikeException.class,
				() -> session.upsert(key).bin(binName).setTo(outer)
						.execute());

		//WARNING (particle): map_from_wire() invalid packed map
		assertEquals(ResultCode.SERVER_ERROR, ae.getResultCode());
	}

	@Test
	public void geoJsonReadBackAsSameType() {
		Key key = args.set.id("geoJsonReadBack");
		String binName = "geobin";

		session.delete(key).execute();

		String geoPoint = "{ \"type\": \"Point\", \"coordinates\": [103.8198, 1.3521] }";

		session.upsert(key)
				.bins(binName)
				.values(Value.getAsGeoJSON(geoPoint))
				.execute();

		RecordStream rs = session.query(key).readingOnlyBins(binName).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		Object val = rec.getValue(binName);
		assertNotNull(val);
		assertTrue(val instanceof Value.GeoJSONValue,
			"Expected GeoJSONValue but got: " + val.getClass().getName());
		assertEquals(geoPoint, val.toString());
	}

	@Test
	public void sortedMapReplace() {
		Key key = args.set.id("sortedMapReplace");
		String binName = "mapbin";

		session.delete(key).execute();

		AerospikeMap<Integer,String> map = new AerospikeMap<>(AerospikeMap.Type.LINKED, 10);
		map.put(1, "s1");
		map.put(2, "s2");
		map.put(3, "s3");

		RecordStream rs = session.replace(key)
	        .bin(binName).setTo(map)
	        .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		rs = session.query(key)
			.readingOnlyBins(binName)
	        .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		AerospikeMap<?,?> receivedMap = rec.getMap(binName);

		assertEquals(3, receivedMap.size());
		assertEquals("s1", receivedMap.get((long)1));
		assertEquals("s2", receivedMap.get((long)2));
		assertEquals("s3", receivedMap.get((long)3));
	}
}
