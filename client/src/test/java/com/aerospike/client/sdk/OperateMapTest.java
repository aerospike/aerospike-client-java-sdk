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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.MapOrder;

public class OperateMapTest extends ClusterTest {
	private static final String binName = "opmapbin";

	@Test
	public void operateMapPut() {
		Key key = args.set.id("operateMapPut");

        session.delete(key).execute();

		// Calling single key writes multiple times performs poorly because the server makes
		// a copy of the map for each call, but we still need to test it.
		// Multiple key writes (like upsertItems()) should be used instead for best performance.
        RecordStream rs = session.upsert(key)
	    	.bin(binName).onMapKey(11).upsert(789)
	    	.bin(binName).onMapKey(10).upsert(999)
	    	.bin(binName).onMapKey(12).insert(500, opt -> opt
	    		.mapOrder(MapOrder.UNORDERED))
	    	.bin(binName).onMapKey(15).insert(1000, opt -> opt
	    		.mapOrder(MapOrder.UNORDERED))
	    	.bin(binName).onMapKey(10).update(1, opt -> opt
	    		.mapOrder(MapOrder.KEY_ORDERED))
	    	.bin(binName).onMapKey(15).update(5, opt -> opt
		    	.mapOrder(MapOrder.UNORDERED))
	        .execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(1, size);

		size = (Long)results.get(i++);
		assertEquals(2, size);

		size = (Long)results.get(i++);
		assertEquals(3, size);

		size = (Long)results.get(i++);
		assertEquals(4, size);

		size = (Long)results.get(i++);
		assertEquals(4, size);

		size = (Long)results.get(i++);
		assertEquals(4, size);

		rs = session.query(key).execute();
		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		Map<?,?> map = rec.getMap(binName);
		assertEquals(4, map.size());
		assertEquals(1L, map.get(10L));
	}

	@Test
	public void operateMapPutItems() {
		Key key = args.set.id("operateMapPutItems");

        session.delete(key).execute();

		Map<Integer,String> addMap = new HashMap<>();
		addMap.put(12, "myval");
		addMap.put(-8734, "str2");
		addMap.put(1, "my default");

		Map<Integer,String> putMap = new HashMap<>();
		putMap.put(12, "myval12222");
		putMap.put(13, "str13");

		Map<Integer,String> updateMap = new HashMap<>();
		updateMap.put(13, "myval2");

		Map<Integer,Object> replaceMap = new HashMap<>();
		replaceMap.put(12, 23);
		replaceMap.put(-8734, "changed");

        RecordStream rs = session.upsert(key)
	    	.bin(binName).mapInsertItems(addMap, opt -> opt
    			.mapOrder(MapOrder.KEY_ORDERED))
	    	.bin(binName).mapUpsertItems(putMap)
	    	.bin(binName).mapUpdateItems(updateMap, opt -> opt
	    		.mapOrder(MapOrder.KEY_ORDERED))
	    	.bin(binName).mapUpdateItems(replaceMap, opt -> opt
		    	.mapOrder(MapOrder.KEY_ORDERED))
	    	.bin(binName).onMapKey(1).getValues()
	    	.bin(binName).onMapKey(-8734).getValues()
	    	.bin(binName).onMapKeyRange(12, 15).getAsOrderedMap()
	    	.bin(binName).onMapKeyRange(12, 15).getAsMap()
	    	.bin(binName).onMapKeyRange(12, 15).getAsOrderedMap()
	        .execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(3, size);

		size = (Long)results.get(i++);
		assertEquals(4, size);

		size = (Long)results.get(i++);
		assertEquals(4, size);

		size = (Long)results.get(i++);
		assertEquals(4, size);

		String str = (String)results.get(i++);
		assertEquals("my default", str);

		str = (String)results.get(i++);
		assertEquals("changed", str);

		AerospikeMap<?,?> map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(2, map.size());

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(2, map.size());

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(2, map.size());
	}

	@Test
	public void operateMapMixed() {
		// Test normal operations with map operations.
		Key key = args.set.id("operateMapMixed");

        session.delete(key).execute();

		Map<Integer,Object> itemMap = new HashMap<>();
		itemMap.put(12, "myval");
		itemMap.put(-8734, "str2");
		itemMap.put(1, "my default");
		itemMap.put(7, 1);

        RecordStream rs = session.upsert(key)
	    	.bin(binName).mapUpsertItems(itemMap, opt -> opt
    			.mapOrder(MapOrder.KEY_VALUE_ORDERED))
	    	.bin("otherbin").setTo("hello")
	        .execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		long size = rec.getLong(binName);
		assertEquals(4, size);

        rs = session.upsert(key)
	    	.bin(binName).onMapKey(12).getIndexes()
	    	.bin("otherbin").append("goodbye")
	    	.bin("otherbin").get()
	        .execute();

        assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		long index = rec.getLong(binName);
		assertEquals(3, index);

		List<?> results = rec.getList("otherbin");
		String val = (String)results.get(1);
		assertEquals("hellogoodbye", val);
	}

	@Test
	public void operateMapSwitch() {
		// Switch from unordered map to a key ordered map.
		Key key = args.set.id("operateMapSwitch");

        session.delete(key).execute();

		Map<Integer,Integer> itemMap = new HashMap<>();
		itemMap.put(4, 4);
		itemMap.put(3, 3);
		itemMap.put(2, 2);
		itemMap.put(1, 1);

		RecordStream rs = session.upsert(key)
	    	.bin(binName).mapUpsertItems(itemMap)
	    	.bin(binName).onMapIndex(2).getAsOrderedMap()
	    	.bin(binName).onMapIndexRange(0, 10).getAsOrderedMap()
	        .execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(4, size);

		AerospikeMap<?,?> map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(1, map.size());

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(4, map.size());

		rs = session.upsert(key)
	    	.bin(binName).mapSetPolicy(MapOrder.KEY_ORDERED, true)
	    	.bin(binName).onMapKeyRange(3, 5).count()
	    	.bin(binName).onMapKeyRange(-5, 2).getAsOrderedMap()
	    	.bin(binName).onMapIndexRange(0, 10).getAsOrderedMap()
	        .execute();

        assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		results = rec.getList(binName);
		i = 0;

		Object obj = results.get(i++);
		assertNull(obj);

		long val = (Long)results.get(i++);
		assertEquals(2, val);

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(1, map.size());

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(4, map.size());
	}

	@Test
	public void operateMapRank() {
		// Test rank.
		Key key = args.set.id("operateMapRank");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("Charlie", 55);
		inputMap.put("Jim", 98);
		inputMap.put("John", 76);
		inputMap.put("Harry", 82);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
	    	.bin(binName).mapUpsertItems(inputMap)
	        .execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		// Increment some user scores.
		rs = session.upsert(key)
	    	.bin(binName).onMapKey("John").add(5)
	    	.bin(binName).onMapKey("Jim").add(-4)
	        .execute();

        assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		long val = (long)results.get(i++);
		assertEquals(81L, val);

		val = (long)results.get(i++);
		assertEquals(94L, val);

		// Get scores.
		rs = session.upsert(key)
	    	.bin(binName).onMapRankRange(-2, 2).getKeys()
	    	.bin(binName).onMapRankRange(0, 2).getAsOrderedMap()
	    	.bin(binName).onMapRank(0).getValues()
	    	.bin(binName).onMapRank(2).getKeys()
	    	.bin(binName).onMapValueRange(90, 95).getRanks()
	    	.bin(binName).onMapValueRange(90, 95).count()
	    	.bin(binName).onMapValueRange(90, 95).getAsOrderedMap()
	    	.bin(binName).onMapValueRange(81, 82).getKeys()
	    	.bin(binName).onMapValue(77).getKeys()
	    	.bin(binName).onMapValue(81).getRanks()
	    	.bin(binName).onMapKey("Charlie").getRanks()
	    	.bin(binName).onMapKey("Charlie").getReverseRanks()
	        .execute();

        assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		results = rec.getList(binName);
		i = 0;

		List<?> list = (List<?>)results.get(i++);
		String str;

		str = (String)list.get(0);
		assertEquals("Harry", str);
		str = (String)list.get(1);
		assertEquals("Jim", str);

		AerospikeMap<?,?> map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(55L, (long)map.get("Charlie"));
		assertEquals(81L, (long)map.get("John"));

		val = (Long)results.get(i++);
		assertEquals(55, val);

		str = (String)results.get(i++);
		assertEquals("Harry", str);

		list = (List<?>)results.get(i++);
		val = (Long)list.get(0);
		assertEquals(3, val);

		val = (Long)results.get(i++);
		assertEquals(1, val);

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(94L, (long)map.get("Jim"));

		list = (List<?>)results.get(i++);
		str = (String)list.get(0);
		assertEquals("John", str);

		list = (List<?>)results.get(i++);
		assertEquals(0, list.size());

		list = (List<?>)results.get(i++);
		val = (Long)list.get(0);
		assertEquals(1, val);

		val = (Long)results.get(i++);
		assertEquals(0, val);

		val = (Long)results.get(i++);
		assertEquals(3, val);
	}

	@Test
	public void operateMapRemove() {
		// Test remove.
		Key key = args.set.id("operateMapRemove");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("Charlie", 55);
		inputMap.put("Jim", 98);
		inputMap.put("John", 76);
		inputMap.put("Harry", 82);
		inputMap.put("Sally", 79);
		inputMap.put("Lenny", 84);
		inputMap.put("Abe", 88);

		List<String> removeItems = new ArrayList<>();
		removeItems.add("Sally");
		removeItems.add("UNKNOWN");
		removeItems.add("Lenny");

		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
	    	.bin(binName).onMapKey("NOTFOUND").removeAnd().getValues()
	    	.bin(binName).onMapKey("Jim").removeAnd().getValues()
	    	.bin(binName).onMapKeyList(removeItems).removeAnd().count()
	    	.bin(binName).onMapValue(55).removeAnd().getKeys()
	    	.bin(binName).mapSize()
		    .execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long val = (Long)results.get(i++);
		assertEquals(7, val);

		Object obj = results.get(i++);
		assertNull(obj);

		val = (Long)results.get(i++);
		assertEquals(98, val);

		val = (Long)results.get(i++);
		assertEquals(2, val);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(1, list.size());
		assertEquals("Charlie", list.get(0));

		val = (Long)results.get(i++);
		assertEquals(3, val);
	}

	@Test
	public void operateMapRemoveRange() {
		// Test remove ranges.
		Key key = args.set.id("operateMapRemoveRange");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("Charlie", 55);
		inputMap.put("Jim", 98);
		inputMap.put("John", 76);
		inputMap.put("Harry", 82);
		inputMap.put("Sally", 79);
		inputMap.put("Lenny", 84);
		inputMap.put("Abe", 88);

		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
	    	.bin(binName).onMapKeyRange("J", "K").removeAnd().count()
	    	.bin(binName).onMapValueRange(80, 85).removeAnd().count()
	    	.bin(binName).onMapIndexRange(0, 2).removeAnd().count()
	    	.bin(binName).onMapRankRange(0, 2).removeAnd().count()
	    	.bin(binName).mapSize()
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long val = (Long)results.get(i++);
		assertEquals(7, val);

		val = (Long)results.get(i++);
		assertEquals(2, val);

		val = (Long)results.get(i++);
		assertEquals(2, val);

		val = (Long)results.get(i++);
		assertEquals(2, val);

		val = (Long)results.get(i++);
		assertEquals(1, val);
	}

	@Test
	public void operateMapClear() {
		// Test clear.
		Key key = args.set.id("operateMapClear");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("Charlie", 55);
		inputMap.put("Jim", 98);

		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		long size = rec.getLong(binName);
		assertEquals(2, size);

		rs = session.upsert(key)
		    .bin(binName).mapClear()
		    .bin(binName).mapSize()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		size = (Long)results.get(1);
		assertEquals(0, size);
	}

	@Test
	public void operateMapScore() {
		// Test score.
		Key key = args.set.id("operateMapScore");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("weiling", 0);
		inputMap.put("briann", 0);
		inputMap.put("brianb", 0);
		inputMap.put("meher", 0);

		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap, opt -> opt
		    	.mapOrder(MapOrder.KEY_VALUE_ORDERED))
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		// Change scores
		rs = session.upsert(key)
		    .bin(binName).onMapKey("weiling").add(10)
		    .bin(binName).onMapKey("briann").add(20)
		    .bin(binName).onMapKey("brianb").add(1)
		    .bin(binName).onMapKey("meher").add(20)
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		long val = (long)results.get(i++);
		assertEquals(10, val);

		val = (long)results.get(i++);
		assertEquals(20, val);

		val = (long)results.get(i++);
		assertEquals(1, val);

		val = (long)results.get(i++);
		assertEquals(20, val);

		// Query top 3 scores
		rs = session.upsert(key)
		    .bin(binName).onMapRankRange(-3, 3).getKeys()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		results = rec.getList(binName);
		i = 0;

		String str = (String)results.get(i++);
		assertEquals("weiling", str);

		str = (String)results.get(i++);
		assertEquals("briann", str);

		str = (String)results.get(i++);
		assertEquals("meher", str);

		// Remove people with score 10 and display top 3 again
		rs = session.upsert(key)
		    .bin(binName).onMapValue(10).removeAnd().getKeys()
		    .bin(binName).onMapRankRange(-3, 3).removeAnd().getKeys()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		results = rec.getList(binName);
		i = 0;

		List<?> list = (List<?>)results.get(i++);
		String s = (String)list.get(0);
		assertEquals("weiling", s);

		list = (List<?>)results.get(i++);
		s = (String)list.get(0);
		assertEquals("brianb", s);
		s = (String)list.get(1);
		assertEquals("briann", s);
		s = (String)list.get(2);
		assertEquals("meher", s);
	}

	@Test
	public void operateMapGetByList() {
		Key key = args.set.id("operateMapGetByList");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("Charlie", 55);
		inputMap.put("Jim", 98);
		inputMap.put("John", 76);
		inputMap.put("Harry", 82);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<String> keyList = new ArrayList<>();
		keyList.add("Harry");
		keyList.add("Jim");

		List<Integer> valueList = new ArrayList<>();
		valueList.add(76);
		valueList.add(50);

		rs = session.upsert(key)
		    .bin(binName).onMapKeyList(keyList).getAsOrderedMap()
		    .bin(binName).onMapValueList(valueList).getAsOrderedMap()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		AerospikeMap<?,?> map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(2, map.size());
		assertEquals(82L, (long)map.get("Harry"));
		assertEquals(98L, (long)map.get("Jim"));

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(1, map.size());
		assertEquals(76L, (long)map.get("John"));
	}

	@Test
	public void operateMapInverted() {
		Key key = args.set.id("operateMapInverted");

        session.delete(key).execute();

		Map<String,Integer> inputMap = new HashMap<>();
		inputMap.put("Charlie", 55);
		inputMap.put("Jim", 98);
		inputMap.put("John", 76);
		inputMap.put("Harry", 82);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<Integer> valueList = new ArrayList<>();
		valueList.add(76);
		valueList.add(55);
		valueList.add(98);
		valueList.add(50);

		rs = session.upsert(key)
		    .bin(binName).onMapValue(81).getAllOtherRanks()
		    .bin(binName).onMapValue(82).getAllOtherRanks()
		    .bin(binName).onMapValueRange(90, 95).getAllOtherRanks()
		    .bin(binName).onMapValueRange(90, 100).getAllOtherRanks()
		    .bin(binName).onMapValueList(valueList).getAllOtherKeysAndValues()
		    .bin(binName).onMapRankRange(-2, 2).getAllOtherKeys()
		    .bin(binName).onMapRankRange(0, 3).getAllOtherKeysAndValues()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(1L, list.get(1));
		assertEquals(2L, list.get(2));

		AerospikeMap<?,?> map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(1, map.size());
		assertEquals(82L, (long)map.get("Harry"));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals("Charlie", list.get(0));
		assertEquals("John", list.get(1));

		map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(1L, map.size());
		assertEquals(98L, (long)map.get("Jim"));
	}

	@Test
	public void operateMapRemoveByKeyListForNonExistingKey() throws Exception {
		Key key = args.set.id("operateMapRemoveByKeyListForNonExistingKey");

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			List<String> list = List.of("key-1");
	        RecordStream rs = session.upsert(key)
		        .bin(binName).onMapKeyList(list).removeAnd().getValues()
		        .execute();

	        assertTrue(rs.hasNext());
	        rs.next().recordOrThrow();
		});

		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, ae.getResultCode());
	}

	@Test
	public void operateMapGetRelative() {
		Key key = args.set.id("operateMapGetRelative");

        session.delete(key).execute();

		Map<Integer,Integer> inputMap = new HashMap<>();
		inputMap.put(0, 17);
		inputMap.put(4, 2);
		inputMap.put(5, 15);
		inputMap.put(9, 10);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		rs = session.upsert(key)
		    .bin(binName).onMapKeyRelativeIndexRange(5, 0).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(5, 1).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(5, -1).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(3, 2).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(3, -2).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(5, 0, 1).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(5, 1, 2).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(5, -1, 1).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(3, 2, 1).getKeys()
		    .bin(binName).onMapKeyRelativeIndexRange(3, -2, 2).getKeys()
		    .bin(binName).onMapValueRelativeRankRange(11, 1).getValues()
		    .bin(binName).onMapValueRelativeRankRange(11, -1).getValues()
		    .bin(binName).onMapValueRelativeRankRange(11, 1, 1).getValues()
		    .bin(binName).onMapValueRelativeRankRange(11, -1, 1).getValues()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		List<?> list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(9L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));
		assertEquals(9L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(9L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(4L, list.get(1));
		assertEquals(5L, list.get(2));
		assertEquals(9L, list.get(3));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(5L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(9L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(4L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(9L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(0L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(17L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(10L, list.get(0));
		assertEquals(15L, list.get(1));
		assertEquals(17L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(17L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(10L, list.get(0));
	}

	@Test
	public void operateMapRemoveRelative() {
		Key key = args.set.id("operateMapRemoveRelative");

        session.delete(key).execute();

		Map<Integer,Integer> inputMap = new HashMap<>();
		inputMap.put(0, 17);
		inputMap.put(4, 2);
		inputMap.put(5, 15);
		inputMap.put(9, 10);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		rs = session.upsert(key)
		    .bin(binName).onMapKeyRelativeIndexRange(5, 0).removeAnd().getValues()
		    .bin(binName).onMapKeyRelativeIndexRange(5, 1).removeAnd().getValues()
		    .bin(binName).onMapKeyRelativeIndexRange(5, -1, 1).removeAnd().getValues()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		List<?> list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(15L, list.get(0));
		assertEquals(10L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(2L, list.get(0));

		// Write values to empty map.
        session.delete(key).execute();

		rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		rs = session.upsert(key)
		    .bin(binName).onMapValueRelativeRankRange(11, 1).removeAnd().getValues()
		    .bin(binName).onMapValueRelativeRankRange(11, -1, 1).removeAnd().getValues()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		results = rec.getList(binName);
		i = 0;

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(17L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(10L, list.get(0));
	}

	@Test
	public void operateMapPartial() {
		Key key = args.set.id("operateMapPartial");

        session.delete(key).execute();

		Map<Integer,Integer> inputMap = new HashMap<>();
		inputMap.put(0, 17);
		inputMap.put(4, 2);
		inputMap.put(5, 15);
		inputMap.put(9, 10);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .bin("bin2").mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		Map<Integer,Integer> sourceMap = new HashMap<>();
		sourceMap.put(3, 3);
		sourceMap.put(5, 15);

		rs = session.upsert(key)
		    .bin(binName).mapInsertItems(sourceMap, opt -> opt
		    	.allowPartial()
		    	.allowFailures())
		    .bin("bin2").mapInsertItems(sourceMap, opt -> opt
		    	.allowFailures())
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		long size = rec.getLong(binName);
		assertEquals(5L, size);

		size = rec.getLong("bin2");
		assertEquals(4L, size);
	}

	@Test
	public void operateMapInfinity() {
		Key key = args.set.id("operateMapInfinity");

        session.delete(key).execute();

		Map<Integer,Integer> inputMap = new HashMap<>();
		inputMap.put(0, 17);
		inputMap.put(4, 2);
		inputMap.put(5, 15);
		inputMap.put(9, 10);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		rs = session.upsert(key)
		    .bin(binName).onMapKeyRange(5, SpecialValue.INFINITY).getKeys()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		// record = client.operate(null, key,
		// 		MapOperation.getByKeyRange(binName, 5, Value.INFINITY, MapReturnType.KEY)
		// 		);

		// assertRecordFound(key, rec);
		//System.out.println("Record: " + record);

		List<?> results = rec.getList(binName);
		int i = 0;

		long v = (Long)results.get(i++);
		assertEquals(5L, v);

		v = (Long)results.get(i++);
		assertEquals(9L, v);
	}

	@Test
	public void operateMapWildcard() {
		Key key = args.set.id("operateMapWildcard");

        session.delete(key).execute();

		List<Object> i1 = new ArrayList<>();
		i1.add("John");
		i1.add(55);

		List<Object> i2 = new ArrayList<>();
		i2.add("Jim");
		i2.add(95);

		List<Object> i3 = new ArrayList<>();
		i3.add("Joe");
		i3.add(80);

		Map<Integer,List<Object>> inputMap = new HashMap<>();
		inputMap.put(4, i1);
		inputMap.put(5, i2);
		inputMap.put(9, i3);

		// Write values to empty map.
		RecordStream rs = session.upsert(key)
		    .bin(binName).mapUpsertItems(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<Object> filterList = new ArrayList<>();
		filterList.add("Joe");
		filterList.add(SpecialValue.WILDCARD);

		rs = session.upsert(key)
		    .bin(binName).onMapValue(filterList).getKeys()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long v = (Long)results.get(i++);
		assertEquals(9L, v);
	}

	@Test
	public void operateNestedMap() {
		Key key = args.set.id("operateNestedMap");

        session.delete(key).execute();

		Map<String,Integer> m1 = new HashMap<>();
		m1.put("key11", 9);
		m1.put("key12", 4);

		Map<String,Integer> m2 = new HashMap<>();
		m2.put("key21", 3);
		m2.put("key22", 5);

		Map<String,Map<String,Integer>> inputMap = new HashMap<>();
		inputMap.put("key1", m1);
		inputMap.put("key2", m2);

		// Create maps.
		RecordStream rs = session.upsert(key)
		    .bin(binName).setTo(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		// Set map value to 11 for map key "key21" inside of map key "key2"
		// and retrieve all maps.
		rs = session.upsert(key)
		    .bin(binName).onMapKey("key2").onMapKey("key21").upsert(11)
		    .bin(binName).get()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(2, count);

		Map<?,?> map = (Map<?,?>)results.get(i++);
		assertEquals(2, map.size());

		map = (Map<?,?>)map.get("key2");
		long v = (Long)map.get("key21");
		assertEquals(11, v);
		v = (Long)map.get("key22");
		assertEquals(5, v);
	}

	@Test
	public void operateDoubleNestedMap() {
		Key key = args.set.id("operateDoubleNestedMaps");

        session.delete(key).execute();

		Map<String,Integer> m11 = new HashMap<>();
		m11.put("key111", 1);

		Map<String,Integer> m12 = new HashMap<>();
		m12.put("key121", 5);

		Map<String,Map<String,Integer>> m1 = new HashMap<>();
		m1.put("key11", m11);
		m1.put("key12", m12);

		Map<String,Integer> m21 = new HashMap<>();
		m21.put("key211", 7);

		Map<String,Map<String,Integer>> m2 = new HashMap<>();
		m2.put("key21", m21);

		Map<String,Map<String,Map<String,Integer>>> inputMap = new HashMap<>();
		inputMap.put("key1", m1);
		inputMap.put("key2", m2);

		// Create maps.
		RecordStream rs = session.upsert(key)
		    .bin(binName).setTo(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		// Set map value to 11 for map key "key21" inside of map key "key2"
		// and retrieve all maps.
		rs = session.upsert(key)
		    .bin(binName).onMapKey("key1").onMapRank(-1).onMapKey("key121").upsert(11)
		    .bin(binName).get()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(1, count);

		Map<?,?> map = (Map<?,?>)results.get(i++);
		assertEquals(2, map.size());

		map = (Map<?,?>)map.get("key1");
		assertEquals(2, map.size());

		map = (Map<?,?>)map.get("key12");
		assertEquals(1, map.size());

		long v = (Long)map.get("key121");
		assertEquals(11, v);
	}

	@Test
	public void operateNestedMapValue() {
		Key key = args.set.id("operateNestedMapValue");

        session.delete(key).execute();

		Map<Integer,String> m1 = new HashMap<>();
		m1.put(1, "in");
		m1.put(3, "order");
		m1.put(2, "key");

		AerospikeMap<String,Map<Integer,String>> inputMap = new AerospikeMap<>(AerospikeMap.Type.ORDERED, 10);
		inputMap.put("first", m1);

		// Create nested maps that are all sorted and lookup by map value.
		RecordStream rs = session.upsert(key)
		    .bin(binName).setTo(inputMap)
		    .bin(binName).onMapKey("first").setTo(m1)
		    .bin(binName).onMapValue(m1).onMapKey(3).getAsOrderedMap()
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		Object obj = results.get(i++);
		assertNull(obj);

		long count = (Long)results.get(i++);
		assertEquals(1, count);

		AerospikeMap<?,?> map = (AerospikeMap<?,?>)results.get(i++);
		assertEquals(1, map.size());
		assertEquals("order", map.get(3L));
	}

	@Test
	public void operateMapCreateContext() {
		Key key = args.set.id("operateMapCreateContext");

        session.delete(key).execute();

		Map<String,Integer> m1 = new HashMap<>();
		m1.put("key11", 9);
		m1.put("key12", 4);

		Map<String,Integer> m2 = new HashMap<>();
		m2.put("key21", 3);
		m2.put("key22", 5);

		Map<String,Map<String,Integer>> inputMap = new HashMap<>();
		inputMap.put("key1", m1);
		inputMap.put("key2", m2);

		// Create maps.
		RecordStream rs = session.upsert(key)
		    .bin(binName).setTo(inputMap)
		    .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		// Set map value to 11 for map key "key21" inside of map key "key2"
		// and retrieve all maps.
		rs = session.upsert(key)
		    .bin(binName).onMapKey("key3").mapCreate(MapOrder.KEY_ORDERED)
		    .bin(binName).onMapKey("key3").onMapKey("key31").upsert(99)
		    .bin(binName).get()
		    .execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 1;

		long count = (Long)results.get(i++);
		assertEquals(1, count);

		Map<?,?> map = (Map<?,?>)results.get(i++);
		assertEquals(3, map.size());

		map = (Map<?,?>)map.get("key3");
		long v = (Long)map.get("key31");
		assertEquals(99, v);
	}
}
