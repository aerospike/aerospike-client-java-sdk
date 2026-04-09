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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListSortFlags;

public class OperateListTest extends ClusterTest {
	private static final String binName = "oplistbin";

	@Test
	public void operateList1() {
		Key key = args.set.id("operateList1");

        session.delete(key).execute();

		// Calling append() multiple times performs poorly because the server makes
		// a copy of the list for each call, but we still need to test it.
		// Using appendItems() should be used instead for best performance.
        RecordStream rs = session.upsert(key)
        	.bin(binName).listAppend(55)
        	.bin(binName).listAppend(77)
        	.bin(binName).listPop(-1)
        	.bin(binName).listSize()
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		List<?> list = rec.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(1, size);

		size = (Long)list.get(1);
		assertEquals(2, size);

		long val = (Long)list.get(2);
		assertEquals(77, val);

		size = (Long)list.get(3);
		assertEquals(1, size);
	}

	@Test
	public void operateList2() {
		Key key = args.set.id("operateList2");

        session.delete(key).execute();

		List<Object> itemList = new ArrayList<>();
		itemList.add(12);
		itemList.add(-8734);
		itemList.add("my string");

        RecordStream rs = session.upsert(key)
        	.bin(binName).listAppendItems(itemList)
        	.bin("otherbin").setTo("hello")
        	.execute();

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

        rs = session.upsert(key)
        	.bin(binName).listInsert(-1, 8)
        	.bin("otherbin").append("goodbye")
        	.bin("otherbin").get()
        	.bin(binName).listGetRange(0, 4)
        	.bin(binName).listGetRange(3)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		List<?> list = rec.getList("otherbin");

		String val = (String)list.get(1);
		assertEquals("hellogoodbye", val);

		list = rec.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(4, size);

		List<?> rangeList = (List<?>)list.get(1);
		long lval = (Long)rangeList.get(0);
		assertEquals(12, lval);

		lval = (Long)rangeList.get(1);
		assertEquals(-8734, lval);

		lval = (Long)rangeList.get(2);
		assertEquals(8, lval);

		val = (String)rangeList.get(3);
		assertEquals("my string", val);

		rangeList = (List<?>)list.get(2);
		val = (String)rangeList.get(0);
		assertEquals("my string", val);
	}

	@Test
	public void operateList3() {
		// Test out of bounds conditions
		Key key = args.set.id("operateList3");

        session.delete(key).execute();

		List<String> itemList = new ArrayList<>();
		itemList.add("str1");
		itemList.add("str2");
		itemList.add("str3");
		itemList.add("str4");
		itemList.add("str5");
		itemList.add("str6");
		itemList.add("str7");

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).listGet(2)
        	.bin(binName).listGetRange(6, 4)
        	.bin(binName).listGetRange(-7, 3)
        	.bin(binName).listGetRange(0, 2)
        	.bin(binName).listGetRange(-2, 4)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		List<?> list = rec.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(7, size);

		assertEquals("str3", list.get(1));

		List<?> rangeList = (List<?>)list.get(2);
		assertEquals(1, rangeList.size());
		assertEquals("str7", rangeList.get(0));

		rangeList = (List<?>)list.get(3);
		assertEquals(3, rangeList.size());
		assertEquals("str1", rangeList.get(0));
		assertEquals("str2", rangeList.get(1));
		assertEquals("str3", rangeList.get(2));

		rangeList = (List<?>)list.get(4);
		assertEquals(2, rangeList.size());
		assertEquals("str1", rangeList.get(0));
		assertEquals("str2", rangeList.get(1));

		rangeList = (List<?>)list.get(5);
		assertEquals(2, rangeList.size());
		assertEquals("str6", rangeList.get(0));
		assertEquals("str7", rangeList.get(1));
	}

	@Test
	public void operateList4() {
		// Test all value types.
		Key key = args.set.id("operateList4");

        session.delete(key).execute();

		List<Object> inputList = new ArrayList<>();
		inputList.add(12);
		inputList.add(-8734.81);
		inputList.add("my string");

		Map<Integer,String> inputMap = new HashMap<>();
		inputMap.put(9, "data 9");
		inputMap.put(-2, "data -2");

		byte[] bytes = "string bytes".getBytes();

		List<Object> itemList = new ArrayList<>();
		itemList.add(true);
		itemList.add(55);
		itemList.add("string value");
		itemList.add(inputList);
		itemList.add(bytes);
		itemList.add(99.99);
		itemList.add(inputMap);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).listGetRange(0, 100)
        	.bin(binName).listSet(1, "88")
        	.bin(binName).listGet(1)
        	.bin(binName).listPopRange(-2, 1)
        	.bin(binName).listPopRange(-1)
        	.bin(binName).listRemove(3)
        	.bin(binName).listRemoveRange(0, 1)
        	.bin(binName).listRemoveRange(2)
        	.bin(binName).listSize()
        	.execute();

        assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();

		List<?> list = rec.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(7, size);

		List<?> rangeList = (List<?>)list.get(1);
		assertTrue((Boolean)rangeList.get(0));
		assertEquals(55, (long)(Long)rangeList.get(1));
		assertEquals("string value", rangeList.get(2));

		List<?> subList = (List<?>)rangeList.get(3);
		assertEquals(3, subList.size());
		assertEquals(12, (long)(Long)subList.get(0));
		assertEquals(-8734.81, (Double)subList.get(1), 0.00001);
		assertEquals("my string", subList.get(2));

		byte[] bt = (byte[])rangeList.get(4);
		assertArrayEquals(bytes, bt, "bytes not equal");

		assertEquals(99.99, (Double)rangeList.get(5), 0.00001);

		Map<?,?> subMap = (Map<?,?>)rangeList.get(6);
		assertEquals(2, subMap.size());
		assertEquals("data 9", subMap.get(9L));
		assertEquals("data -2", subMap.get(-2L));

		assertEquals("88", list.get(3));

		subList = (List<?>)list.get(4);
		assertEquals(1, subList.size());
		assertEquals(99.99, (Double)subList.get(0), 0.00001);

		subList = (List<?>)list.get(5);
		assertEquals(1, subList.size());
		assertTrue(subList.get(0) instanceof Map);

		assertEquals(1, (long)(Long)list.get(6));
		assertEquals(1, (long)(Long)list.get(7));
		assertEquals(1, (long)(Long)list.get(8));

		size = (Long)list.get(9);
		assertEquals(2, size);
	}

	@Test
	public void operateList5() {
		// Test trim.
		Key key = args.set.id("operateList5");

        session.delete(key).execute();

		List<String> itemList = new ArrayList<>();
		itemList.add("s11");
		itemList.add("s22222");
		itemList.add("s3333333");
		itemList.add("s4444444444");
		itemList.add("s5555555555555555");

		RecordStream rs = session.upsert(key)
	        .bin(binName).listInsertItems(0, itemList)
        	.bin(binName).listTrim(-5, 5)
        	.bin(binName).listTrim(1, -5)
        	.bin(binName).listTrim(1, 2)
        	.execute();

        assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();

		List<?> list = rec.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(5, size);

		size = (Long)list.get(1);
		assertEquals(0, size);

		size = (Long)list.get(2);
		assertEquals(1, size);

		size = (Long)list.get(3);
		assertEquals(2, size);
	}

	@Test
	public void operateList6() {
		// Test clear.
		Key key = args.set.id("operateList6");

        session.delete(key).execute();

		List<String> itemList = new ArrayList<>();
		itemList.add("s11");
		itemList.add("s22222");
		itemList.add("s3333333");
		itemList.add("s4444444444");
		itemList.add("s5555555555555555");

		RecordStream rs = session.upsert(key)
	        .bin("otherbin").setTo(11)
        	.bin("otherbin").get()
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).listClear()
        	.bin(binName).listSize()
        	.execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> list = rec.getList("otherbin");
		assertEquals(2, list.size());
		assertNull(list.get(0));
		assertEquals(11, (long)(Long)list.get(1));

		list = rec.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(5, size);

		// clear() does not return value by default, but we set respondAllOps, so it returns null.
		assertNull(list.get(1));

		size = (Long)list.get(2);
		assertEquals(0, size);
	}

	@Test
	public void operateList7() {
		// Test null values.
		Key key = args.set.id("operateList7");

        session.delete(key).execute();

		List<String> itemList = new ArrayList<>();
		itemList.add("s11");
		itemList.add(null);
		itemList.add("s3333333");

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).listGet(0)
        	.bin(binName).listGet(1)
        	.bin(binName).listGet(2)
        	.execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(3, size);

		String str = (String)results.get(i++);
		assertEquals("s11", str);

		str = (String)results.get(i++);
		assertNull(str);

		str = (String)results.get(i++);
		assertEquals("s3333333", str);
	}

	@Test
	public void operateList8() {
		// Test increment.
		Key key = args.set.id("operateList8");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(1);
		itemList.add(2);
		itemList.add(3);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).listIncrement(2)
        	.bin(binName).listIncrement(2)
        	.bin(binName).listIncrement(1, 7)
        	.bin(binName).listIncrement(1, 7)
        	.bin(binName).listGet(0)
        	.execute();

        assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(3, size);

		long val = (Long)results.get(i++);
		assertEquals(4, val);

		val = (Long)results.get(i++);
		assertEquals(5, val);

		val = (Long)results.get(i++);
		assertEquals(9, val);

		val = (Long)results.get(i++);
		assertEquals(16, val);

		val = (Long)results.get(i++);
		assertEquals(1, val);
	}

	@Test
	public void operateListSwitchSort() {
		Key key = args.set.id("operateListSwitchSort");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(4);
		itemList.add(3);
		itemList.add(1);
		itemList.add(5);
		itemList.add(2);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).onListIndex(3).getValues()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(5L, size);

		long val = (Long)results.get(i++);
		assertEquals(5L, val);

		List<Integer> valueList = new ArrayList<>();
		valueList.add(4);
		valueList.add(2);

		// Sort list.
		rs = session.upsert(key)
	        .bin(binName).listSetOrder(ListOrder.ORDERED)
        	.bin(binName).onListValue(3).getIndexes()
        	.bin(binName).onListValueRange(-1, 3).count()
        	.bin(binName).onListValueRange(-1, 3).exists()
        	.bin(binName).onListValueList(valueList).getRanks()
        	.bin(binName).onListIndex(3).getValues()
        	.bin(binName).onListIndexRange(-2, 2).getValues()
        	.bin(binName).onListRank(0).getValues()
        	.bin(binName).onListRankRange(2, 3).getValues()
       	.execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		results = rec.getList(binName);
		i = 1;

		List<?> list = (List<?>)results.get(i++);
		assertEquals(2L, list.get(0));

		val = (Long)results.get(i++);
		assertEquals(2L, val);

		boolean b = (Boolean)results.get(i++);
		assertTrue(b);

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(3L, list.get(0));
		assertEquals(1L, list.get(1));

		val = (Long)results.get(i++);
		assertEquals(4L, val);

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));

		val = (Long)results.get(i++);
		assertEquals(1L, val);

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(3L, list.get(0));
		assertEquals(4L, list.get(1));
		assertEquals(5L, list.get(2));
	}

	@Test
	public void operateListSort() {
		Key key = args.set.id("operateListSort");

        session.delete(key).execute();

        List<Integer> itemList = new ArrayList<>();
		itemList.add(-44);
		itemList.add(33);
		itemList.add(-1);
		itemList.add(33);
		itemList.add(-2);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).listSort(ListSortFlags.DROP_DUPLICATES)
        	.bin(binName).listSize()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(5L, size);

		Object obj = results.get(i++);
		assertNull(obj);

		long val = (Long)results.get(i++);
		assertEquals(4L, val);
	}

	@Test
	public void operateListRemove() {
		Key key = args.set.id("operateListRemove");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(-44);
		itemList.add(33);
		itemList.add(-1);
		itemList.add(33);
		itemList.add(-2);
		itemList.add(0);
		itemList.add(22);
		itemList.add(11);
		itemList.add(14);
		itemList.add(6);

		List<Integer> valueList = new ArrayList<>();
		valueList.add(-45);
		valueList.add(14);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).onListValue(0).removeAnd().getIndexes()
        	.bin(binName).onListValueList(valueList).removeAnd().getValues()
        	.bin(binName).onListValueRange(33, 100).removeAnd().getValues()
        	.bin(binName).onListIndex(1).removeAnd().getValues()
        	.bin(binName).onListIndexRange(100, 101).removeAnd().getValues()
        	.bin(binName).onListRank(0).removeAnd().getValues()
        	.bin(binName).onListRankRange(3, 1).removeAnd().getValues()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(10L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(5L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(14L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(33L, list.get(0));
		assertEquals(33L, list.get(1));

		long val = (Long)results.get(i++);
		assertEquals(-1L, val);

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		val = (Long)results.get(i++);
		assertEquals(-44L, val);

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(22L, list.get(0));
	}

	@Test
	public void operateListInverted() {
		Key key = args.set.id("operateListInverted");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(4);
		itemList.add(3);
		itemList.add(1);
		itemList.add(5);
		itemList.add(2);

		List<Integer> valueList = new ArrayList<>();
		valueList.add(4);
		valueList.add(2);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).onListValue(3).getAllOtherIndexes()
        	.bin(binName).onListValueRange(-1, 3).countAllOthers()
        	.bin(binName).onListValueList(valueList).getAllOtherRanks()
        	.bin(binName).onListIndexRange(-2, 2).getAllOtherValues()
        	.bin(binName).onListRankRange(2, 3).getAllOtherValues()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(5L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(2L, list.get(1));
		assertEquals(3L, list.get(2));
		assertEquals(4L, list.get(3));

		long val = (Long)results.get(i++);
		assertEquals(3L, val);

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(2L, list.get(1));
		assertEquals(4L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(3L, list.get(1));
		assertEquals(1L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(1L, list.get(0));
		assertEquals(2L, list.get(1));
	}

	@Test
	public void operateListGetRelative() {
		Key key = args.set.id("operateListGetRelative");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(0);
		itemList.add(4);
		itemList.add(5);
		itemList.add(9);
		itemList.add(11);
		itemList.add(15);

		RecordStream rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).onListValueRelativeRankRange(5, 0).getValues()
        	.bin(binName).onListValueRelativeRankRange(5, 1).getValues()
        	.bin(binName).onListValueRelativeRankRange(5, -1).getValues()
        	.bin(binName).onListValueRelativeRankRange(3, 0).getValues()
        	.bin(binName).onListValueRelativeRankRange(3, 3).getValues()
        	.bin(binName).onListValueRelativeRankRange(3, -3).getValues()
        	.bin(binName).onListValueRelativeRankRange(5, 0, 2).getValues()
        	.bin(binName).onListValueRelativeRankRange(5, 1, 1).getValues()
        	.bin(binName).onListValueRelativeRankRange(5, -1, 2).getValues()
        	.bin(binName).onListValueRelativeRankRange(3, 0, 1).getValues()
        	.bin(binName).onListValueRelativeRankRange(3, 3, 7).getValues()
        	.bin(binName).onListValueRelativeRankRange(3, -3, 2).getValues()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 1;

		long size = (Long)results.get(i++);
		assertEquals(6L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));
		assertEquals(11L, list.get(2));
		assertEquals(15L, list.get(3));

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(9L, list.get(0));
		assertEquals(11L, list.get(1));
		assertEquals(15L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(5L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));
		assertEquals(9L, list.get(2));
		assertEquals(11L, list.get(3));
		assertEquals(15L, list.get(4));

		list = (List<?>)results.get(i++);
		assertEquals(5L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));
		assertEquals(9L, list.get(2));
		assertEquals(11L, list.get(3));
		assertEquals(15L, list.get(4));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(11L, list.get(0));
		assertEquals(15L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(6L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(4L, list.get(1));
		assertEquals(5L, list.get(2));
		assertEquals(9L, list.get(3));
		assertEquals(11L, list.get(4));
		assertEquals(15L, list.get(5));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(9L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(4L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(11L, list.get(0));
		assertEquals(15L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());
	}

	@Test
	public void operateListRemoveRelative() {
		Key key = args.set.id("operateListRemoveRelative");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(0);
		itemList.add(4);
		itemList.add(5);
		itemList.add(9);
		itemList.add(11);
		itemList.add(15);

		RecordStream rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED)
	        .bin(binName).listAppendItems(itemList)
        	.bin(binName).onListValueRelativeRankRange(5, 0).removeAnd().getValues()
        	.bin(binName).onListValueRelativeRankRange(5, 1).removeAnd().getValues()
        	.bin(binName).onListValueRelativeRankRange(5, -1).removeAnd().getValues()
        	.bin(binName).onListValueRelativeRankRange(3, -3, 1).removeAnd().getValues()
        	.bin(binName).onListValueRelativeRankRange(3, -3, 2).removeAnd().getValues()
        	.bin(binName).onListValueRelativeRankRange(3, -3, 3).removeAnd().getValues()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 1;

		long size = (Long)results.get(i++);
		assertEquals(6L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));
		assertEquals(11L, list.get(2));
		assertEquals(15L, list.get(3));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(4L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(0L, list.get(0));
	}

	@Test
	public void operateListPartial() {
		Key key = args.set.id("operateListPartial");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(0);
		itemList.add(4);
		itemList.add(5);
		itemList.add(9);
		itemList.add(9);
		itemList.add(11);
		itemList.add(15);
		itemList.add(0);

		RecordStream rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED)
	        .bin(binName).listAppendItems(itemList, opt -> opt
        		.addUnique()
        		.allowPartial()
        		.allowFailures())
	        .bin("bin2").listAppendItems(itemList, opt -> opt
	        	.addUnique()
        		.allowFailures())
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		long size = (long)results.get(1);
		assertEquals(6L, size);

		size = rec.getLong("bin2");
		assertEquals(0L, size);

		itemList = new ArrayList<>();
		itemList.add(11);
		itemList.add(3);

		rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED)
	        .bin(binName).listAppendItems(itemList, opt -> opt
        		.addUnique()
        		.allowPartial()
        		.allowFailures())
	        .bin("bin2").listAppendItems(itemList, opt -> opt
	        	.addUnique()
        		.allowFailures())
        	.execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		results = rec.getList(binName);
		size = (long)results.get(1);
		assertEquals(7L, size);

		size = rec.getLong("bin2");
		assertEquals(2L, size);
	}

	@Test
	public void operateListInfinity() {
		Key key = args.set.id("operateListInfinity");

        session.delete(key).execute();

		List<Integer> itemList = new ArrayList<>();
		itemList.add(0);
		itemList.add(4);
		itemList.add(5);
		itemList.add(9);
		itemList.add(11);
		itemList.add(15);

		RecordStream rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED)
	        .bin(binName).listAppendItems(itemList)
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		long size = (long)results.get(1);
		assertEquals(6L, size);

		itemList = new ArrayList<>();
		itemList.add(11);
		itemList.add(3);

		rs = session.upsert(key)
	        .bin(binName).onListValueRange(10, SpecialValue.INFINITY).getValues()
        	.execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		results = rec.getList(binName);
		int i = 0;

		long val = (Long)results.get(i++);
		assertEquals(11L, val);

		val = (Long)results.get(i++);
		assertEquals(15L, val);
	}

	@Test
	public void operateListWildcard() {
		Key key = args.set.id("operateListWildcard");

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

		List<List<Object>> itemList = new ArrayList<>();

		itemList.add(i1);
		itemList.add(i2);
		itemList.add(i3);

		RecordStream rs = session.upsert(key)
	        .bin(binName).listAppendItems(itemList)
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		long size = rec.getLong(binName);
		assertEquals(3L, size);

		List<Object> itemList2 = new ArrayList<>();
		itemList2.add("Jim");
		itemList2.add(Value.WILDCARD);

		rs = session.upsert(key)
	        .bin(binName).onListValue(itemList2).getValues()
        	.execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();
		//System.out.println("REC=" + rec);

		List<?> results = rec.getList(binName);
		int i = 0;

		List<?> items = (List<?>)results.get(i++);
		String s = (String)items.get(0);
		assertEquals("Jim", s);

		long v = (Long)items.get(1);
		assertEquals(95L, v);
	}

	@Test
	public void operateNestedList() {
		Key key = args.set.id("operateNestedList");

        session.delete(key).execute();

		List<Integer> l1 = new ArrayList<>();
		l1.add(7);
		l1.add(9);
		l1.add(5);

		List<Integer> l2 = new ArrayList<>();
		l2.add(1);
		l2.add(2);
		l2.add(3);

		List<Integer> l3 = new ArrayList<>();
		l3.add(6);
		l3.add(5);
		l3.add(4);
		l3.add(1);

		List<List<Integer>> inputList = new ArrayList<>();
		inputList.add(l1);
		inputList.add(l2);
		inputList.add(l3);

		// Create list.
		RecordStream rs = session.upsert(key)
			.bin(binName).setTo(inputList)
        	.execute();

		// Append value to last list and retrieve all lists.
		rs = session.upsert(key)
			.bin(binName).onListIndex(-1).listAppend(11)
			.bin(binName).get()
	        .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(5, count);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(3, list.size());

		// Test last nested list.
		list = (List<?>)list.get(2);
		assertEquals(5, list.size());
		assertEquals(6, (long)(Long)list.get(0));
		assertEquals(5, (long)(Long)list.get(1));
		assertEquals(4, (long)(Long)list.get(2));
		assertEquals(1, (long)(Long)list.get(3));
		assertEquals(11, (long)(Long)list.get(4));
	}

	@Test
	public void operateNestedListMap() {
		Key key = args.set.id("operateNestedListMap");

        session.delete(key).execute();

		List<Integer> l11 = new ArrayList<>();
		l11.add(7);
		l11.add(9);
		l11.add(5);

		List<Integer> l12 = new ArrayList<>();
		l12.add(13);

		List<List<Integer>> l1 = new ArrayList<>();
		l1.add(l11);
		l1.add(l12);

		List<Integer> l21 = new ArrayList<>();
		l21.add(9);

		List<Integer> l22 = new ArrayList<>();
		l22.add(2);
		l22.add(4);

		List<Integer> l23 = new ArrayList<>();
		l23.add(6);
		l23.add(1);
		l23.add(9);

		List<List<Integer>> l2 = new ArrayList<>();
		l2.add(l21);
		l2.add(l22);
		l2.add(l23);

		Map<String,List<List<Integer>>> inputMap = new HashMap<>();
		inputMap.put("key1", l1);
		inputMap.put("key2", l2);

		// Create map.
		RecordStream rs = session.upsert(key)
			.bin(binName).setTo(inputMap)
        	.execute();

		// Append value to last list and retrieve map.
		rs = session.upsert(key)
			.bin(binName).onMapKey("key2").onListRank(0).listAppend(11)
			.bin(binName).get()
	        .execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(3, count);

		Map<?,?> map = (Map<?,?>)results.get(i++);
		assertEquals(2, map.size());

		// Test affected nested list.
		List<?> list = (List<?>)map.get("key2");
		assertEquals(3, list.size());

		list = (List<?>)list.get(1);
		assertEquals(3, list.size());
		assertEquals(2, (long)(Long)list.get(0));
		assertEquals(4, (long)(Long)list.get(1));
		assertEquals(11, (long)(Long)list.get(2));
	}

	@Test
	public void operateListCreateContext() {
		Key key = args.set.id("operateListCreateContext");

        session.delete(key).execute();

		List<Integer> l1 = new ArrayList<>();
		l1.add(7);
		l1.add(9);
		l1.add(5);

		List<Integer> l2 = new ArrayList<>();
		l2.add(1);
		l2.add(2);
		l2.add(3);

		List<Integer> l3 = new ArrayList<>();
		l3.add(6);
		l3.add(5);
		l3.add(4);
		l3.add(1);

		List<List<Integer>> inputList = new ArrayList<>();
		inputList.add(l1);
		inputList.add(l2);
		inputList.add(l3);

		// Create list.
		RecordStream rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED)
	        .bin(binName).listAppendItems(inputList)
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		rs = session.upsert(key)
			.bin(binName).onListIndex(3, ListOrder.ORDERED, false).listAppend(2L)
			.bin(binName).get()
			.execute();

		assertTrue(rs.hasNext());
		rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(1, count);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4, list.size());

		// Test last nested list.
		list = (List<?>)list.get(1);
		assertEquals(1, list.size());
		assertEquals(2, (long)(Long)list.get(0));
	}

	@Test
	public void operateListCreate() {
		Key key = args.set.id("operateListCreate");

        session.delete(key).execute();

		List<Integer> l1 = new ArrayList<>();
		l1.add(3);
		l1.add(2);
		l1.add(1);

		// Create list with persisted index.
		RecordStream rs = session.upsert(key)
			.bin(binName).listSetOrder(ListOrder.ORDERED, true)
	        .bin(binName).listAppendItems(l1)
	        .bin(binName).get()
        	.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binName);
		assertEquals(3, results.size());  // 3 operations equals 3 results.

		int i = 0;
		Object obj = results.get(i++);
		assertNull(obj);

		long val = (Long)results.get(i++);
		assertEquals(3, val);  // appendItems returns 3 for number of items appended.

		List<?> list = (List<?>)results.get(i++);
		assertEquals(3, list.size());

		val = (Long)list.get(0);
		assertEquals(1, val);  // List is returned sorted, so 1 will be the first item.

		val = (Long)list.get(1);
		assertEquals(2, val);

		val= (Long)list.get(2);
		assertEquals(3, val);
	}
}
