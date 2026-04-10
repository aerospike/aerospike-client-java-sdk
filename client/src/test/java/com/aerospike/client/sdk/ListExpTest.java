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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListPolicy;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.exp.MapExp;

public class ListExpTest extends ClusterTest {
	String binA = "A";
	String binB = "B";
	String binC = "C";

	Key keyA = args.set.id("A");
	Key keyB = args.set.id("B");

	@BeforeEach
	public void setUp() throws Exception {
        session.delete(keyA, keyB).execute();
	}

	@Test
	public void modifyWithContext() {
		List<String> listSubA = new ArrayList<>();
		listSubA.add("e");
		listSubA.add("d");
		listSubA.add("c");
		listSubA.add("b");
		listSubA.add("a");

		List<Object> listA = new ArrayList<>();
		listA.add("a");
		listA.add("b");
		listA.add("c");
		listA.add("d");
		listA.add(listSubA);

		List<String> listB = new ArrayList<>();
		listB.add("x");
		listB.add("y");
		listB.add("z");

		RecordStream rs = session.upsert(keyA)
	        .bin(binA).listAppendItems(listA)
	        .bin(binB).listAppendItems(listB)
	        .bin(binC).setTo("M")
	        .execute();

		assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		// TODO Port expression to AEL.
        CTX ctx = CTX.listIndex(4);

		Expression where = Exp.build(
			Exp.eq(
				ListExp.size(
					// Temporarily append binB/binC to binA in expression.
					ListExp.appendItems(ListPolicy.Default, Exp.listBin(binB),
						ListExp.append(ListPolicy.Default, Exp.stringBin(binC), Exp.listBin(binA), ctx),
						ctx),
					ctx),
				Exp.val(9)));

		rs = session.query(keyA)
			.readingOnlyBins(binA)
			.where(where)
	        .failOnFilteredOut()
			.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		List<?> result = rec.getList(binA);
		assertEquals(5, result.size());

		where = Exp.build(
			Exp.eq(
				ListExp.size(
					// Temporarily append local listB and local "M" string to binA in expression.
					ListExp.appendItems(ListPolicy.Default, Exp.val(listB),
						ListExp.append(ListPolicy.Default, Exp.val("M"), Exp.listBin(binA), ctx),
						ctx),
					ctx),
				Exp.val(9)));

		rs = session.query(keyA)
			.readingOnlyBins(binA)
			.where(where)
	        .failOnFilteredOut()
			.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		result = rec.getList(binA);
		assertEquals(5, result.size());
	}

	@Test
	public void listExpressionFilterMapElementInListBin() {
		Key keyMatch = args.set.id("listMapFilter");
		Key keyFiltered = args.set.id("listMapFilterNoMatch");

		session.delete(keyMatch).execute();
		session.delete(keyFiltered).execute();

		List<Map<String, Object>> listOfMaps = List.of(
				Map.of("name", "alice", "age", 30),
				Map.of("name", "bob", "age", 25)
		);

		session.upsert(keyMatch)
				.bin(binA).setTo(listOfMaps)
				.execute();

		List<Map<String, Object>> listNoMatch = List.of(
				Map.of("name", "charlie", "age", 40),
				Map.of("name", "dave", "age", 35)
		);

		session.upsert(keyFiltered)
			.bin(binA).setTo(listNoMatch)
			.execute();

		// Filter: get "name" from map at list index 0, check if it equals "alice".
		Expression filter = Exp.build(
				Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val("name"),
						ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.MAP, Exp.val(0), Exp.listBin(binA))),
						Exp.val("alice")
				));

		RecordStream rs = session.query(List.of(keyMatch, keyFiltered))
			.where(filter)
			.failOnFilteredOut()
			.execute();

		assertTrue(rs.hasNext());
		RecordResult match = rs.next();
		assertEquals(ResultCode.OK, match.resultCode());
		Record rec = match.recordOrThrow();
		List<?> result = rec.getList(binA);
		assertNotNull(result);
		assertEquals(2, result.size());

		assertTrue(rs.hasNext());
		assertEquals(ResultCode.FILTERED_OUT, rs.next().resultCode());
		assertFalse(rs.hasNext());
	}

	@Test
	public void listExpressionWithReturnTypeIndex() {
		Key key = args.set.id("listRetIndex");

		session.delete(key).execute();

		List<Integer> list = List.of(10, 20, 30, 40);

		session.upsert(key)
			.bin(binA).setTo(list)
			.execute();

		// Use ListReturnType.INDEX to get the index of value 20.
		Expression readExp = Exp.build(
				ListExp.getByValue(ListReturnType.INDEX, Exp.val(20), Exp.listBin(binA)));

		RecordStream rs = session.query(key)
				.bin(binA).selectFrom(readExp)
				.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		List<?> indices = rec.getList(binA);
		assertNotNull(indices);
		assertTrue(indices.contains(1L), "Expected index 1 for value 20");
	}

	@Test
	public void relativeRankListExpressionOrder() {
		Key key = args.set.id("relRank");

		session.delete(key).execute();

		List<Integer> list = List.of(10, 20, 30, 40);

		session.upsert(key)
			.bin(binA).setTo(list)
			.execute();

		// Get 2 elements starting from relative rank 0 of value 20.
		// Should return 20 (rank 0) and 30 (rank 1).
		Expression readExp = Exp.build(
				ListExp.getByValueRelativeRankRange(
						ListReturnType.VALUE,
						Exp.val(20),
						Exp.val(0),
						Exp.val(2),
						Exp.listBin(binA)
				)
		);

		RecordStream rs = session.query(key)
				.bin(binA)
				.selectFrom(readExp)
				.execute();

		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		List<?> result = rec.getList(binA);
		assertNotNull(result);
		assertEquals(2, result.size());
		assertTrue(result.contains(20L));
		assertTrue(result.contains(30L));
	}

	@Test
	public void expReturnsList() {
		List<String> list = new ArrayList<>();
		list.add("a");
		list.add("b");
		list.add("c");
		list.add("d");

		Expression exp = Exp.build(Exp.val(list));

		RecordStream rs = session.upsert(keyA)
        	.bin(binC).upsertFrom(exp)
        	.bin(binC).get()
	        .bin("var").selectFrom(exp)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binC);
		assertEquals(2, results.size());

		List<?> rlist = (List<?>)results.get(1);
		assertEquals(4, rlist.size());

		List<?> results2 = rec.getList("var");
		assertEquals(4, results2.size());
	}
}
