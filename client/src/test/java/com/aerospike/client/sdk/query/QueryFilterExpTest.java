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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.exp.BitExp;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.exp.MapExp;
import com.aerospike.client.sdk.info.classes.IndexType;

public class QueryFilterExpTest extends ClusterTest {
	private static final String setName = args.set + "flt";
	private static final String indexName = "flt";
	private static final String keyPrefix = "flt";
	private static final String binName = "fltint";
	private static final int size = 50;

	private static DataSet dataSet;

	@BeforeAll
	public static void prepare() {
		dataSet = DataSet.of(args.namespace, setName);

		// Clean up any existing test data
		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;
			session.delete(dataSet.ids(key));
		}

		try {
			session.createIndex(dataSet, indexName, binName, IndexType.INTEGER, IndexCollectionType.DEFAULT)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;

			List<Integer> list = null;
			Map<String,String> map = null;

			if (i == 1) {
				list = new ArrayList<Integer>(5);
				list.add(1);
				list.add(2);
				list.add(4);
				list.add(9);
				list.add(20);
			}
			else if (i == 2) {
				list = new ArrayList<Integer>(3);
				list.add(5);
				list.add(9);
				list.add(100);
			}
			else if (i == 3) {
				map = new HashMap<String,String>();
				map.put("A", "AAA");
				map.put("B", "BBB");
				map.put("C", "BBB");
			}
			else {
				list = new ArrayList<Integer>(0);
				map = new HashMap<String,String>(0);
			}
			session.upsert(dataSet.ids(key))
				.bins(binName, "bin2", "listbin", "mapbin")
				.values(i, i, list, map)
				.execute();
		}
	}

	@AfterAll
	public static void destroy() {
		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;
			session.delete(dataSet.ids(key));
		}
		session.dropIndex(dataSet, indexName);
	}

	@Test
	public void queryAndOr() {
		int begin = 10;
		int end = 45;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.or(
				Exp.and(
					Exp.gt(Exp.intBin("bin2"), Exp.val(40)),
					Exp.lt(Exp.intBin("bin2"), Exp.val(44))),
				Exp.eq(Exp.intBin("bin2"), Exp.val(22)),
				Exp.eq(Exp.intBin("bin2"), Exp.val(9))),
			Exp.eq(Exp.intBin(binName), Exp.intBin("bin2")));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(4, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryNot() {
		int begin = 10;
		int end = 45;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.not(
				Exp.and(
					Exp.ge(Exp.intBin("bin2"), Exp.val(15)),
					Exp.le(Exp.intBin("bin2"), Exp.val(42)))));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(8, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryLastUpdate() {
		int begin = 10;
		int end = 45;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.gt(Exp.lastUpdate(), Exp.val(System.currentTimeMillis() * 1000000L + 100)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			while (rs.hasNext()) {
				rs.next();
			}
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryList1() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.gt(ListExp.getByValue(ListReturnType.COUNT, Exp.val(4), Exp.listBin("listbin")), Exp.val(0)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryList2() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(ListExp.getByValue(ListReturnType.COUNT, Exp.val(5), Exp.listBin("listbin")), Exp.val(0)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(8, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryList3() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(4), Exp.listBin("listbin")), Exp.val(20)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryMap1() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.gt(MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val("B"), Exp.mapBin("mapbin")), Exp.val(0)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryMap2() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			MapExp.getByValue(MapReturnType.EXISTS, Exp.val("BBB"), Exp.mapBin("mapbin")));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryMap3() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val("D"), Exp.mapBin("mapbin")), Exp.val(0)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(8, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryMap4() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(MapExp.getByValue(MapReturnType.COUNT, Exp.val("AAA"), Exp.mapBin("mapbin")), Exp.val(0)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(7, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryMap5() {
		int begin = 1;
		int end = 10;

		List<String> list = new ArrayList<String>();
		list.add("A");
		list.add("C");

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(MapExp.size(MapExp.getByKeyList(MapReturnType.KEY_VALUE, Exp.val(list), Exp.mapBin("mapbin"))), Exp.val(2)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryMap6() {
		int begin = 1;
		int end = 10;

		List<String> list = new ArrayList<String>();
		list.add("A");
		list.add("C");

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(ListExp.size(MapExp.getByKeyList(MapReturnType.VALUE, Exp.val(list), Exp.mapBin("mapbin"))), Exp.val(2)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryDigestModulo() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(Exp.digestModulo(3), Exp.val(1)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(3, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryBinExists() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.binExists("bin2"));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(10, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryBinType() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.eq(Exp.binType("listbin"), Exp.val(ParticleType.LIST)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(9, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryRecordSize() {
		int begin = 1;
		int end = 10;

		Exp filterExp = Exp.and(
			Exp.ge(Exp.intBin(binName), Exp.val(begin)),
			Exp.le(Exp.intBin(binName), Exp.val(end)),
			Exp.ge(Exp.recordSize(), Exp.val(0)));

		RecordStream rs = session.query(dataSet)
			.where(filterExp)
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				rs.next();
				count++;
			}

			assertEquals(10, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryInvalidFilterExpression() {
		Expression invalidFilter = Exp.build(Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8), Exp.intBin(binName)),
				Exp.val(new byte[] {0})
		));

		AerospikeException ae = assertThrows(AerospikeException.class, () ->
				session.query(dataSet.id(keyPrefix+"1"))
						.where(invalidFilter)
						.execute());
		assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
	}
}
