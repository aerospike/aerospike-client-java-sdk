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
package com.aerospike.client.fluent.query;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.ClusterTest;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.command.Info;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.client.fluent.util.Version;

public class ExpSecondaryIndexTest extends ClusterTest {
	private static final DataSet dataSet = DataSet.of(args.set.getNamespace(), "exp_SI_test_set");
	private static final String indexName = "εχπ_ΣΙ_τεστ_ιδχ";
	private static final List<String> countries =  List.of("Australia", "Canada", "USA");
	private static final Expression exp = Exp.build(
		// IF (age >= 18 AND country IN ["Australia, "Canada", "USA"])
		Exp.cond(
			Exp.and(
				Exp.ge(   // Is the age 18 or older?
					Exp.intBin("age"),
					Exp.val(18)
				),
				Exp.or( // Do they live in a target country?
					Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(0))),
					Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(1))),
					Exp.eq(Exp.stringBin("country"), Exp.val(countries.get(2)))
				)
			),
			Exp.val(1),
			Exp.unknown()
		)
	);

	@BeforeAll
	public static void setup() {
		Version serverVersion = session.getCluster().getRandomNode().getVersion();
		boolean condition = serverVersion.isGreaterOrEqual(8, 1, 0, 0);
		Assumptions.assumeTrue(condition, "SI Expression tests skipped because the server does not support this feature");
	}

	public void insertTestRecords() {
		insertPersonRecord( 1, "Tim", 312, "Australia");
		insertPersonRecord( 2, "Bob", 47, "Canada");
		insertPersonRecord( 3, "Jo", 15, "USA");
		insertPersonRecord( 4, "Steven", 23, "Botswana");
		insertPersonRecord( 5, "Susan", 32, "Canada");
		insertPersonRecord( 6, "Jess", 17, "USA");
		insertPersonRecord( 7, "Sam", 18, "USA");
		insertPersonRecord( 8, "Alex", 47, "Canada");
		insertPersonRecord( 9, "Pam", 56, "Australia");
		insertPersonRecord( 10, "Vivek", 12, "India");
		insertPersonRecord( 11, "Kiril", 22, "Sweden");
		insertPersonRecord( 12, "Bill", 23, "UK");
	}

	private static void insertPersonRecord(int key, String name, int age, String country) {
		session.upsert(args.set.id(key))
			.bins("name", "age", "country")
			.values(name, age, country)
			.execute();
	}

	public static String getSecondaryIndices() {
		String cmd = "sindex-list/" + args.namespace + '/' + indexName;
		return Info.request(session.getCluster().getRandomNode(), cmd);
	}

	public static void addExpSI() {
		try {
			session.createIndex(dataSet, indexName, IndexType.INTEGER, IndexCollectionType.DEFAULT, exp)
				.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	@AfterAll
	public static void destroy() {
		session.dropIndex(dataSet, indexName);
	}

	@Test
	public void createExpSI() {
		addExpSI();
		String indices = getSecondaryIndices();
		assertTrue(indices.contains("indexname=" + indexName));
	}

	@Test
	public void queryExpSIbyName() {
		/*
		String sincdices = getSecondaryIndices();
		if (!sincdices.contains("indexname=" + indexName)) {
			addExpSI();
		}

		insertTestRecords();

		RecordStream rs = session.query(dataSet)
			.where(exp)
			.execute();

		// TODO Tim: How port Filter.rangeByIndex to fluent client?
		stmt.setFilter(Filter.rangeByIndex(indexName, 1, 1));
		QueryPolicy qp = new QueryPolicy();

		int count = 0;
		try (RecordSet recordSet = client.query(qp, stmt)) {
			while (recordSet.next()) {
				Record record = recordSet.getRecord();
				int age = record.getInt("age");
				String country = record.getString("country");
				assertTrue(age >= 18);
				assertTrue(countries.contains(country));
				count++;
			}
		}
		*/
		/*
		(bins:(name:Alex),(age:47),(country:Canada))
		(bins:(name:Tim),(age:312),(country:Australia))
		(bins:(name:Pam),(age:56),(country:Australia))
		(bins:(name:Bob),(age:47),(country:Canada))
		(bins:(name:Sam),(age:18),(country:USA))
		(bins:(name:Susan),(age:32),(country:Canada))
		 */
//		assertEquals(6, count);
	}

	@Test
	public void queryExpSIbyExp() {
		/*
		String sincdices = getSecondaryIndices();
		if (!sincdices.contains("indexname=" + indexName)) {
			addExpSI();
		}

		insertTestRecords();
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(setName);

		// TODO Tim: How port Filter.range to fluent client?
		stmt.setFilter(Filter.range(exp, 1, 1));
		QueryPolicy qp = new QueryPolicy();

		int count = 0;
		try (RecordSet recordSet = client.query(qp, stmt)) {
			while (recordSet.next()) {
				Record record = recordSet.getRecord();
				int age = record.getInt("age");
				String country = record.getString("country");
				assertTrue(age >= 18);
				assertTrue(countries.contains(country));
				count++;
			}
		}
		*/
		/*
		(bins:(name:Alex),(age:47),(country:Canada))
		(bins:(name:Tim),(age:312),(country:Australia))
		(bins:(name:Pam),(age:56),(country:Australia))
		(bins:(name:Bob),(age:47),(country:Canada))
		(bins:(name:Sam),(age:18),(country:USA))
		(bins:(name:Susan),(age:32),(country:Canada))
		 */
//		assertEquals(6, count);
	}
}
