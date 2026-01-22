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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.aerospike.client.fluent.exp.Exp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.info.classes.IndexType;

public class QueryKeyTest extends ClusterTest {
	private static final String setName = "querykey";
	private static final String indexName = "skindex";
	private static final String keyPrefix = "skkey";
	private static final String binName = "skbin";
	private static final int size = 10;

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
			session.createIndex(dataSet, indexName, binName, IndexType.NUMERIC, IndexCollectionType.DEFAULT)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;
			session.upsert(dataSet.ids(key))
				.sendKey()
				.bins(binName)
				.values(i)
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
	public void queryKey() {
		int begin = 2;
		int end = 5;

		RecordStream rs = session.query(dataSet)
			.where(Exp.and(
				Exp.ge(Exp.intBin(binName), Exp.val(begin)),
				Exp.le(Exp.intBin(binName), Exp.val(end))
			))
			.execute();

		int count = 0;
		while (rs.hasNext()) {
			RecordResult result = rs.next();
			Key key = result.key();
			assertNotNull(key.userKey);

			Object userkey = key.userKey.getObject();
			assertNotNull(userkey);
			count++;
		}

		assertEquals(4, count);
	}
}
