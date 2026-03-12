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

import com.aerospike.client.fluent.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.info.classes.IndexType;

public class QueryIntegerTest extends ClusterTest {
	private static final String setName = "queryint";
	private static final String indexName = "testindexint";
	private static final String keyPrefix = "testkeyint";
	private static final String binName = "testbinint";
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
			session.upsert(dataSet.ids(key))
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
	public void queryInteger() {
		int begin = 14;
		int end = 18;

		RecordStream rs = session.query(dataSet)
			.where(Exp.and(
				Exp.ge(Exp.intBin(binName), Exp.val(begin)),
				Exp.le(Exp.intBin(binName), Exp.val(end))
			))
			.execute();

		int count = 0;
		while (rs.hasNext()) {
			rs.next();
			count++;
		}

		assertEquals(5, count);
	}
}
