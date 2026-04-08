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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

public class QueryStringTest extends ClusterTest {
	private static final String indexName = "queryindex";
	private static final String keyPrefix = "querykey";
	private static final String valuePrefix = "queryvalue";
	private static final String binName = "querybin";
	private static int size = 5;

	@BeforeAll
	public static void prepare() {
		try {
			session.createIndex(args.set, indexName, binName, IndexType.STRING, IndexCollectionType.DEFAULT)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;
			String value = valuePrefix + i;
			session.upsert(args.set.ids(key))
				.bins(binName)
				.values(value)
				.execute();
		}
	}

	@AfterAll
	public static void destroy() {
		session.dropIndex(args.set, indexName);
	}

	@Test
	public void queryString() {
		String filter = valuePrefix + 3;

		RecordStream rs = session.query(args.set)
			.readingOnlyBins(binName)
			.where("$." + binName + " == '" + filter + "'")
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				Record rec = rs.next().recordOrThrow();
				String result = rec.getString(binName);
				assertEquals(filter, result);
				count++;
			}

			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryStringEmptyBinName() {
		String filter = valuePrefix + 3;

		RecordStream rs = session.query(args.set)
			.where("$." + binName + " == '" + filter + "'")
			.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				Record record = rs.next().recordOrThrow();
				String result = record.getString(binName);
				assertEquals(filter, result);
				count++;
			}

			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}
}
