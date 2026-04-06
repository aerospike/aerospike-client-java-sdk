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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.info.classes.IndexType;

public class QueryCollectionTest extends ClusterTest {
	private static final String setName = "querycoll";
	private static final String indexName = "mapkey_index";
	private static final String keyPrefix = "qkey";
	private static final String mapKeyPrefix = "mkey";
	private static final String mapValuePrefix = "qvalue";
	private static final String binName = "map_bin";
	private static final int size = 20;

	private static DataSet dataSet;

	@BeforeAll
	public static void prepare() {
		dataSet = DataSet.of(args.namespace, setName);

		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;
			session.delete(dataSet.ids(key));
		}

		try {
			session.createIndex(dataSet, indexName, binName, IndexType.STRING, IndexCollectionType.MAPKEYS)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			String key = keyPrefix + i;
			HashMap<String,String> map = new HashMap<String,String>();

			map.put(mapKeyPrefix+1, mapValuePrefix+i);
			if (i%2 == 0) {
				map.put(mapKeyPrefix+2, mapValuePrefix+i);
			}
			if (i%3 == 0) {
				map.put(mapKeyPrefix+3, mapValuePrefix+i);
			}

			session.upsert(dataSet.ids(key))
				.bins(binName)
				.values(map)
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
	public void queryCollection() throws Exception {
		String queryMapKey = mapKeyPrefix+2;

		String where = "$." + binName + "." + queryMapKey + ".get(return: EXISTS) == true";

		RecordStream rs = session.query(dataSet)
			.where(where)
			.execute();

		try {
			int count = 0;

			while (rs.hasNext()) {
				Record record = rs.next().recordOrThrow();
				Map<?,?> result = record.getMap(binName);

				if (!result.containsKey(queryMapKey)) {
					fail("Query mismatch: Expected mapKey " + queryMapKey + " Received " + result);
				}
				count++;
			}

			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}
}
