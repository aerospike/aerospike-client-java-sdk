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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.info.classes.IndexType;

public class QueryBlobTest extends ClusterTest {
	private static final String setName = "queryblob";
	private static final String indexName = "qbindex";
	private static final String binName = "bb";
	private static final String indexNameList = "qblist";
	private static final String binNameList = "bblist";
	private static int size = 5;

	private static DataSet dataSet;

	@BeforeAll
	public static void prepare() {
		dataSet = DataSet.of(args.namespace, setName);

		for (int i = 1; i <= size; i++) {
			session.delete(dataSet.ids(i));
		}

		try {
			session.createIndex(dataSet, indexName, binName, IndexType.BLOB, IndexCollectionType.DEFAULT)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		try {
			session.createIndex(dataSet, indexNameList, binNameList, IndexType.BLOB, IndexCollectionType.LIST)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			byte[] bytes = new byte[8];
			Buffer.longToBytes(50000 + i, bytes, 0);

			List<byte[]> list = new ArrayList<>();
			list.add(bytes);

			session.upsert(dataSet.ids(i))
				.bins(binName, binNameList)
				.values(bytes, list)
				.execute();
		}
	}

	@AfterAll
	public static void destroy() {
		for (int i = 1; i <= size; i++) {
			session.delete(dataSet.ids(i));
		}
		session.dropIndex(dataSet, indexName);
		session.dropIndex(dataSet, indexNameList);
	}

/* TODO Implement this test when AEL supports blobs in the where clause.
	@Test
	public void queryBlob() throws Exception {
		byte[] bytes = new byte[8];
		Buffer.longToBytes(50003, bytes, 0);

		Filter filter = Filter.equal(binName, bytes);

		WhereClauseProcessor filterProcessor = new WhereClauseProcessor(true) {
			@Override
			public ParseResult process(String namespace, com.aerospike.client.sdk.Session session) {
				return new ParseResult(filter, null);
			}
		};

		var queryBuilder = session.query(dataSet)
			.readingOnlyBins(binName);

		var setWhereMethod = queryBuilder.getClass().getSuperclass()
			.getDeclaredMethod("setWhereClause", WhereClauseProcessor.class);
		setWhereMethod.setAccessible(true);
		setWhereMethod.invoke(queryBuilder, filterProcessor);

		RecordStream rs = queryBuilder.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				Record record = rs.next().recordOrThrow();
				byte[] result = record.getBytes(binName);
				assertTrue(Arrays.equals(bytes, result));
				count++;
			}
			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}
*/

/* TODO Implement this test when AEL supports blobs in the where clause.
	@Test
	public void queryBlobInList() throws Exception {
		byte[] bytes = new byte[8];
		Buffer.longToBytes(50003, bytes, 0);

		Filter filter = Filter.contains(binNameList, IndexCollectionType.LIST, bytes);

		WhereClauseProcessor filterProcessor = new WhereClauseProcessor(true) {
			@Override
			public ParseResult process(String namespace, com.aerospike.client.sdk.Session session) {
				return new ParseResult(filter, null);
			}
		};

		var queryBuilder = session.query(dataSet)
			.readingOnlyBins(binName, binNameList);

		var setWhereMethod = queryBuilder.getClass().getSuperclass()
			.getDeclaredMethod("setWhereClause", WhereClauseProcessor.class);
		setWhereMethod.setAccessible(true);
		setWhereMethod.invoke(queryBuilder, filterProcessor);

		RecordStream rs = queryBuilder.execute();

		try {
			int count = 0;
			while (rs.hasNext()) {
				Record record = rs.next().recordOrThrow();

				List<?> list = record.getList(binNameList);
				assertEquals(1, list.size());

				byte[] result = (byte[])list.get(0);
				assertTrue(Arrays.equals(bytes, result));
				count++;
			}
			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}
*/
}
