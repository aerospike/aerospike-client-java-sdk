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
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.dsl.ParseResult;

public class QueryContextTest extends ClusterTest {
	private static final String indexName = "listrank";
	private static final String binName = "list";
	private static final int size = 50;

	private static DataSet dataSet;

	@BeforeAll
	public static void prepare() {
		dataSet = args.set;

		try {
			session.createIndex(
				dataSet, indexName, binName,
				IndexType.INTEGER, IndexCollectionType.DEFAULT,
				CTX.listRank(-1)
			).waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			ArrayList<Integer> list = new ArrayList<Integer>(5);
			list.add(i);
			list.add(i + 1);
			list.add(i + 2);
			list.add(i + 3);
			list.add(i + 4);

			session.upsert(dataSet.ids(i))
				.bins(binName)
				.values(list)
				.execute();
		}
	}

	@AfterAll
	public static void destroy() {
		session.dropIndex(dataSet, indexName);
	}

	@Test
	public void queryContext() throws Exception {
		long begin = 14;
		long end = 18;

		Filter filter = Filter.range(binName, begin, end, CTX.listRank(-1));

		WhereClauseProcessor filterProcessor = new WhereClauseProcessor(true) {
			@Override
			public ParseResult process(String namespace, com.aerospike.client.fluent.Session session) {
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

		int count = 0;
		while (rs.hasNext()) {
			List<?> list = rs.next().recordOrThrow().getList(binName);
			long received = (Long)list.get(list.size() - 1);

			if (received < begin || received > end) {
				fail("Received not between: " + begin + " and " + end);
			}
			count++;
		}

		assertEquals(5, count);
	}
}
