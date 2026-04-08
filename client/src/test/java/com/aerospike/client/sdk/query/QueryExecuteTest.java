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
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.UdfTest;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.info.classes.IndexType;
import com.aerospike.client.sdk.task.ExecuteTask;
import com.aerospike.client.sdk.task.RegisterTask;

public class QueryExecuteTest extends ClusterTest {
	private static final String indexName = "tqeindex";
	private static final String keyPrefix = "tqekey";
	private static final String binName1 = "tqebin1";
	private static final String binName2 = "tqebin2";
	private static final int size = 10;

	@BeforeAll
	public static void prepare() {
		RegisterTask task = session.registerUdfString(UdfTest.lua, "record_example.lua");
		task.waitTillComplete();

		try {
			session.createIndex(args.set, indexName, binName1, IndexType.INTEGER, IndexCollectionType.DEFAULT)
				.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = args.set.id(keyPrefix + i);
			session.upsert(key)
				.bins(binName1, binName2)
				.values(i, i)
				.execute();
		}
	}

	@AfterAll
	public static void destroy() {
		session.dropIndex(args.set, indexName);
	}

	@Test
	public void queryExecute() {
		ExecuteTask task = session.backgroundTask()
            .executeUdf(args.set)
            .function("record_example", "processRecord")
            .passing(binName1, binName2, 100)
			.where("$.tqebin1 >= 3 and $.tqebin1 <= 9")
            .execute();

		task.waitTillComplete(3000, 3000);
		validateRecords();
	}

	private void validateRecords() {
		RecordStream rs = session.query(args.set)
			.where("$.tqebin1 >= 1 and $.tqebin1 <= " + (size + 100))
			.execute();

		try {
			int[] expectedList = new int[] {1,2,3,104,5,106,7,108,-1,10};
			int expectedSize = size - 1;
			int count = 0;

			while (rs.hasNext()) {
				Record rec = rs.next().recordOrThrow();
				int value1 = rec.getInt(binName1);
				int value2 = rec.getInt(binName2);

				int val1 = value1;

				if (val1 == 9) {
					fail("Data mismatch. value1 " + val1 + " should not exist");
				}

				if (val1 == 5) {
					if (value2 != 0) {
						fail("Data mismatch. value2 " + value2 + " should be null");
					}
				}
				else if (value1 != expectedList[value2-1]) {
					fail("Data mismatch. Expected " + expectedList[value2-1] + ". Received " + value1);
				}
				count++;
			}
			assertEquals(expectedSize, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryExecuteOperate() {
		String binName = "foo";
		String binValue = "bar";

		ExecuteTask task = session.backgroundTask()
            .update(args.set)
            .bin(binName).setTo(binValue)
			.where("$.tqebin1 >= 3 and $.tqebin1 <= 9")
            .execute();

		task.waitTillComplete(3000, 3000);

		RecordStream rs = session.query(args.set)
			.where("$.tqebin1 >= 3 and $.tqebin1 <= 9")
			.execute();

		try {
			int count = 0;

			while (rs.hasNext()) {
				Record rec = rs.next().recordOrThrow();
				String value = rec.getString(binName);

				if (value == null) {
					fail("Bin " + binName + " not found");
				}

				if (! value.equals(binValue)) {
					fail("Data mismatch. Expected " + binValue + ". Received " + value);
				}
				count++;
			}
			assertEquals(7, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryExecuteOperateExp() {
		String binName = "foo";
		String binValue = "bar";
		Expression exp = Exp.build(Exp.val(binValue));

		ExecuteTask task = session.backgroundTask()
            .update(args.set)
            .bin(binName).updateFrom(exp)
			.where("$.tqebin1 >= 3 and $.tqebin1 <= 9")
            .execute();

		task.waitTillComplete(3000, 3000);

		RecordStream rs = session.query(args.set)
			.where("$.tqebin1 >= 3 and $.tqebin1 <= 9")
			.execute();

		try {
			int count = 0;

			while (rs.hasNext()) {
				Record rec = rs.next().recordOrThrow();
				String value = rec.getString(binName);

				if (value == null) {
					fail("Bin " + binName + " not found");
				}

				if (! value.equals("bar")) {
					fail("Data mismatch. Expected bar. Received " + value);
				}
				count++;
			}
			assertEquals(7, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryExecuteSetNotFound() {
		// Previous client versions might timeout when set does not exist.
		// Test to make sure regression has not resurfaced.
		DataSet ds = DataSet.of(args.set.getNamespace(), "notfound");

		session.backgroundTask()
			.touch(ds)
			.where("$.tqebin1 >= 1 and $.tqebin1 <= 3")
            .execute();
	}
}
