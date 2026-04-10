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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

public class OperateTest extends ClusterTest {
	@Test
	public void operate() {
		// Write initial record.
		Key key = args.set.id("operate");
		String binName1 = "optintbin";
		String binName2 = "optstringbin";

		session.upsert(key)
	        .bin(binName1).setTo(7)
	        .bin(binName2).setTo("string value")
	        .execute();

		// Add integer, write new string and read record.
		RecordStream rs = session.upsert(key)
	        .bin(binName1).add(4)
	        .bin(binName2).setTo("new string")
	        .bin(binName1).get()
	        .bin(binName2).get()
	        .execute();

		assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        List<?> list1 = rec.getList(binName1);
        List<?> list2 = rec.getList(binName2);

		assertEquals(11, (int)(long)list1.get(1));
		assertEquals("new string", list2.get(1));
	}

	@Test
	public void invalidOperationTypeMismatchReturnsError() {
		Key key = args.set.id("invalidOp");

		// Write an integer bin.
		session.upsert(key)
			.bin("intbin").setTo(42)
			.execute();

		// Attempt a list append on an integer bin (type mismatch).
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs = session.upsert(key)
				.bin("intbin").listAppend("item")
				.execute();

			assertTrue(rs.hasNext());
			rs.next().recordOrThrow();
		});

		//Apr 10 2026 07:37:45 GMT: WARNING (particle) cdt_process_state_packed_list_modify_optype() invalid type 1
		int rc = ae.getResultCode();
        assertEquals(ResultCode.BIN_TYPE_ERROR, rc, "Expected BIN_TYPE_ERROR, got: " +
                rc + " (" + ResultCode.getResultString(rc) + ")");
	}

	/* TODO Implement test when an operate call is supported that deletes the full record.
	@Test
	public void operateDelete() {
		// Write initial record.
		Key key = args.set.id("operateDelete");
		String binName1 = "optintbin1";
		String binName2 = "optintbin2";

		session.upsert(key)
	        .bin(binName1).setTo(1)
	        .execute();

		// Read bin1 and then delete all.
		RecordStream rs = session.upsert(key)
		    .bin(binName1).get()
	        .delete(key)
	        .execute();

		assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

        // Verify record is gone.
		rs = session.exists(key)
			.execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        System.out.println(rec);

		// Rewrite record.
		Bin bin2 = new Bin("optintbin2", 2);

		client.put(null, key, bin1, bin2);

		// Read bin 1 and then delete all followed by a write of bin2.
		record = client.operate(null, key, Operation.get(bin1.name), Operation.delete(),
				Operation.put(bin2), Operation.get(bin2.name));
		assertBinEqual(key, record, bin1.name, 1);

		// Read record.
		record = client.get(null, key);
		assertBinEqual(key, record, bin2.name, 2);
		assertTrue(record.bins.size() == 1);
	}
*/
}
