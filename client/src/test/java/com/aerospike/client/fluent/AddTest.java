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
package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

public class AddTest extends ClusterTest {
	@Test
	public void add() {
		String key = "addkey";
		String binName = "addbin";

		// Delete record if it already exists.
        session.delete(args.set.id(key)).execute();

		// Perform some adds and check results.
        session.upsert(args.set.id(key))
        	.bin(binName).add(10)
	        .execute();

        session.upsert(args.set.id(key))
	    	.bin(binName).add(5)
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

    	int val = rec.getInt(binName);
		assertEquals(15, val);

		// Test add and get combined.
		rs = session.upsert(args.set.id(key))
        	.bin(binName).add(30)
        	.get(binName)
	        .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();

        // TODO: Return values in op order and add Record.getValue(int offset) methods?
        List<?> list = rec.getList(binName);
        val = (int)(long)list.get(1);
		assertEquals(45, val);
	}
}
