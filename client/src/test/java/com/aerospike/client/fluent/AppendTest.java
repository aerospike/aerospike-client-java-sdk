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

public class AppendTest extends ClusterTest {
	@Test
	public void append() {
		String key = "append";
		String binName = "appendbin";

		// Delete record if it already exists.
        session.delete(args.set.id(key)).execute();

		// Perform some appends and check results.
        session.upsert(args.set.id(key))
	    	.bin(binName).append("Hello")
	        .execute();

        session.upsert(args.set.id(key))
    		.bin(binName).append(" World")
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

    	String val = rec.getString(binName);
		assertEquals("Hello World", val);

		// Test append and get combined.
		rs = session.upsert(args.set.id(key))
        	.bin(binName).append("!")
        	.get(binName)
	        .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();

        List<?> list = rec.getList(binName);
        val = (String)list.get(1);
		assertEquals("Hello World!", val);
	}

	@Test
	public void prepend() {
		String key = "prepend";
		String binName = "appendbin";

		// Delete record if it already exists.
        session.delete(args.set.id(key)).execute();

		// Perform some appends and check results.
        session.upsert(args.set.id(key))
	    	.bin(binName).prepend("!")
	        .execute();

        session.upsert(args.set.id(key))
    		.bin(binName).prepend("World")
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

    	String val = rec.getString(binName);
		assertEquals("World!", val);

		// Test append and get combined.
		rs = session.upsert(args.set.id(key))
        	.bin(binName).prepend("Hello ")
        	.get(binName)
	        .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();

        List<?> list = rec.getList(binName);
        val = (String)list.get(1);
		assertEquals("Hello World!", val);
	}
}
