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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class GenerationTest extends ClusterTest {
	@Test
	public void generation() {
		Key key = args.set.id("generation");
		String binName = "genbin";

		// Delete record if it already exists.
        session.delete(key).execute();

		// Set values for the same record.
        session.upsert(key)
	        .bin(binName).setTo("genvalue1")
	        .execute();

        session.upsert(key)
	        .bin(binName).setTo("genvalue2")
	        .execute();

		// Retrieve record and its generation count.
        RecordStream rs = session.query(key).readingOnlyBins(binName).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        String val = rec.getString(binName);
		assertEquals("genvalue2", val);

		// Set record and fail if it's not the expected generation.
        session.upsert(key)
        	.ensureGenerationIs(rec.generation)
	        .bin(binName).setTo("genvalue3")
	        .execute();

		// Set record with invalid generation and check results .
        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
        	RecordStream rs2 = session.upsert(key)
    	    	.ensureGenerationIs(9999)
    	        .bin(binName).setTo("genvalue4")
    	        .execute();

            assertTrue(rs2.hasNext());
            rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.GENERATION_ERROR, ae.getResultCode());

		// Verify results.
        rs = session.query(key).readingOnlyBins(binName).execute();
        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getString(binName);
		assertEquals("genvalue3", val);
	}
}
