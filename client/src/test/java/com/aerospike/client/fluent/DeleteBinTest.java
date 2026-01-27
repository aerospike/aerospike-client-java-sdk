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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DeleteBinTest extends ClusterTest {
	@Test
	public void deleteBin() {
		String key = "deleteBin";
		String binName1 = "bin1";
		String binName2 = "bin2";

        session.upsert(args.set.id(key))
	        .bin(binName1).setTo("value1")
	        .bin(binName2).setTo("value2")
	        .execute();

        // Set bin value to null to drop bin.
        session.upsert(args.set.id(key))
	        .bin(binName1).remove()
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName1, binName2, "bin3")
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		assertNotNull(rec);

		Object v1 = rec.getValue(binName1);
		assertNull(v1);

		String v2 = rec.getString(binName2);
		assertEquals("value2", v2);

		v1 = rec.getValue("bin3");
		assertNull(v1);
	}
}
