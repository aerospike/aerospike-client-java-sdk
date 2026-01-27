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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Behavior.Selectors;
import com.aerospike.client.fluent.util.Util;

public class ExpireTest extends ClusterTest {
	private static final String binName = "expirebin";

	@Test
	public void expire() {
		Assumptions.assumeTrue(args.hasTtl);

		String key = "expire";

		// Specify that record expires 1 second after it's written.
        session.upsert(args.set.id(key))
        	.expireRecordAfterSeconds(1)
	        .bin(binName).setTo("expirevalue")
	        .execute();

		// Read the record before it expires, showing it is there.
        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		assertNotNull(rec);

		String val = rec.getString(binName);
		assertEquals("expirevalue", val);

		// Read the record after it expires, showing it's gone.
		Util.sleep(3 * 1000);

        rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertFalse(rs.hasNext());
	}

	@Test
	public void noExpire() {
		String key = "noExpire";

		// Specify that record NEVER expires.
		// The "Never Expire" value is -1, or 0xFFFFFFFF.
        session.upsert(args.set.id(key))
	    	.expireRecordAfterSeconds(-1)  // TODO: Is this correct?
	        .bin(binName).setTo("noexpirevalue")
	        .execute();

		// Read the record, showing it is there.
        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		assertNotNull(rec);

		String val = rec.getString(binName);
		assertEquals("noexpirevalue", val);

		// Read this Record after the Default Expiration, showing it is still there.
		// We should have set the Namespace TTL at 5 sec.
		Util.sleep(10 * 1000);

        rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		assertNotNull(rec);

		val = rec.getString(binName);
		assertEquals("noexpirevalue", val);
	}

	@Test
	public void resetReadTtl() {
		Assumptions.assumeTrue(args.hasTtl);

		String key = "resetReadTtl";

        session.upsert(args.set.id(key))
	    	.expireRecordAfterSeconds(2)  // TODO: Is this correct?
	        .bin(binName).setTo("expirevalue")
	        .execute();

		// Read the record before it expires and reset read ttl.
		Util.sleep(1000);

        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("readttl", builder -> builder
            .on(Selectors.all(), ops -> ops
                .resetTtlOnReadAtPercent(80)
            )
        );

        // Use local session to change default behavior.
        Session session = cluster.createSession(behavior);

        RecordStream rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		assertNotNull(rec);

		String val = rec.getString(binName);
		assertEquals("expirevalue", val);

		// Read the record again, but don't reset read ttl.
		Util.sleep(1000);

        behavior = Behavior.DEFAULT.deriveWithChanges("readttl", builder -> builder
            .on(Selectors.all(), ops -> ops
                .resetTtlOnReadAtPercent(-1)
            )
        );

        session = cluster.createSession(behavior);

        rs = session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		assertNotNull(rec);

		val = rec.getString(binName);
		assertEquals("expirevalue", val);

		// Read the record after it expires, showing it's gone.
		Util.sleep(2000);

        rs = ClusterTest.session.query(args.set.id(key))
        	.readingOnlyBins(binName)
            .execute();

        assertFalse(rs.hasNext());
	}
}
