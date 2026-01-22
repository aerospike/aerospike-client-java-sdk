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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.mapper.Address;
import com.aerospike.client.fluent.mapper.Customer;
import com.aerospike.client.fluent.mapper.CustomerMapper;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Behavior.Selectors;

public class PutGetTest extends ClusterTest {
	@Test
	public void putGet() {
		String key = "putgetkey";

        // Write record.
        session.upsert(args.set.id(key))
	        .bins("bin1", "bin2")
	        .values("value1", "value2")
	        .execute();

        // Query all bins.
        RecordStream rs = session.query(args.set.id(key)).execute();
    	Record rec = rs.next().recordOrThrow();

    	String val = rec.getString("bin1");
		assertEquals("value1", val);

		val = rec.getString("bin2");
		assertEquals("value2", val);

        // Query specific bin.
        rs = session.query(args.set.id(key))
        	.readingOnlyBins("bin2")
        	.execute();
    	rec = rs.next().recordOrThrow();

    	val = rec.getString("bin1");
		assertNull(val);

		val = rec.getString("bin2");
		assertEquals("value2", val);
	}

	@Test
	public void getHeader() {
		String key = "getHeader";

        session.upsert(args.set.id(key))
	        .bins("mybin")
	        .values("myvalue")
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.withNoBins()
        	.execute();

    	Record rec = rs.next().recordOrThrow();

    	String val = rec.getString("mybin");
		assertNull(val);

		// Generation should be greater than zero.  Make sure it's populated.
		if (rec.generation == 0) {
			fail("Invalid record header: generation=" + rec.generation + " expiration=" + rec.expiration);
		}
	}

	@Test
	public void putGetBool() {
		String key = "putGetBool";

        session.delete(args.set.id(key)).execute();

        session.upsert(args.set.id(key))
	        .bins("bin1", "bin2", "bin3", "bin4")
	        .values(false, true, 0, 1)
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.execute();

    	Record rec = rs.next().recordOrThrow();

    	boolean b = rec.getBoolean("bin1");
		assertFalse(b);
		b = rec.getBoolean("bin2");
		assertTrue(b);
		b = rec.getBoolean("bin3");
		assertFalse(b);
		b = rec.getBoolean("bin4");
		assertTrue(b);
	}

	@Test
	public void putGetCompress() {
		assumeTrue(args.enterprise);

        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("compress", builder -> builder
            .on(Selectors.all(), ops -> ops
                .useCompression(true)
            )
        );

        // Use local session to change default behavior.
        Session session = cluster.createSession(behavior);

	    String key = "putGetCompress";
		byte[] bytes = new byte[2000];

		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte)(i % 256);
		}

        session.upsert(args.set.id(key))
	        .bins("bb")
	        .values(bytes)
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
            .execute();

        Record rec = rs.next().recordOrThrow();

		byte[] rcv = rec.getBytes("bb");
		assertEquals(2000, rcv.length);

		for (int i = 0; i < rcv.length; i++) {
			byte b = (byte)(i % 256);
			assertEquals(b, rcv[i]);
		}
	}

	@Test
	public void putGetObject() {
		int key = 999;

        CustomerMapper customerMapper = new CustomerMapper();

        session.delete(args.set.id(key)).execute();

        Customer customer = new Customer(key, "sample", 456, new Date(),
        	new Address("123 Main St", "Denver", "CO", "USA", "80112"));

        TypeSafeDataSet<Customer> customerDataSet =
        	new TypeSafeDataSet<Customer>(args.namespace, args.set.getSet(), Customer.class);

        session.insert(customerDataSet)
        	.object(customer)
        	.using(customerMapper)
	        .execute();

        List<Customer> readCustomers = session.query(customerDataSet.id(key))
        	.execute()
        	.toObjectList(customerMapper);

		assertNotNull(readCustomers);
		assertEquals(1, readCustomers.size());

		Customer readCustomer = readCustomers.get(0);

		assertEquals(customer.getId(), readCustomer.getId());
		assertEquals(customer.getAge(), readCustomer.getAge());
		assertEquals(customer.getName(), readCustomer.getName());
	}
}
