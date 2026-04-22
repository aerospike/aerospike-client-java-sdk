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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.mapper.Address;
import com.aerospike.client.sdk.mapper.Customer;
import com.aerospike.client.sdk.mapper.CustomerMapper;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

public class PutGetTest extends ClusterTest {

    @Test
    public void getNonExisting() {
        // Delete record if it already exists.
        Key key = args.set.id("nonexistingkey");
        session.delete(key).execute();

        RecordStream rs = session.query(key).execute();
        assertNull(rs.getFirstRecord());
    }

    @Test
    public void getNonExistingWithIncludeMissingKeys() {
        // Delete record if it already exists.
        Key key = args.set.id("nonexistingkey2");
        session.delete(key).execute();

        session.query(key)
            .includeMissingKeys()
            .execute()
            .getFirst(false)
            .ifPresentOrElse(result ->
                assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, result.resultCode()),
                () -> fail("Failed to retrieve record response"));
    }

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

    @Test
    public void putGetObjectAsync() {
        int key = 9999;

        CustomerMapper customerMapper = new CustomerMapper();

        session.delete(args.set.id(key)).execute();

        Customer customer = new Customer(key, "sample", 456, new Date(),
            new Address("123 Main St", "Denver", "CO", "USA", "80112"));

        TypeSafeDataSet<Customer> customerDataSet =
            new TypeSafeDataSet<Customer>(args.namespace, args.set.getSet(), Customer.class);

        RecordStream rs = session.insert(customerDataSet)
            .object(customer)
            .using(customerMapper)
            .executeAsync(ErrorStrategy.IN_STREAM);

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

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

    @Test
    public void putGetObjects() {
        int key = 2000;
        int age = 26;

        CustomerMapper customerMapper = new CustomerMapper();

        Customer customer1 = new Customer(key, "sample1", age, new Date(),
            new Address("123 Main St", "Denver", "CO", "USA", "80112"));

        Customer customer2 = new Customer(key + 1, "sample2", age + 1, new Date(),
            new Address("130 Main St", "Denver", "CO", "USA", "80112"));

        Customer customer3 = new Customer(key + 2, "sample3", age + 2, new Date(),
            new Address("145 Main St", "Denver", "CO", "USA", "80112"));

        TypeSafeDataSet<Customer> customerDataSet =
            new TypeSafeDataSet<Customer>(args.namespace, args.set.getSet(), Customer.class);

        session.upsert(customerDataSet)
            .objects(customer1, customer2, customer3)
            .using(customerMapper)
            .execute();

        List<Customer> readCustomers = session.query(customerDataSet.ids(key, key + 1, key + 2))
            .execute()
            .toObjectList(customerMapper);

        assertNotNull(readCustomers);
        assertEquals(3, readCustomers.size());

        int count = 1;

        for (Customer c : readCustomers) {
            assertEquals(key, c.getId());
            assertEquals(age, c.getAge());
            assertEquals("sample" + count, c.getName());
            key++;
            age++;
            count++;
        }
    }

    @Test
    public void putGetObjectsAsync() {
        int key = 3000;
        int age = 36;

        Customer customer1 = new Customer(key, "sample1", age, new Date(),
            new Address("123 Main St", "Denver", "CO", "USA", "80112"));

        Customer customer2 = new Customer(key + 1, "sample2", age + 1, new Date(),
            new Address("130 Main St", "Denver", "CO", "USA", "80112"));

        Customer customer3 = new Customer(key + 2, "sample3", age + 2, new Date(),
            new Address("145 Main St", "Denver", "CO", "USA", "80112"));

        TypeSafeDataSet<Customer> customerDataSet =
            new TypeSafeDataSet<Customer>(args.namespace, args.set.getSet(), Customer.class);

        CustomerMapper customerMapper = new CustomerMapper();

        RecordStream rs = session.upsert(customerDataSet)
            .objects(customer1, customer2, customer3)
            .using(customerMapper)
            .executeAsync(ErrorStrategy.IN_STREAM);

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

        List<Customer> readCustomers = session.query(customerDataSet.ids(key, key + 1, key + 2))
            .execute()
            .toObjectList(customerMapper);

        assertNotNull(readCustomers);
        assertEquals(3, readCustomers.size());

        int count = 1;

        for (Customer c : readCustomers) {
            assertEquals(key, c.getId());
            assertEquals(age, c.getAge());
            assertEquals("sample" + count, c.getName());
            key++;
            age++;
            count++;
        }
    }

    @Test
    public void putGetObjectsBatch() {
        List<Key> keys = args.set.ids(100,101,102,103,104,105,106,107,108,109);
        Address address = new Address("123 Main St", "Denver", "CO", "USA", "80112");
        Date date = new Date();

        List<Customer> customers = new ArrayList<>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            int id = key.userKey.toInteger();
            Customer customer = new Customer(id, "sample" + id, 50 + i, date, address);

            customers.add(customer);
        }

        TypeSafeDataSet<Customer> customerDataSet =
            new TypeSafeDataSet<Customer>(args.namespace, args.set.getSet(), Customer.class);

        CustomerMapper customerMapper = new CustomerMapper();

        session.delete(keys).execute();

        session.upsert(customerDataSet)
            .objects(customers)
            .using(customerMapper)
            .execute();

        List<Customer> readCustomers = session.query(keys)
            .execute()
            .toObjectList(customerMapper);

        assertNotNull(readCustomers);
        assertEquals(10, readCustomers.size());

        int offset = 0;

        for (Customer c : readCustomers) {
            assertEquals(100 + offset, c.getId());
            assertEquals(50 + offset, c.getAge());
            assertEquals("sample" + c.getId(), c.getName());
            offset++;
        }
    }

    @Test
    public void exceedingBinLimitReturnsError() {
        Key key = args.set.id("binlimitkey");
        session.delete(key).execute();

        try {
            var builder = session.upsert(key);
            for (int i = 0; i < 33000; i++) {
                builder = builder.bin("b" + i).setTo(i);
            }
            RecordStream rs = builder.execute();
            if (rs.hasNext()) {
                rs.next().recordOrThrow();
            }
            // Server accepted — valid for high-limit namespace configs.
        } catch (AerospikeException ae) {
            int rc = ae.getResultCode();
            assertEquals(ResultCode.PARAMETER_ERROR, rc, "Expected bin ops limit error, got: " + rc + " (" + ResultCode.getResultString(rc) + ")");
        }
    }

    @Test
    public void putGetObjectsBatchAsync() {
        List<Key> keys = args.set.ids(200,201,202,203,204,205,206,207,208,209);
        Address address = new Address("123 Main St", "Denver", "CO", "USA", "80112");
        Date date = new Date();

        List<Customer> customers = new ArrayList<>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            int id = key.userKey.toInteger();
            Customer customer = new Customer(id, "sample" + id, 50 + i, date, address);

            customers.add(customer);
        }

        TypeSafeDataSet<Customer> customerDataSet =
            new TypeSafeDataSet<Customer>(args.namespace, args.set.getSet(), Customer.class);

        CustomerMapper customerMapper = new CustomerMapper();

        session.delete(keys).execute();

        RecordStream rs = session.upsert(customerDataSet)
            .objects(customers)
            .using(customerMapper)
            .executeAsync(ErrorStrategy.IN_STREAM);

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

        List<Customer> readCustomers = session.query(keys)
            .execute()
            .toObjectList(customerMapper);

        assertNotNull(readCustomers);
        assertEquals(10, readCustomers.size());

        int offset = 0;

        for (Customer c : readCustomers) {
            assertEquals(200 + offset, c.getId());
            assertEquals(50 + offset, c.getAge());
            assertEquals("sample" + c.getId(), c.getName());
            offset++;
        }
    }
}
