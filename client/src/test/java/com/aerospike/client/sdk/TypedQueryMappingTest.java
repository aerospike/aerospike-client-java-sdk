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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.mapper.Address;
import com.aerospike.client.sdk.mapper.Customer;
import com.aerospike.client.sdk.mapper.CustomerMapper;

/**
 * Integration-style tests for typed reads (requires a running cluster like {@link ClusterTest}).
 */
public class TypedQueryMappingTest extends ClusterTest {

    @Test
    public void typedDatasetQueryUsesRecordMappingFactory() {
        CustomerMapper customerMapper = new CustomerMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));

        int key = 91001;
        session.delete(args.set.id(key)).execute();

        TypedDataSet<Customer> customerDataSet =
            new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);

        Customer customer = new Customer(key, "typed-read", 21, new Date(),
            new Address("1 Typed St", "Boulder", "CO", "USA", "80301"));

        session.insert(customerDataSet).object(customer).execute();

        TypedRecordStream<Customer> stream = session.query(customerDataSet)
            .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
            .limit(10)
            .execute();

        List<Customer> customers = stream.toObjectList();
        assertEquals(1, customers.size());
        assertEquals(key, customers.get(0).getId());
        assertEquals("typed-read", customers.get(0).getName());
    }

    @Test
    public void typedKeyBatchResultsSupportToObject() {
        CustomerMapper customerMapper = new CustomerMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));

        TypedDataSet<Customer> ds = new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        int k1 = 91002;
        int k2 = 91003;
        session.delete(ds.id(k1)).execute();
        session.delete(ds.id(k2)).execute();

        session.insert(ds).object(new Customer(k1, "a", 1, new Date(),
            new Address("x", "y", "CO", "US", "1"))).execute();
        session.insert(ds).object(new Customer(k2, "b", 2, new Date(),
            new Address("x", "y", "CO", "US", "2"))).execute();

        RecordStream rs = session.query(ds.id(k1), ds.id(k2)).execute();

        Customer c1 = rs.next().toObject(session);
        Customer c2 = rs.next().toObject(session);
        assertEquals(k1, c1.getId());
        assertEquals(k2, c2.getId());
        rs.close();
    }

    @Test
    public void singleTypedKeyPointReadCarriesHintForToObject() {
        CustomerMapper customerMapper = new CustomerMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));

        TypedDataSet<Customer> ds = new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        int k = 91005;
        session.delete(ds.id(k)).execute();
        session.insert(ds).object(new Customer(k, "point-read", 3, new Date(),
            new Address("a", "b", "CO", "US", "1"))).execute();

        RecordResult rr = session.query(ds.id(k)).execute().getFirst().orElseThrow();
        Customer c = rr.toObject(session);
        assertEquals(k, c.getId());
        assertEquals("point-read", c.getName());
    }

    @Test
    public void explicitMapperOnTypedStreamPassesRecordReadContextWithSession() {
        CustomerMapper baseMapper = new CustomerMapper();
        RecordMapper<Customer> capturingMapper = new RecordMapper<Customer>() {
            @Override
            public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
                return baseMapper.fromMap(map, recordKey, generation);
            }

            @Override
            public Customer fromMap(
                    Map<String, Object> map, Key recordKey, int generation, RecordReadContext<Customer> ctx) {
                assertNotNull(ctx.getSession());
                assertEquals(Customer.class, ctx.getEntityType());
                assertEquals(Customer.class, ctx.getEntityClass());
                assertEquals(session, ctx.getSession());
                return baseMapper.fromMap(map, recordKey, generation);
            }

            @Override
            public Map<String, Object> toMap(Customer element) {
                return baseMapper.toMap(element);
            }

            @Override
            public Object id(Customer element) {
                return baseMapper.id(element);
            }
        };

        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, baseMapper));

        int key = 91004;
        session.delete(args.set.id(key)).execute();

        TypedDataSet<Customer> ds =
            new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        Customer customer = new Customer(key, "ctx-explicit", 30, new Date(),
            new Address("2 Ctx St", "Boulder", "CO", "USA", "80302"));
        session.insert(ds).object(customer).execute();

        List<Customer> out = session.query(ds)
            .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
            .limit(5)
            .execute()
            .toObjectList(capturingMapper);

        assertEquals(1, out.size());
        assertEquals("ctx-explicit", out.get(0).getName());
    }

    @Test
    public void recordResultWithoutHintToObjectThrows() {
        Key key = args.set.id("no-hint-key");
        session.delete(key).execute();
        session.upsert(key).bin("x").setTo(1).execute();
        RecordResult rr = session.query(key).execute().getFirst().orElseThrow();
        assertThrows(IllegalStateException.class, () -> rr.toObject(session));
    }
}
