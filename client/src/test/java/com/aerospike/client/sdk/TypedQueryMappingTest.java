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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ael.Ael;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.mapper.Address;
import com.aerospike.client.sdk.mapper.Customer;
import com.aerospike.client.sdk.mapper.CustomerMapper;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.query.TypedQueryBuilder;
import com.aerospike.client.sdk.tend.Partition;
import com.aerospike.client.sdk.util.MapUtil;
import com.aerospike.client.sdk.util.Version;

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
    public void typedDatasetQueryReadingOnlyBinsMapsPartialRecord() {
        CustomerMapper customerMapper = new CustomerMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));

        int key = 91024;
        session.delete(args.set.id(key)).execute();

        TypedDataSet<Customer> ds =
            new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        session.insert(ds).object(new Customer(key, "partial-bins", 42, new Date(),
            new Address("1 Partial St", "Boulder", "CO", "USA", "80301"))).execute();

        try (TypedRecordStream<Customer> stream = session.query(ds)
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .readingOnlyBins("name", "age")
                .limit(1)
                .execute()) {
            Customer customer = stream.getFirstObject().orElseThrow();
            assertEquals("partial-bins", customer.getName());
            assertEquals(42, customer.getAge());
            assertEquals(null, customer.getAddress());
        }
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

        try (TypedRecordStream<Customer> rs = session.query(ds.id(k1), ds.id(k2)).execute()) {
            Customer c1 = rs.next().toObject();
            Customer c2 = rs.next().toObject();
            assertEquals(k1, c1.getId());
            assertEquals(k2, c2.getId());
        }
        try (TypedRecordStream<Customer> rs2 = session.queryTypedKeys(List.of(ds.id(k1), ds.id(k2))).execute()) {
            List<Customer> list = rs2.toObjectList();
            assertEquals(2, list.size());
            assertEquals(k1, list.get(0).getId());
            assertEquals(k2, list.get(1).getId());
        }
    }

    @Test
    public void singleTypedKeyPointReadCarriesHintForToObject() {
        CustomerMapper customerMapper = new CustomerMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));

        TypedDataSet<Customer> ds = new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        int k = 91005;
        session.delete(ds.id(k).getKey()).execute();
        session.insert(ds).object(new Customer(k, "point-read", 3, new Date(),
            new Address("a", "b", "CO", "US", "1"))).execute();

        RecordResult rr = session.query(ds.id(k)).execute().getFirst().orElseThrow();
        Customer c = rr.toObject();
        assertEquals(k, c.getId());
        assertEquals("point-read", c.getName());

        try (TypedRecordStream<Customer> stream = session.query(ds.id(k)).execute()) {
            Optional<Customer> opt = stream.getFirstObject();
            assertTrue(opt.isPresent());
            assertEquals("point-read", opt.get().getName());
        }
    }

    @Test
    public void typedKeySingleLegQueryThroughBinBuilderReturnsTypedRecordStream() {
        CustomerMapper customerMapper = new CustomerMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, customerMapper));

        TypedDataSet<Customer> ds = new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        int k = 91022;
        session.delete(ds.id(k).getKey()).execute();
        session.insert(ds).object(new Customer(k, "bin-chain", 6, new Date(),
            new Address("a", "b", "CO", "US", "1"))).execute();

        try (TypedRecordStream<Customer> stream = session.query(ds.id(k)).bin("name").get().execute()) {
            Optional<Customer> opt = stream.getFirstObject();
            assertTrue(opt.isPresent());
            assertEquals("bin-chain", opt.get().getName());
        }
    }

    @Test
    public void typedKeySecondQueryWidensToChainableQueryBuilder() {
        CustomerMapper customerMapper = new CustomerMapper();
        RecordMapper<TagRow> tagMapper = new TagRowMapper();
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(
            Customer.class, customerMapper, TagRow.class, tagMapper));

        TypedDataSet<Customer> cds = new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        TypedDataSet<TagRow> tds = new TypedDataSet<>(args.namespace, args.set.getSet(), TagRow.class);
        int k1 = 91023;
        String k2 = "tag-91023";
        session.delete(cds.id(k1).getKey()).execute();
        session.delete(args.set.id(k2)).execute();

        session.insert(cds).object(new Customer(k1, "row1", 1, new Date(),
            new Address("a", "b", "CO", "US", "1"))).execute();
        session.upsert(args.set.id(k2)).bin("tag").setTo("row2").execute();

        ChainableQueryBuilder widened = session.query(cds.id(k1)).query(tds.id(k2));
        try (RecordStream rs = widened.execute()) {
            Customer c = rs.next().toObject();
            TagRow t = rs.next().toObject();
            assertEquals(k1, c.getId());
            assertEquals("row2", t.tag());
        }
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
    public void untypedRecordStreamMapperOverloadsPassRecordReadContext() {
        CustomerMapper baseMapper = new CustomerMapper();
        RecordMapper<Customer> capturingMapper = new RecordMapper<Customer>() {
            @Override
            public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
                throw new AssertionError("expected 4-arg fromMap with RecordReadContext");
            }

            @Override
            public Customer fromMap(
                    Map<String, Object> map, Key recordKey, int generation, RecordReadContext<Customer> ctx) {
                assertNotNull(ctx.getSession());
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

        int key = 91005;
        Key nativeKey = args.set.id(key);
        session.delete(nativeKey).execute();

        TypedDataSet<Customer> ds =
            new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
        Customer customer = new Customer(key, "ctx-untyped", 31, new Date(),
            new Address("3 Ctx St", "Boulder", "CO", "USA", "80302"));
        session.insert(ds).object(customer).execute();

        RecordReadContext<Customer> ctx = new RecordReadContext<>(session, Customer.class);

        Optional<Customer> first = session.query(nativeKey).execute().getFirst(capturingMapper, ctx);
        assertEquals("ctx-untyped", first.orElseThrow().getName());

        List<Customer> list = session.query(nativeKey).execute().toObjectList(capturingMapper, ctx);
        assertEquals(1, list.size());
        assertEquals("ctx-untyped", list.get(0).getName());

        Optional<Customer> byKey = session.query(nativeKey).execute().get(nativeKey, capturingMapper, ctx);
        assertEquals("ctx-untyped", byKey.orElseThrow().getName());

        Optional<RecordStream.ObjectWithMetadata<Customer>> meta =
            session.query(nativeKey).execute().getFirstWithMetadata(capturingMapper, ctx);
        assertEquals("ctx-untyped", meta.orElseThrow().get().getName());
        assertTrue(meta.get().getGeneration() > 0);
    }

    @Test
    public void typedDatasetQueryWithNoBinsExecuteAsync() {
        installCustomerMapper();
        int key = 91025;
        TypedDataSet<Customer> ds = customerDataSet();
        seedCustomer(ds, key, "no-bins");

        try (TypedRecordStream<Customer> stream = session.query(ds)
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .withNoBins()
                .limit(1)
                .executeAsync(ErrorStrategy.IN_STREAM)) {
            RecordResult rr = stream.getFirst().orElseThrow();
            assertNotNull(rr.getKey());
            assertTrue(rr.recordOrThrow().generation > 0);
            assertNull(rr.recordOrThrow().getString("name"));
        }
    }

    @Test
    public void typedDatasetQueryAlternateWhereClauses() {
        assumeSupportsAel();
        installCustomerMapper();
        int key = 91026;
        TypedDataSet<Customer> ds = customerDataSet();
        seedCustomer(ds, key, "where-overloads");

        assertCustomerName(session.query(ds)
            .where("$.id == " + key)
            .limit(1)
            .execute()
            .getFirstObject()
            .orElseThrow(), "where-overloads");

        PreparedAel prepared = PreparedAel.prepare("$.id == ?0");
        assertCustomerName(session.query(ds)
            .where(prepared, key)
            .limit(1)
            .execute()
            .getFirstObject()
            .orElseThrow(), "where-overloads");

        assertCustomerName(session.query(ds)
            .where(Ael.longBin("id").eq(key))
            .limit(1)
            .execute()
            .getFirstObject()
            .orElseThrow(), "where-overloads");

        Expression expression = Exp.build(Exp.eq(Exp.intBin("id"), Exp.val(key)));
        assertCustomerName(session.query(ds)
            .where(expression)
            .limit(1)
            .execute()
            .getFirstObject()
            .orElseThrow(), "where-overloads");
    }

    @Test
    public void typedDatasetQueryExecuteOverloadSmoke() {
        installCustomerMapper();
        int key = 91027;
        TypedDataSet<Customer> ds = customerDataSet();
        seedCustomer(ds, key, "execute-overloads");

        try (TypedRecordStream<Customer> stream = session.query(ds)
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .limit(1)
                .execute(ErrorStrategy.IN_STREAM)) {
            assertCustomerName(stream.getFirstObject().orElseThrow(), "execute-overloads");
        }

        try (TypedRecordStream<Customer> stream = session.query(ds)
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .limit(1)
                .execute((ignoredKey, ignoredIndex, ex) -> { })) {
            assertCustomerName(stream.getFirstObject().orElseThrow(), "execute-overloads");
        }

        try (TypedRecordStream<Customer> stream = session.query(ds)
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .limit(1)
                .executeAsync((ignoredKey, ignoredIndex, ex) -> { })) {
            assertCustomerName(stream.getFirstObject().orElseThrow(), "execute-overloads");
        }
    }

    @Test
    public void typedDatasetQueryChunkedExecuteOnIsolatedSet() {
        installCustomerMapper();
        TypedDataSet<Customer> ds = chunkedCustomerDataSet();
        int key = 91030;
        seedCustomer(ds, key, "chunk-a");

        Key aerospikeKey = ds.id(key).getKey();
        int partition = Partition.getPartitionId(aerospikeKey.digest);

        try (TypedRecordStream<Customer> stream = session.query(ds)
                .onPartition(partition)
                .readingOnlyBins("name", "id")
                .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .limit(1)
                .chunkSize(1)
                .execute()) {
            int chunkCount = 0;
            int recordCount = 0;
            while (stream.hasMoreChunks()) {
                chunkCount++;
                Optional<Customer> customer;
                while ((customer = stream.popObject()).isPresent()) {
                    assertCustomerName(customer.get(), "chunk-a");
                    recordCount++;
                }
            }
            assertEquals(1, recordCount);
            assertEquals(1, chunkCount);
        }
    }

    @Test
    public void typedDatasetQueryPartitionPinning() {
        installCustomerMapper();
        int key = 91035;
        TypedDataSet<Customer> ds = customerDataSet();
        seedCustomer(ds, key, "partition");

        Key aerospikeKey = ds.id(key).getKey();
        int partition = Partition.getPartitionId(aerospikeKey.digest);

        TypedQueryBuilder<Customer> rangeQb = session.query(ds).onPartitionRange(partition, partition + 1);
        assertEquals(partition, rangeQb.getStartPartition());
        assertEquals(partition + 1, rangeQb.getEndPartition());

        TypedQueryBuilder<Customer> qb = session.query(ds)
            .onPartition(partition)
            .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
            .limit(1);
        assertEquals(partition, qb.getStartPartition());
        assertEquals(partition + 1, qb.getEndPartition());

        assertCustomerName(qb.execute().getFirstObject().orElseThrow(), "partition");
    }

    @Test
    public void typedDatasetQueryBinBuilderOnDataset() {
        assumeTrue(
            cluster.getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_2),
            "dataset bin projection requires server 8.1.2+");

        installCustomerMapper();
        int key = 91033;
        TypedDataSet<Customer> ds = customerDataSet();
        seedCustomer(ds, key, "bin-builder");

        try (RecordStream stream = session.query(ds)
                .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                .bin("name")
                .get()
                .execute()) {
            assertEquals("bin-builder", stream.getFirst().orElseThrow().recordOrThrow().getString("name"));
        }
    }

    @Test
    public void typedDatasetQueryInTransaction() {
        assumeTrue(args.scMode, "transactions require strong consistency");
        installCustomerMapper();
        int key = 91034;
        TypedDataSet<Customer> ds = customerDataSet();
        seedCustomer(ds, key, "in-txn");

        session.doInTransaction(txnSession -> {
            Txn txn = txnSession.getCurrentTransaction();
            assertNotNull(txn);

            try (TypedRecordStream<Customer> stream = txnSession.query(ds)
                    .inTransaction(txn)
                    .where(Exp.eq(Exp.intBin("id"), Exp.val(key)))
                    .execute()) {
                assertCustomerName(stream.getFirstObject().orElseThrow(), "in-txn");
            }
        });
    }

    private void installCustomerMapper() {
        cluster.setRecordMappingFactory(DefaultRecordMappingFactory.of(Customer.class, new CustomerMapper()));
    }

    private TypedDataSet<Customer> customerDataSet() {
        return new TypedDataSet<>(args.namespace, args.set.getSet(), Customer.class);
    }

    private TypedDataSet<Customer> chunkedCustomerDataSet() {
        return new TypedDataSet<>(args.namespace, "typed_chunk_test", Customer.class);
    }

    private void seedCustomer(TypedDataSet<Customer> ds, int key, String name) {
        session.delete(ds.id(key).getKey()).execute();
        session.insert(ds).object(new Customer(key, name, 21, new Date(),
            new Address("1 St", "Boulder", "CO", "USA", "80301"))).execute();
    }

    private static void assertCustomerName(Customer customer, String expectedName) {
        assertEquals(expectedName, customer.getName());
    }

    @Test
    public void recordResultWithoutHintToObjectThrows() {
        Key key = args.set.id("no-hint-key");
        session.delete(key).execute();
        session.upsert(key).bin("x").setTo(1).execute();
        RecordResult rr = session.query(key).execute().getFirst().orElseThrow();
        assertThrows(IllegalStateException.class, () -> rr.toObject());
    }

    public record TagRow(String tag) {
    }

    public static final class TagRowMapper implements RecordMapper<TagRow> {
        @Override
        public TagRow fromMap(Map<String, Object> map, Key recordKey, int generation) {
            return new TagRow(MapUtil.asString(map, "tag"));
        }

        @Override
        public Map<String, Object> toMap(TagRow element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object id(TagRow element) {
            throw new UnsupportedOperationException();
        }
    }
}
