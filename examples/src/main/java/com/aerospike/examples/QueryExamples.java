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
package com.aerospike.examples;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.DefaultRecordMappingFactory;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.NavigatableRecordStream;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypeSafeDataSet;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.Log.Level;
import com.aerospike.client.sdk.ael.Dsl;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.info.classes.NamespaceDetail;
import com.aerospike.client.sdk.info.classes.Sindex;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.query.SortDir;
import com.aerospike.client.sdk.query.SortProperties;
import com.aerospike.client.sdk.task.ExecuteTask;
import com.aerospike.client.sdk.util.MapUtil;

public class QueryExamples {
    public static class Address {
        private final String line1;
        private final String city;
        private final String state;
        private final String country;
        private final String zipCode;

        public Address(String line1, String city, String state, String country, String zipCode) {
            super();
            this.line1 = line1;
            this.city = city;
            this.state = state;
            this.country = country;
            this.zipCode = zipCode;
        }

        public String getLine1() {
            return line1;
        }
        public String getCity() {
            return city;
        }

        public String getState() {
            return state;
        }
        public String getCountry() {
            return country;
        }
        public String getZipCode() {
            return zipCode;
        }
        @Override
        public String toString() {
            return "Address [line1=" + line1 + ", city=" + city + ", state=" + state + ", country=" + country + ", zipCode="
                    + zipCode + "]";
        }
    }

    public static class Customer {
        private long id;
        private String name;
        private int age;
        private Date dob;
        private Address address;

        public Customer() {
            super();
        }
        public Customer(long id, String name, int age, Date dob) {
            this(id, name, age, dob, null);
        }
        public Customer(long id, String name, int age, Date dob, Address address) {
            super();
            this.id = id;
            this.name = name;
            this.age = age;
            this.dob = dob;
            this.address = address;
        }

        public long getId() {
            return id;
        }
        public void setId(long id) {
            this.id = id;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public int getAge() {
            return age;
        }
        public void setAge(int age) {
            this.age = age;
        }
        public Date getDob() {
            return dob;
        }
        public void setDob(Date dob) {
            this.dob = dob;
        }

        public Address getAddress() {
            return address;
        }
        public void setAddress(Address address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return "Customer [id=" + id + ", name=" + name + ", age=" + age + ", dob=" + dob + ", address=" + address + "]";
        }
    }

    public static class AddressMapper implements RecordMapper<Address> {

        @Override
        public Address fromMap(Map<String, Object> map, Key recordKey, int generation) {
            Address result = new Address(
                    MapUtil.asString(map, "line1"),
                    MapUtil.asString(map, "city"),
                    MapUtil.asString(map, "state"),
                    MapUtil.asString(map, "country"),
                    MapUtil.asString(map, "zip"));
            return result;
        }

        @Override
        public Map<String, Value> toMap(Address addr) {
            return MapUtil.buildMap()
                    .add("line1", addr.getLine1())
                    .add("city", addr.getCity())
                    .add("state", addr.getState())
                    .add("country", addr.getCountry())
                    .add("zip", addr.getZipCode())
                    .done();
        }

        @Override
        public Object id(Address element) {
            return null;
        }

    }

    public static class CustomerMapper implements RecordMapper<Customer> {
        @Override
        public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
            Customer result = new Customer();
            result.setId(recordKey.userKey.toLong());
            result.setAge(MapUtil.asInt(map, "age"));
            result.setDob(MapUtil.asDateFromLong(map, "dob"));
            result.setName(MapUtil.asString(map, "name"));
            result.setAddress(MapUtil.asObjectFromMap(map, "address", new AddressMapper()));
            return result;
        }

        @Override
        public Map<String, Value> toMap(Customer customer) {
            return MapUtil.buildMap()
                    .add("id", customer.getId())
                    .add("age", customer.getAge())
                    .addAsLong("dob", customer.getDob())
                    .add("name", customer.getName())
                    .add("address", customer.getAddress(), new AddressMapper())
                    .done();
        }

        @Override
        public Object id(Customer customer) {
            return customer.getId();
        }
    }
    public static void print(RecordStream recordStream) {
        int count = 0;
        while (recordStream.hasNext()) {
            RecordResult key = recordStream.next();
            System.out.printf("%5d - Key: %s, Value: %s\n", (++count), key.key(), key);
        }
    }

    @SuppressWarnings("unused")
	public static void main(String[] args) {
        try (Cluster cluster = new ClusterDefinition("localhost", 3100)
                .usingServicesAlternate()
                .withNativeCredentials("admin", "password123")
                .preferringRacks(1)
                .withLogLevel(Level.DEBUG)
                .withSystemSettings(builder -> builder
                    .circuitBreaker(ops -> ops.maximumErrorsInErrorWindow(200))
                    .connections(conn -> conn
                            .minimumConnectionsPerNode(200)
                            .maximumConnectionsPerNode(200)
                    )
                )
                .connect()) {

            CustomerMapper customerMapper = new CustomerMapper();

            cluster.setRecordMappingFactory(new DefaultRecordMappingFactory(Map.of(
                        Customer.class, customerMapper,
                        Address.class, new AddressMapper()
                    )
                ));

            Behavior newBehavior = Behavior.DEFAULT.deriveWithChanges("newBehavior", builder ->
                builder.on(Selectors.all(), ops -> ops
                        .waitForSocketResponseAfterCallFails(Duration.ofSeconds(3))
                )
                .on(Selectors.reads().ap(), ops -> ops
                        .waitForCallToComplete(Duration.ofMillis(25))
                        .abandonCallAfter(Duration.ofMillis(100))
                        .maximumNumberOfCallAttempts(3)
                )
                .on(Selectors.reads().query(), ops -> ops
                        .waitForCallToComplete(Duration.ofSeconds(2))
                        .abandonCallAfter(Duration.ofSeconds(30))
                )
                .on(Selectors.reads().batch(), ops -> ops
                        .maximumNumberOfCallAttempts(7)
                        .allowInlineMemoryAccess(true)
                )
            );
            Behavior childBehavior = newBehavior.deriveWithChanges("child", builder ->
                builder.on(Selectors.writes().batch(), ops -> ops
                    .allowInlineSsdAccess(true)
                    .maxConcurrentNodes(5)
                )
                .on(Selectors.reads().ap(), ops -> ops
                    .maximumNumberOfCallAttempts(8)
                )
            );
            Behavior nonExceptionBehvaior = Behavior.DEFAULT.deriveWithChanges("nonException", builder ->
                builder.on(Selectors.all(), ops -> ops.stackTraceOnException(false)));

            TypeSafeDataSet<Customer> customerDataSet = TypeSafeDataSet.of("test", "person", Customer.class);
//            DataSet customerDataSet = DataSet.of("test", "person");

            Session session = cluster.createSession(newBehavior);
            Session sessionWithoutExceptions = cluster.createSession(nonExceptionBehvaior);

            Set<String> namespaces = session.info().namespaces();
            namespaces.forEach(ns -> {
                Optional<NamespaceDetail> details = session.info().namespaceDetails(ns);
                details.ifPresent(System.out::println);
            });

            List<Sindex> sindexes = session.info().secondaryIndexes();
            System.out.println(sindexes);
            sindexes.forEach(sindex -> {
                System.out.printf("Secondary index: %s\n", sindex);
                System.out.println("   " + session.info().secondaryIndexDetails(sindex));
            });

            session.truncate(customerDataSet);

            try {
                session.update(customerDataSet.id(1)).execute();
            }
            catch (AerospikeException ae) {
                System.out.printf("Exception caught as expected: %s (%s)\n", ae.getMessage(), ae.getClass());
            }
            try {
                session.update(customerDataSet.id(1)).bin("bob").setTo(5).execute();
            }
            catch (AerospikeException ae) {
                System.out.printf("Exception caught as expected on node %s: %s (%s)\n", ae.getNode(), ae.getMessage(), ae.getClass());
            }
            RecordStream rss = session.update(customerDataSet.id(1)).bin("bob").setTo(5).executeAsync(ErrorStrategy.IN_STREAM);
//            rss.getFirst().ifPresent(rr -> System.out.println(rr.));
            System.out.println(rss.getFirst());


            System.exit(0);

            session.insert(customerDataSet.id(1))
                .bin("Name").setTo("test1")
                .bin("i1").setTo(1)
                .bin("i2").setTo(2)
                .bin("f1").setTo(1.1)
                .bin("f2").setTo(2.2)
                .bin("s1").setTo("hello ")
                .bin("s2").setTo("world")
                .execute();

//            print(session.query(customerDataSet.id(1))
//                .bin("calc0").selectFrom("1.0 == $.f1")
//                .bin("calc0").selectFrom("$.f1 == 1.0 and $.i1 == 2")
//                .bin("calc0").selectFrom("1.0 == $.f1.toFloat()")
//                .bin("calc0").selectFrom("1.0 == $.f1.nonsense()")
//                .bin("calc0").selectFrom("$.f1 == 15.0 and $.f1 == 'String'")
//                .bin("calc0").selectFrom("1.0 + $.f1")
//                .bin("calc0").selectFrom("1.0 + $.i1.get(type: INT).asFloat()")
//                .bin("calc0").selectFrom("$.i1 + $.i2")
//                .bin("calc0").selectFrom("$.f1 + $.f2")
//                .bin("calc0").selectFrom("$.f1 and $.f2 and $.i2 and $.i1")
//                .bin("calc1").selectFrom("$.i1.asFloat() + 4.0")
//                .bin("calc2").selectFrom("1.2 + $.f1 + $.f2 + 1.0")
//                .bin("calc3").selectFrom("$.s1 == 'Tim'")
//                .bin("calc0").selectFrom("$.f1.get(type: FLOAT) + $.f2")
//                .execute());
//            System.exit(0);

            session.upsert(customerDataSet.id("bob")).bin("A").setTo(2).bin("B").setTo(2.2).execute();
            RecordStream rs1 = session.query(customerDataSet.id("bob"))
                    .readingOnlyBins("name")
                    .execute();
            System.out.println(rs1.getFirst());

            session.upsert(customerDataSet.ids(1,2,3,4,5)).bin("holdings").add(1).execute();
            session.upsert(customerDataSet)
                    .bins("name", "age")
                    .id(1).values("Tim", 312)
                    .id(2).values("Bob", 25)
                    .id(3).values("Jane", 46)
                    .execute();

            System.out.printf("id(2) exists: %b\n", session.exists(customerDataSet.ids(2)).execute().getFirst());
            session.delete(customerDataSet.ids(2)).durablyDelete(false).execute();
//            System.out.printf("id(2) exists: %b\n", session.exists(customerDataSet.ids(2)).execute().getFirst());

            DataSet users = DataSet.of("test", "users");

            RecordStream result = session.upsert(customerDataSet.id(80))
                    .bin("name").setTo("Tim")
                    .bin("age").setTo(342)
                    .execute();
            System.out.println(result.getFirst());

            session.upsert(customerDataSet.ids(81, 82))
                    .bin("name").setTo("Tim")
                    .bin("age").setTo(343)
                    .execute();
            session.upsert(customerDataSet)
                    .bins("name", "age")
                    .id(83).values("Tim", 342)
                    .execute();
            session.upsert(customerDataSet)
                    .bins("name", "age")
                    .id(84).values("Tim", 342)
                    .id(85).values("Fred", 37)
                    .execute();

//            sessionWithoutExceptions.insert(customerDataSet.id(80))
//                    .bin("name").setTo("Bob")
//                    .execute()
//                    .getFirst();

            session.upsert(customerDataSet.id(100))
                    .bin("name").setTo("Tim")
                    .bin("age").setTo(312)
                    .bin("dob").setTo(new Date().getTime())
                    .bin("id2").setTo(100)
                    .expireRecordAt(LocalDateTime.of(2030, 1, 1, 0, 0))
                    .execute();

            session.delete(customerDataSet.ids(900, 901, 902, 903, 904, 905)).execute();

            session.insert(customerDataSet)
                    .bins("name", "age", "hair", "dob")
                    .id(899).values("Tim", 312, "brown", new Date().getTime());

            RecordStream values = session.insert(customerDataSet)
                    .bins("name", "age", "hair", "dob")
                    .id(900).values("Tim", 312, "brown", new Date().getTime())
                    .id(901).values("Jane", 28, "blonde", new Date().getTime())
                    .id(902).values("Bob", 54, "brown", new Date().getTime()).expireRecordAfter(Duration.ofDays(5))
                    .id(903).values("Jordan", 45, "red", new Date().getTime())
                    .id(904).values("Alex", 67, "blonde", new Date().getTime())
                    .id(905).values("Sam", 24, "brown", new Date().getTime())
                    .defaultExpireRecordAfter(Duration.ofDays(30))
                    .execute();
            values.forEach(kr -> System.out.printf("%s -> %s\n", kr.key(), kr.recordOrThrow()));

            for (int i = 0; i < 15; i++) {
                session.upsert(customerDataSet.id(i))
                    .bin("name").setTo("Tim-" + i)
                    .bin("age").setTo(312+i)
                    .bin("hair").setTo("brown")
                    .bin("dob").setTo(new Date().getTime())
                    .execute();

                session.upsert(customerDataSet)
                    .bins("name", "age", "hair", "dob")
                    .id(1000+i).values("Tim-"+i, 312+i, "brown", new Date().getTime())
                    .expireRecordAfter(Duration.ofDays(30))
                    .execute();
//              .values("Jane", 28, "blonde", new Date().getTime())
            }

            session.delete(customerDataSet.ids(1,2,3,5,7,11,13,17)).execute();

            session.delete(customerDataSet.id(102)).execute();

            session.insert(customerDataSet.id(102))
                .bin("name").setTo("Sue")
                .bin("age").setTo(27)
                .bin("id").setTo(102)
                .bin("dob").setTo(new Date().getTime())
                .execute();

            session.update(customerDataSet.id(102))
                .bin("age").setTo(26)
                .execute();

            session.delete(customerDataSet.id(102)).execute();

            session.upsert(customerDataSet.id(102))
                .bin("name").setTo("Sue")
                .bin("age").setTo(27)
                .bin("id").setTo(102)
                .bin("dob").setTo(new Date().getTime())
                .bin("rooms").setTo(Map.of(
                        "room1", Map.of("occupied", false, "rates", Map.of(1, 100, 2, 150, 3, -1)),
                        "room2", Map.of("occupied", true, "rates", Map.of(1, 90, 2, -1, 3, -1)),
                        "room3", Map.of("occupied", false, "rates", Map.of(1, 67, 2, 200, 3, 99)),
                        "room4", Map.of("occupied", true, "rates", Map.of(1, 98, 2, -1, 3, -1)),
                        "room5", Map.of("occupied", false, "rates", Map.of(1, 98, 2, -1, 3, -1)),
                        "room6", Map.of("occupied", true, "rates", Map.of(1, 98, 2, -1, 3, -1))
                    ))
                .bin("rooms2").setTo(Map.of("test", true))
                .execute();


            RecordStream results = session.upsert(customerDataSet.id(102))
                .bin("name").setTo("Bob")
                .bin("age").setTo(30)
                .bin("id").get()
                .bin("dob").setTo(new Date().getTime())
//                .bin("rooms").onMapKey("room1").update("Hello")
                .bin("rooms").onMapIndex(2).getValues()
                .bin("rooms").onMapKeyRange("room1", "room2").countAllOthers()
                .bin("rooms").onMapKey("room1").getValues()
                .bin("rooms").onMapKeyRange("room1", "room3").count()
                .bin("rooms").onMapKey("room1").onMapKey("rates").onMapKey(1).setTo(110)
                .bin("rooms").onMapKey("room2").mapClear()
                .bin("rooms").onMapKeyRange("room4", "room9").remove()
                .bin("rooms").onMapKey("room1").onMapKey("rates").onMapKey(1).add(5)
                .bin("rooms").onMapKeyRelativeIndexRange("bob", -1, 1).getKeysAndValues()
                .bin("rooms2").mapClear()
                .bin("rooms2").onMapKey("child", MapOrder.KEY_ORDERED).onMapKey("subChild").setTo(5)
                // TODO: How to insert an element into a list which doesn't exist?
                // TODO: Should complex operations return one item per call?
//                .bin("rooms2").onMapKey("child", MapOrder.KEY_ORDERED).onListIndex(0, ListOrder.UNORDERED, false).listAdd(5)
                .execute();

            System.out.println(results.getFirst());
            System.out.println(session.query(customerDataSet.id(102)).execute().getFirst());
            session.update(customerDataSet.id(102))
                .bin("name").append("-test")
                .bin("age").add(1)
                .execute();
            System.out.println(session.query(customerDataSet.id(102)).execute().getFirst());


            session.upsert(customerDataSet.id(102))
                .bin("name").setTo("Sue")
                .bin("age").setTo(26)
                .bin("dob").setTo(new Date().getTime())
                .execute();

            List<Customer> customers = List.of(
                    new Customer(20, "Jordan", 36, new Date()),
                    new Customer(21, "Alex", 27, new Date()),
                    new Customer(22, "Betty", 27, new Date()),
                    new Customer(23, "Bob", 33, new Date()),
                    new Customer(24, "Fred", 6, new Date()),
                    new Customer(25, "Alex", 28, new Date()),
                    new Customer(26, "Alex", 26, new Date()),
                    new Customer(27, "Jordan", 19, new Date()),
                    new Customer(28, "Gruper", 28, new Date()),
                    new Customer(29, "Bree", 24, new Date()),
                    new Customer(30, "Perry", 44, new Date()),
                    new Customer(31, "Alex", 27, new Date()),
                    new Customer(32, "Betty", 27, new Date()),
                    new Customer(33, "Wilma", 18, new Date()),
                    new Customer(34, "Joran", 82, new Date()),
                    new Customer(35, "Alex", 27, new Date()),
                    new Customer(36, "Fred", 99, new Date()),
                    new Customer(37, "Sydney", 22, new Date()),
                    new Customer(38, "Ita", 99, new Date()),
                    new Customer(39, "Rupert", 83, new Date()),
                    new Customer(40, "Dominic", 53, new Date()),
                    new Customer(41, "Tim", 27, new Date()),
                    new Customer(42, "Tim", 29, new Date()),
                    new Customer(43, "Tim", 31, new Date()),
                    new Customer(44, "Tim", 30, new Date()),
                    new Customer(45, "Tim", 33, new Date()),
                    new Customer(46, "Tim", 35, new Date())
                );

            session.insert(customerDataSet)
                .objects(customers)
                .using(customerMapper)
                .execute();

            System.out.println("Updating all customers called Tim");
            print(session.update(customerDataSet)
                .where("$.name == 'Tim'")
                .objects(customers)
                .using(customerMapper)
                .execute());

            System.out.printf("Customer 46 age before scan: %d\n",
                    session.query(customerDataSet.id(46)).execute().getFirstRecord().getInt("age"));

            ExecuteTask task = session.backgroundTask().update(customerDataSet)
                .bin("age").add(1)
                .execute();

            System.out.printf("task id = %d\n", task.getTaskId());
            task.waitTillComplete();
            System.out.printf("Customer 46 age after scan: %d\n",
                    session.query(customerDataSet.id(46)).execute().getFirstRecord().getInt("age"));

            // Batch partition filter test
            List<Key> keys = customerDataSet.ids(IntStream.rangeClosed(20, 48).toArray());
            System.out.println("Read 25 records, but only those in partitions 0->2047");
            print(session.query(keys)
                    .onPartitionRange(0, 2048)
                    .execute());

            System.out.println("Full batch read:");
            print(session.query(keys).execute());

            System.out.println("\nBatchRead where name = 'Tim':");
            print(session.query(keys).where("$.name == 'Tim'").execute());

            System.out.println("\nBatchRead where name = 'Tim':");
            print(session.query(keys).includeMissingKeys().where("$.name == 'Tim'").execute());

            System.out.println("\nBatchRead where name = 'Tim':");
            print(session.query(keys).where("$.name == 'Tim'").includeMissingKeys().failOnFilteredOut().execute());

//            System.out.println("Read the set, limit 6, test than includeMissingKeys() gives a compile error");
//            print(session.query(customerDataSet).includeMissingKeys().execute());
//            print(session.query(customerDataSet).failOnFilteredOut().execute());


            System.out.println("Read the set, limit 6");
            print(session.query(customerDataSet).limit(6).execute());

            List<Key> keyList2 = customerDataSet.ids(20,21,22,23,24,25,26,27);
            RecordStream thisStream = session.update(keyList2)
                   .bin("age").add(1)
                   .execute();

            System.out.println("Showing results before guaranteeing execution has finished.");
            print(session.query(keyList2).execute());
            System.out.println("Showing async results");
            print(thisStream);
            System.out.println("Showing results now execution has finished.");
            print(session.query(keyList2).execute());


            System.out.printf("Update people in list whose age is < 35 (%s)\n", keyList2);
            print(session.update(keyList2)
                   .bin("age").add(1)
                   .where("$.age < 35")
                   .execute());
            print(session.query(keyList2).execute());

            session.update(keyList2)
                    .bin("age").add(1)
                    .where("$.age < 35")
                    .failOnFilteredOut()
                    .execute();
             print(session.query(keyList2).execute());

            // Query contract:
            // - If a list of ids is provided and there is not "sort" clause, the records in the stream are returned in the order which the ids are specified
            // - If we're processing the records with notifiers, we cannot also get them back in a stream
            // Should there be a KeyRecord style class with an error code on it and inDoubt? (Similar to Batch Record, but this violates SOLID by being used both for
            // input and output)

            // Need a class to turn a resultcode into an exception similar to SQLExceptionTranslator in Spring Boot. These are customizable in `sql-error-codes.xml`
            // file, eg:
            // <error-codes>
            //   <database-product-name>PostgreSQL</database-product-name>
            //   <duplicate-key-codes>23505</duplicate-key-codes>
            //   <data-integrity-violation-codes>23000,23502,23503</data-integrity-violation-codes>
            // </error-codes>


            // Threads are really cheap in JDK 21+ so all calls could notify async via a new thread and an ArrayBlockingQueue for example?
//            session.query(customerDataSet)
//                    .onRecordArrival(keyRecord -> System.out.println(keyRecord))
//                    .onRecordError()
//                    .onDone(() -> System.out.println("Done"))
//                    .execute();

            System.out.println("\nRead point records - in the same order as the keys, limit to 3");
            print(session.query(customerDataSet.ids(1,3,5,7)).limit(3).execute());

            System.out.println("\nSingle point record");
            print(session.query(customerDataSet.ids(6)).execute());

            System.out.println("Read the set, output as stream, limit of 5");
            session.query(customerDataSet).limit(5).execute()
                    .stream()
                    .map(keyRec -> "Name: " + keyRec.recordOrThrow().getString("name"))
                    .forEach(str -> System.out.println(str));

            System.out.println("Read header, point read");
            print(session.query(customerDataSet.id(6)).withNoBins().execute());
            System.out.println("Read header, batch read");
            print(session.query(customerDataSet.ids(6,7,8)).withNoBins().execute());
            System.out.println("Read header, set read");
            print(session.query(customerDataSet).withNoBins().execute());

            System.out.println("Read with select bins, point read");
            print(session.query(customerDataSet.ids(6)).readingOnlyBins("name", "age").execute());
            System.out.println("Read with select bins, batch read");
            print(session.query(customerDataSet.ids(6,7,8)).readingOnlyBins("name", "age").execute());
            System.out.println("Read with select bins, set read");
            print(session.query(customerDataSet).readingOnlyBins("name", "age").execute());

//            session.update(customerDataSet.ids(1,2,3,4))
//                    .bin

            // Throw an exception
            try {
                print(session.query(customerDataSet.ids(6,7,8)).readingOnlyBins("name", "age").withNoBins().execute());
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // TODO: Put transaction control into policies
//            session.doInTransaction(txnSession -> {
//                Optional<RecordResult> recResult = txnSession.query(customerDataSet.id(1)).execute().getFirst();
//                if (true) {
//                    txnSession.insert(customerDataSet.id(3));
//                }
//                txnSession.delete(customerDataSet.id(3));
//                txnSession.insert(customerDataSet.id(3)).notInAnyTransaction().execute();
//            });

            customers = session.query(customerDataSet.ids(20, 21)).execute().toObjectList(customerMapper);
            System.out.println(customers);

            // Records-per-second check
            RecordStream queryResults = session.query(customerDataSet).recordsPerSecond(1).execute();
            queryResults.forEach(rr -> System.out.println(rr.recordOrThrow()));
            // session.query(customerDataSet.id(1)).recordsPerSecond(100).execute();

            // Server-side chunking example - fetch records in chunks of 20
            customers = session.query(customerDataSet).chunkSize(20).execute().toObjectList(customerMapper);
            System.out.println(customers);

            // Server-side chunking example - process records in chunks of 10
            RecordStream rs = session.query(customerDataSet).chunkSize(10).execute();
            int chunk = 0;
            while (rs.hasMoreChunks()) {
                System.out.println("Chunk: " + (++chunk));
                rs.forEach(rec -> System.out.println(rec));
            }

            int total = session.query(customerDataSet)
                .execute()
                .stream()
                .mapToInt(kr -> kr.recordOrThrow().getInt("quantity"))
                .sum();
            System.out.println("\n\nSorting customers by Name with a where clause using NavigatableRecordStream");
            customers = session.query(customerDataSet)
                    .where("$.name == 'Tim' and $.age > 30")
                    .limit(1000)
                    .execute()
                    .asNavigatableStream()
                    .sortBy("name", SortDir.SORT_ASC, true)
                    .toObjectList(customerMapper);

            for (Customer customer : customers) {
                System.out.println(customer);
            }

            System.out.println("End sorting customers by Name with a where clause\n");

            customers = session.query(customerDataSet)
                    .where(Dsl.stringBin("name").eq("Tim").and(Dsl.longBin("age").gt(30)))
                    .limit(1000)
                    .execute()
                    .asNavigatableStream()
                    .sortBy("name", SortDir.SORT_ASC, true)
                    .toObjectList(customerMapper);
            for (Customer customer : customers) {
                System.out.println(customer);
            }

            System.out.println("---- End sort ---");


            System.out.println("\n\nSorting customers by Age (desc) then name (asc), using NavigatableRecordStream for client-side pagination");
            try (NavigatableRecordStream navStream = session.query(customerDataSet)
                    .limit(13)
                    .execute()
                    .asNavigatableStream()
                    .sortBy(List.of(
                        SortProperties.descending("age"),
                        SortProperties.ascendingIgnoreCase("name")
                    ))
                    .pageSize(5)) {

                int page = 0;
                while (navStream.hasMorePages()) {
                    System.out.println("---- Page " + (++page) + " -----");
                    customers = navStream.toObjectList(customerMapper);
                    customers.forEach(cust -> System.out.println(cust));
                }
                System.out.println("---- End sort ---");

                // Jump to page 2
                System.out.println("--- Setting page to 2 ---");
                navStream.setPageTo(2);
                navStream.forEach(rec -> System.out.println(rec));
                System.out.println("--- done with page 2 ---");

                // Now re-sort by name only
                System.out.println("Re-sorting records by name");
                navStream.sortBy(SortProperties.ascending("name"));
                int pageNum = 0;
                while (navStream.hasMorePages()) {
                    System.out.println("---- Page " + (++pageNum) + " -----");
                    List<Customer> custList = navStream.toObjectList(customerMapper);
                    custList.forEach(cust -> System.out.println(cust));
                }
                System.out.println("---- End sort ---");
            }

            // -------------
            // UDF Calls
            // -------------
//            Object udfResult = session.executeUdf(customerDataSet.id(1)).function("pkg", "myFunc").execute().getFirstUdfResult();
//            System.out.println("UDF Result = " + udfResult);

            // ---------------------------
            // TTL Test
            // ---------------------------
            System.out.println("--- Test TTL ---");
            session.delete(customerDataSet.id(1)).execute();

            session.upsert(customerDataSet)
                .bins("binA")
                .id(1).values(5)
                .expireRecordAfterSeconds(5)
                .execute();
            System.out.println("Initial read, should be there");
            System.out.println(session.query(customerDataSet.id(1)).execute().getFirst());
            try {
                Thread.sleep(Duration.ofSeconds(6));
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println("Read after TTL exires, should not be there");
            System.out.println(session.query(customerDataSet.id(1)).execute().getFirst());

            // ---------------------------
            // Read and write expressions
            // ---------------------------
            System.out.println("--- Expression testing ---");
            session.replace(customerDataSet.id(223))
                .bin("age").setTo(500)
                .bin("value").setTo(123)
                .execute();

            System.out.printf("Base record: %s\n",
                    session.query(customerDataSet.id(223)).execute().getFirst());

            print(session.query(customerDataSet.id(223))
                .bin("age").get()
                .execute());
            System.out.println("Using a read expression");

            rs = session.query(customerDataSet.ids(223))
                .bin("bob").selectFrom("$.age + $.value", arg -> arg.ignoreEvalFailure())
                .execute();
            print(rs);

            System.out.println("Using a write expression");
            session.upsert(customerDataSet.id(1))
                .bin("age").setTo(50)
                .bin("value").setTo(10)
                .bin("c2").upsertFrom("$.age + 2 * $.value", arg -> arg.ignoreOpFailure().deleteIfNull())
                .execute();
            session.update(customerDataSet.id(223))
                .bin("bob").upsertFrom("$.age + 2 * $.value")
                .execute();
            System.out.printf("Modified record: %s\n",
                    session.query(customerDataSet.id(223)).execute().getFirst());


            session.query(customerDataSet)
                .readingOnlyBins("name", "age")
                .execute();

            // ---------------------------
            // Query hints
            // ---------------------------
            System.out.println("\n--- Query hints ---");

            // Hint with index name: tell the server to use a specific secondary index
            session.query(customerDataSet)
                .where("$.age > 30")
                .withHint(hint -> hint.forIndex("age_idx"))
                .execute();

            // Hint with bin name: prefer the secondary index on a given bin
            session.query(customerDataSet)
                .where("$.age > 30")
                .withHint(hint -> hint.forBin("age"))
                .execute();

            // Hint with query duration only: override expected duration
            session.query(customerDataSet)
                .where("$.age > 30")
                .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
                .execute();

            // Hint combining index name and query duration
            session.query(customerDataSet)
                .where("$.age > 30")
                .withHint(hint -> hint.forIndex("age_idx").queryDuration(QueryDuration.SHORT))
                .execute();

            // Hint combining query duration first, then bin name
            session.query(customerDataSet)
                .where("$.age > 30")
                .withHint(hint -> hint.queryDuration(QueryDuration.SHORT).forBin("age"))
                .execute();

            // ---------------------------
            // Background query operations
            // ---------------------------
            session.backgroundTask().update(customerDataSet)
                .bin("age").add(1)
                .bin("bob").upsertFrom("$.age + $.value")
                .where("$.state == 'nsw'")
                .execute();

            // ------------------------
            // Multi operation batches
            // ------------------------
            RecordStream rsStream = session
                .update(customerDataSet.ids(1000, 1001))
                    .bin("age").add(1)
                    .bin("dob").setTo(new Date().getTime())
//                    .where("$.age > 100")
                    .expireRecordAfter(Duration.ofMinutes(5))
                .exists(customerDataSet.ids(1000,1001))
                .query(customerDataSet.ids(10,12))
                .delete(customerDataSet.id(1003))
                .notInAnyTransaction()
                .inTransaction(null)
//                .defaultWhere("$.value > 200")
                .defaultExpireRecordAfter(Duration.ofMinutes(20))
                .execute();
            System.out.println("Multi operations:");
            print(rsStream);

            rsStream = session.query(customerDataSet.ids(1,2,3))
                        .bin("name").get()
                        .bin("map").onMapKeyRange(5, 10).getKeysAndValues()
                    .update(customerDataSet.ids(1))
                        .bin("age").add(1)
                    .execute();

            rsStream = session
                    .update(customerDataSet.ids(1,2,3))
                        .bin("age").add(1)
                        .bin("updated").setTo(true)
                        .where("$.age < 21")
                    .delete(customerDataSet.ids(11,12,13,14,15))
                    .update(customerDataSet.ids(5,6,7))
                        .bin("luckyWinner").setTo("true")
                    .defaultWhere("$.updated == false")
                    .execute();

            rsStream = session.query(customerDataSet.ids(1,2,3))
                            .limit(2)
                    .update(customerDataSet.ids(4,5,6))
                        .bin("name").setTo("bob")
                        .expireRecordAfterSeconds(500)
                    .query(customerDataSet.id(7))
                    .execute();

            // --------------------
            // Object mapping
            // --------------------
            // Insert then read back a customer with an address
            System.out.println("\n--- Object mapping test ----");
            Customer sampleCust = new Customer(999, "sample", 456, new Date(), new Address("123 Main St", "Denver", "CO", "USA", "80112"));
            System.out.println("Reference customer: " + sampleCust);

            session.delete(customerDataSet.id(999)).execute();
            session.insert(customerDataSet).object(sampleCust).execute();
            Customer readCustomer = session.query(customerDataSet.id(999)).execute().toObjectList(customerMapper).get(0);
            System.out.println("Customer read back: " + readCustomer);
            System.out.println("--- End object mapping test ----");

            session.query(customerDataSet).where("$.name == 'Tim'")
                .bin("fred").get()
                .execute();

            // -----------------------
            // Object mapping (nested)
            // -----------------------
            Address address1 = new Address("123 Main St", "Denver", "CO", "USA", "80000");
            Customer customer = new Customer(1, "Bob", 37, new Date(), null);
            session.replace(customerDataSet).object(customer).using(customerMapper).execute();
            session.upsert(customerDataSet.id(customer.getId())).bin("addrs").onMapKey("home").upsert(address1, new AddressMapper());

            // ----------------
            // Generation check
            // ----------------
            System.out.println("\n--- Generation check test ----");

            RecordStream data = session.query(customerDataSet.id(999)).execute();
            data.getFirst().ifPresent(keyRecord -> {
                int generation = keyRecord.recordOrThrow().generation;
                System.out.println("   Read record with generation of " + generation);
                session.update(customerDataSet.id(999))
                        .bin("gen").setTo(generation)
                        .ensureGenerationIs(generation)
                        .execute();
                System.out.println("   First update was successful");

                try {
                    // Second update should fail with a generation exception
                    session.update(customerDataSet)
                        .object(readCustomer)
                        .ensureGenerationIs(generation)
                        .execute();
                    System.out.println("   Second update was successful -- this is an error");

                }
                catch (AerospikeException ae) {
                    System.out.println("   Second update failed as expected");
                    System.out.println(ae.getResultCode() == ResultCode.GENERATION_ERROR);
                }
            });
        }
    }
}
