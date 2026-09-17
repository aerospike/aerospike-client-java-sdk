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

import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.query.IndexType;

/**
 * Demonstrates secondary indexes: the scalar index types ({@code INTEGER}, {@code STRING}), the
 * collection types that index inside a list or map ({@code LIST}, {@code MAPKEYS},
 * {@code MAPVALUES}), an index on a nested element via {@link CTX}, querying through each of
 * them, {@link AerospikeException.IndexException} on a duplicate create, and dropping an index.
 *
 * <p>Index creation is asynchronous on the server. {@code createIndex(...)} returns a task and
 * {@code waitTillComplete()} blocks until every node has finished building, which is what makes
 * the query immediately afterwards deterministic.</p>
 */
public class SecondaryIndexExample extends Example {
    public static final String SET = "sindex-demo";

    static final String AGE_INDEX = "sidx_age";
    static final String CITY_INDEX = "sidx_city";
    static final String TAGS_INDEX = "sidx_tags";
    static final String SCORE_INDEX = "sidx_scores";
    static final String NESTED_INDEX = "sidx_addr_zip";

    /** Every index this example creates. The fixture drops these before and after the run. */
    public static final List<String> INDEX_NAMES =
        List.of(AGE_INDEX, CITY_INDEX, TAGS_INDEX, SCORE_INDEX, NESTED_INDEX);

    @Override
    public void runExample() throws Exception {
        Session session = cluster().createSession(Behavior.DEFAULT);
        DataSet people = dataSet(SET);

        seed(session, people);

        console.info("--- 1) Scalar indexes: INTEGER and STRING ---");
        session.createIndex(people, AGE_INDEX, "age", IndexType.INTEGER, IndexCollectionType.DEFAULT)
                .waitTillComplete();
        session.createIndex(people, CITY_INDEX, "city", IndexType.STRING, IndexCollectionType.DEFAULT)
                .waitTillComplete();

        console.info("people over 30: "
                + count(session, people, Exp.gt(Exp.intBin("age"), Exp.val(30))));
        console.info("people in Berlin: "
                + count(session, people, Exp.eq(Exp.stringBin("city"), Exp.val("Berlin"))));

        console.info("--- 2) Collection index: LIST indexes each element of a list bin ---");
        session.createIndex(people, TAGS_INDEX, "tags", IndexType.STRING, IndexCollectionType.LIST)
                .waitTillComplete();
        console.info("people tagged 'admin': "
                + count(session, people, Exp.gt(
                        ListExp.getByValue(ListReturnType.COUNT, Exp.val("admin"), Exp.listBin("tags")),
                        Exp.val(0))));

        console.info("--- 3) Collection index: MAPVALUES indexes each value of a map bin ---");
        session.createIndex(people, SCORE_INDEX, "scores", IndexType.INTEGER, IndexCollectionType.MAPVALUES)
                .waitTillComplete();
        console.info("created " + SCORE_INDEX + " over the values of the 'scores' map");

        console.info("--- 4) Index a nested element with a CTX path ---");
        session.createIndex(people, NESTED_INDEX, "address", IndexType.STRING,
                        IndexCollectionType.DEFAULT, CTX.mapKey(Value.get("zip")))
                .waitTillComplete();
        console.info("created " + NESTED_INDEX + " over address.zip");

        console.info("--- 5) Reusing an index name for a different bin raises IndexException ---");
        // Re-creating an index with an identical definition is accepted as a no-op, which makes
        // startup code that unconditionally creates its indexes safe. Reusing the name for a
        // different definition is the case that fails.
        try {
            session.createIndex(people, AGE_INDEX, "city", IndexType.STRING, IndexCollectionType.DEFAULT)
                    .waitTillComplete();
            throw new AssertionError("Expected IndexException for a conflicting redefinition");
        }
        catch (AerospikeException.IndexException e) {
            console.info("  caught IndexException (code " + e.getResultCode() + "): " + e.getBaseMessage());
        }

        console.info("--- 6) Drop an index ---");
        session.dropIndex(people, SCORE_INDEX).waitTillComplete();
        console.info("dropped " + SCORE_INDEX);

        console.info("Overall: SUCCESS");
    }

    private static long count(Session session, DataSet dataSet, Exp filter) {
        long found = 0;

        try (RecordStream stream = session.query(dataSet).where(filter).execute()) {
            while (stream.hasNext()) {
                stream.next().orThrow();
                found++;
            }
        }

        return found;
    }

    private static void seed(Session session, DataSet people) {
        record Person(int id, String name, int age, String city, List<String> tags,
                      Map<String, Integer> scores, Map<String, String> address) {
        }

        List<Person> seed = List.of(
            new Person(1, "Alice", 34, "Berlin", List.of("admin", "ops"),
                Map.of("math", 91, "art", 70), Map.of("zip", "10115", "street", "Torstr")),
            new Person(2, "Bob", 28, "Berlin", List.of("dev"),
                Map.of("math", 64, "art", 88), Map.of("zip", "10247", "street", "Boxhagener")),
            new Person(3, "Carol", 45, "Lisbon", List.of("admin", "dev"),
                Map.of("math", 78, "art", 95), Map.of("zip", "1100", "street", "Rua Augusta")),
            new Person(4, "Dan", 22, "Lisbon", List.of("intern"),
                Map.of("math", 55, "art", 61), Map.of("zip", "1200", "street", "Rua do Ouro")));

        for (Person p : seed) {
            session.upsert(people.id(p.id()))
                    .bin("name").setTo(p.name())
                    .bin("age").setTo(p.age())
                    .bin("city").setTo(p.city())
                    .bin("tags").setTo(p.tags())
                    .bin("scores").setTo(p.scores())
                    .bin("address").setTo(p.address())
                    .execute();
        }
    }
}
