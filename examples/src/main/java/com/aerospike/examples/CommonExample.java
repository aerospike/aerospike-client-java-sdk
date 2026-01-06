/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import java.util.Optional;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.dsl.Dsl;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.IndexCollectionType;
import com.aerospike.client.fluent.task.ExecuteTask;

/**
 * Examples for common commands.
 */
public class CommonExample extends Example {
    public CommonExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        Session session = cluster.createSession(Behavior.DEFAULT);
        DataSet set = DataSet.of("test", "set");

        System.out.println("Truncate records");

        session.truncate(set);

        System.out.println("Write 1 record");

        session.upsert(set.ids(10))
            .bins("name", "age")
            .values("Charlie", 11)
            .execute();

        System.out.println("Write 1 record async");

        RecordStream rs = session.upsert(set.ids(100))
            .bins("name", "age")
            .values("Charlie", 999)
            .executeAsync();

        //System.out.println(rs.getFirst());

        System.out.println("Write 3 records");

        session.upsert(set.ids(1,2,3))
            .bins("name", "age")
            .values("Tim", 312)
            .values("Bob", 25)
            .values("Jane", 46)
            .execute();

        System.out.println("Write 10 records");

        session.upsert(set.ids(10,11,12,13,14,15,16,17,18,19))
            .bins("name", "age")
            .values("Tim", 312)
            .values("Bob", 25)
            .values("Jane", 46)
            .values("Tim", 200)
            .values("User1", 201)
            .values("User2", 202)
            .values("User3", 203)
            .values("User4", 204)
            .values("User5", 205)
            .values("User6", 206)
            .execute();

        System.out.println("Write 10 records async");

        rs = session.upsert(set.ids(110,111,112,113,114,115,116,117,118,119))
            .bins("name", "age")
            .values("Tim", 21)
            .values("Bob", 25)
            .values("Jane", 46)
            .values("Tim", 200)
            .values("User1", 201)
            .values("User2", 202)
            .values("User3", 203)
            .values("User4", 204)
            .values("User5", 205)
            .values("User6", 206)
            .executeAsync();

        while (rs.hasNext()) {
            System.out.println(rs.next());
        }

        System.out.println("Read 1 record");

        rs = session.query(set.ids(100)).execute();

        if (rs.hasNext()) {
        	Record rec = rs.next().recordOrThrow();
        	System.out.println("Record = " + rec);
        }
        else {
        	System.out.println("Error: No records returned");
        }

        System.out.println("Read 2 records");

        rs = session.query(set.ids(1,2)).execute();

        while (rs.hasNext()) {
        	Record rec = rs.next().recordOrThrow();
        	System.out.println("Record = " + rec);
        }

        System.out.println("Exists 1 record");

        List<Boolean> results = session.exists(set.ids(113)).execute();

        for (boolean b : results) {
        	System.out.println("Result: " + b);
        }

        System.out.println("Touch 1 record");

        results = session.touch(set.ids(113)).execute();

        for (boolean b : results) {
        	System.out.println("Result: " + b);
        }

        System.out.println("Delete 1 record");

        results = session.delete(set.ids(118)).execute();

        for (boolean b : results) {
        	System.out.println("Result: " + b);
        }

        System.out.println("Batch exists");

        results = session.exists(set.ids(113,114,999)).execute();

        for (boolean b : results) {
        	System.out.println("Result: " + b);
        }

        System.out.println("Batch touch");

        results = session.touch(set.ids(113,114,999)).execute();

        for (boolean b : results) {
        	System.out.println("Result: " + b);
        }

        System.out.println("Batch delete");

        results = session.delete(set.ids(113,114,999)).execute();

        for (boolean b : results) {
        	System.out.println("Result: " + b);
        }

        // Test filtering out
        System.out.println("Test filtering out");

        Optional<RecordResult> rec = session.query(set.ids(2))
            .where(Dsl.stringBin("name").eq("Bob"))
            .execute()
            .getFirst();

        rec.ifPresentOrElse(
                val -> System.out.println("Record for Bob exists, value: " + val),
                () -> System.out.println("ERROR: Record for Bob does not exist"));

        // Run again, this time looking for "fred" on the "Bob" record, but without failing on filtering out
        rec = session.query(set.ids(2))
                .where(Dsl.stringBin("name").eq("Fred"))
                .execute()
                .getFirst();

        rec.ifPresentOrElse(
                val -> System.out.println("ERROR: Record for Fred exists, value: " + val),
                () -> System.out.println("Record for Fred does not exist (expected)"));

        rec = session.query(set.ids(2))
                .where(Dsl.stringBin("name").eq("Fred"))
                .respondAllKeys()
                .execute()
                .getFirst();

        rec.ifPresentOrElse(
                val -> System.out.println("Record for Fred exists, value: " + val),
                () -> System.out.println("ERROR: Record for Fred does not exist"));


        // Run again, failing on filtered out
        try {
            rec = session.query(set.ids(2))
                    .where(Dsl.stringBin("name").eq("Fred"))
                    .failOnFilteredOut()
                    .execute()
                    .getFirst();
            System.out.println("ERROR: No exception was thrown, this is unexpected");
        }
        catch (AerospikeException ae) {
            System.out.printf("Exception received as expected: %s (%s)\n", ae.getMessage(), ae.getClass().getSimpleName());
        }

        System.out.println("Foreground primary index query");

        rs = session.query(set)
        	.recordsPerSecond(5000)
        	.execute();

        int count = 0;

        while (rs.hasNext()) {
            System.out.println(rs.next());
            count++;
        }

        System.out.println("Query count: " + count);

        Settings settings = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.QUERY, Mode.CP);
        System.out.printf("Batch mode maxConcurrentNodes = %d\n", settings.getMaxConcurrentNodes());

        Exp exp = Exp.or(
                Exp.eq(Exp.stringBin("name"), Exp.val("Tim")),
                Exp.gt(Exp.intBin("age"), Exp.val(21))
        );
        System.out.println(exp);

        System.out.println("Create index");

        session.createIndex(set, "ageidx", "age", IndexType.NUMERIC, IndexCollectionType.DEFAULT);

        System.out.println("Foreground secondary index query");

        rs = session.query(set)
        	.where("$.age > 200")
        	.execute();

        count = 0;

        while (rs.hasNext()) {
            System.out.println(rs.next());
            count++;
        }

        System.out.println("Query count: " + count);

        rs = session.query(set.ids(10,11,110))
        	.execute();

        while (rs.hasNext()) {
        	Record r = rs.next().recordOrThrow();
        	System.out.println("Record = " + r);
        }

        System.out.println("Background query");

        ExecuteTask task = session.backgroundTask().update(set)
            .bin("age").add(1)
            .where("$.name == 'Tim'")
            .execute();

        task.waitTillComplete();

        rs = session.query(set.ids(10,11,110))
        	.execute();

        while (rs.hasNext()) {
        	Record r = rs.next().recordOrThrow();
        	System.out.println("Record = " + r);
        }

        /*
        System.out.println("Transaction");

        session.doInTransaction(txnSession -> {
        	txnSession.upsert(set.id(2222))
            .bins("name", "age")
            .values("Charlie", 33)
            .execute();

        	txnSession.upsert(set.id(3333))
            .bins("name", "age")
            .values("Tom", 22)
            .execute();

        	System.out.println("Read in transaction");
            RecordStream stream = txnSession.query(set.ids(2222)).execute();

            while (stream.hasNext()) {
            	Record r = stream.next().recordOrThrow();
            	System.out.println("Record = " + r);
            }
        });

        System.out.println("Read Transaction Values");

        RecordStream stream = session.query(set.ids(2222,3333)).execute();

        while (stream.hasNext()) {
        	Record r = stream.next().recordOrThrow();
        	System.out.println("Record = " + r);
        }
    */
    }
}
