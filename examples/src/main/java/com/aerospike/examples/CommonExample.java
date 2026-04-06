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

import java.util.Optional;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.ael.Ael;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.info.classes.IndexType;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.task.ExecuteTask;

/**
 * Examples for common commands.
 */
public class CommonExample extends Example {
    public CommonExample(Console console) {
        super(console);
    }

    public void printRecordStream(String header, RecordStream rs) {
        if (header != null) {
            System.out.println("=".repeat(40));
            System.out.println(header);
            System.out.println("=".repeat(40));
        }
        if (rs == null) {
            System.out.println("null");
        }
        else {
            rs.forEach(rr -> System.out.println(rr));
        }
        if (header != null) {
            int count = 40 - header.length() - 2;
            String dashes = (count > 0) ? "-".repeat(count) : "";
            System.out.printf("%s %s %s\n", dashes, header, dashes);
        }
    }
    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        Session session = cluster.createSession(Behavior.DEFAULT);
        DataSet set = DataSet.of("test", "set");

        System.out.println("Truncate records");

        session.truncate(set);

        System.out.println("Write 1 record");

        session.upsert(set)
            .bins("name", "age")
            .id(10).values("Charlie", 11)
            .execute();

        System.out.println("Write 1 record async");

        RecordStream rs = session.upsert(set)
            .bins("name", "age")
            .id(100).values("Charlie", 999)
            .execute();

        //System.out.println(rs.getFirst());

        System.out.println("Write 3 records");

        session.upsert(set)
            .bins("name", "age")
            .id(1).values("Tim", 312)
            .id(2).values("Bob", 25)
            .id(3).values("Jane", 46)
            .execute();

        System.out.println("Write 10 records");

        session.upsert(set)
            .bins("name", "age")
            .id(10).values("Tim", 312)
            .id(11).values("Bob", 25)
            .id(12).values("Jane", 46)
            .id(13).values("Tim", 200)
            .id(14).values("User1", 201)
            .id(15).values("User2", 202)
            .id(16).values("User3", 203)
            .id(17).values("User4", 204)
            .id(18).values("User5", 205)
            .id(19).values("User6", 206)
            .execute();

        System.out.println("Write 10 records async");

        rs = session.upsert(set)
            .bins("name", "age")
            .id(110).values("Tim", 21)
            .id(111).values("Bob", 25)
            .id(112).values("Jane", 46)
            .id(113).values("Tim", 200)
            .id(114).values("User1", 201)
            .id(115).values("User2", 202)
            .id(116).values("User3", 203)
            .id(117).values("User4", 204)
            .id(118).values("User5", 205)
            .id(119).values("User6", 206)
            .execute();

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

        Optional<Boolean> result = session.exists(set.ids(113)).execute().getFirstBoolean();

        result.ifPresentOrElse(bool -> {
            System.out.println("Result: " + bool);
        }, () -> {
            System.out.println("No record returned. This is unexpected");
        });

        System.out.println("Touch 1 record");

        result = session.touch(set.ids(113)).execute().getFirstBoolean();

        result.ifPresentOrElse(bool -> {
            System.out.println("Result: " + bool);
        }, () -> {
            System.out.println("No record returned. This is unexpected");
        });

        System.out.println("Delete 1 record");

        result = session.delete(set.ids(118)).execute().getFirstBoolean();

        result.ifPresentOrElse(bool -> {
            System.out.println("Result: " + bool);
        }, () -> {
            System.out.println("No record returned. This is unexpected");
        });

        System.out.println("Batch exists");

        RecordStream results = session.exists(set.ids(113,114,999)).includeMissingKeys().execute();

        results.forEach(rr -> {
            System.out.printf("   Key: %s -> %b\n", rr.key(), rr.asBoolean());
        });

        System.out.println("Batch touch");

        results = session.touch(set.ids(113,114,999)).includeMissingKeys().execute();

        results.forEach(rr -> {
            System.out.printf("   Key: %s -> %b\n", rr.key(), rr.asBoolean());
        });

        System.out.println("Batch delete");

        results = session.delete(set.ids(113,114,999)).includeMissingKeys().execute();

        results.forEach(rr -> {
            System.out.printf("   Key: %s -> %b\n", rr.key(), rr.asBoolean());
        });

        // Test filtering out
        System.out.println("Test filtering out");

        Optional<RecordResult> rec = session.query(set.ids(2))
            .where(Ael.stringBin("name").eq("Bob"))
            .execute()
            .getFirst();

        rec.ifPresentOrElse(
                val -> System.out.println("Record for Bob exists, value: " + val),
                () -> System.out.println("ERROR: Record for Bob does not exist"));

        // Run again, this time looking for "fred" on the "Bob" record, but without failing on filtering out
        rec = session.query(set.ids(2))
                .where(Ael.stringBin("name").eq("Fred"))
                .execute()
                .getFirst();

        rec.ifPresentOrElse(
                val -> System.out.println("ERROR: Record for Fred exists, value: " + val),
                () -> System.out.println("Record for Fred does not exist (expected)"));

        rec = session.query(set.ids(2))
                .where(Ael.stringBin("name").eq("Fred"))
                .includeMissingKeys()
                .execute()
                .getFirst();

        rec.ifPresentOrElse(
                val -> System.out.println("Record for Fred exists, value: " + val),
                () -> System.out.println("ERROR: Record for Fred does not exist"));


        // Run again, failing on filtered out
        try {
            rec = session.query(set.ids(2))
                    .where(Ael.stringBin("name").eq("Fred"))
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

        try {
	        int count = 0;

	        while (rs.hasNext()) {
	            System.out.println(rs.next());
	            count++;
	        }
	        System.out.println("Query count: " + count);
        }
        finally {
        	rs.close();
        }

        Settings settings = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.QUERY, Mode.CP);
        System.out.printf("Batch mode maxConcurrentNodes = %d\n", settings.getMaxConcurrentNodes());

        Exp exp = Exp.or(
                Exp.eq(Exp.stringBin("name"), Exp.val("Tim")),
                Exp.gt(Exp.intBin("age"), Exp.val(21))
        );
        System.out.println(exp);

        System.out.println("Create index");

        session.createIndex(set, "ageidx", "age", IndexType.INTEGER, IndexCollectionType.DEFAULT);

        System.out.println("Foreground secondary index query");

        rs = session.query(set)
        	.where("$.age > 200")
        	.execute();

        try {
	        int count = 0;

	        while (rs.hasNext()) {
	            System.out.println(rs.next());
	            count++;
	        }

	        System.out.println("Query count: " + count);
        }
        finally {
        	rs.close();
        }

        rs = session.query(set.ids(10,11,110))
        	.execute();

        while (rs.hasNext()) {
        	Record r = rs.next().recordOrThrow();
        	System.out.println("Record = " + r);
        }

        System.out.println("Paginated secondary index query");

        rs = session.query(set).execute();

        try {
	        int count = 0;

	        while (rs.hasNext()) {
	        	rs.next().recordOrThrow();
	        	count++;
	        }
	        System.out.println("Expected paginated query count: " + count);
        }
        finally {
        	rs.close();
        }

        rs = session.query(set).chunkSize(5).execute();

        try {
	        int chunk = 0;
	        int count = 0;

	        while (rs.hasMoreChunks()) {
	            System.out.println("Chunk: " + (++chunk));

	            while (rs.hasNext()) {
	                System.out.println(rs.next());
	                count++;
	            }
	        }

	        System.out.println("Actual Query count: " + count);
        }
        finally {
        	rs.close();
        }

        System.out.println("Background query");

        ExecuteTask task = session.backgroundTask().update(set)
            .bin("age").add(1)
            //.where("$.age > 200")
            .where("$.name == 'Tim' and $.age > 20")
            .execute();

        task.waitTillComplete();

        rs = session.query(set.ids(10,11,110))
        	.execute();

        while (rs.hasNext()) {
        	Record r = rs.next().recordOrThrow();
        	System.out.println("Record = " + r);
        }

        session.query(set).withHint(hint -> hint.queryDuration(QueryDuration.LONG)).recordsPerSecond(20).execute();

        session.backgroundTask().update(set).bin("age").add(1).recordsPerSecond(35).execute();

        // Exp operations - read and write.
        System.out.println("Read and write operation example");
        rs = session.upsert(set.ids(1,2,3))
            .bin("name").setTo("Tim")
            .bin("age").setTo(312)
            .bin("readBin").selectFrom("$.age + 12")
            .bin("writeBin").upsertFrom("$.age + 30")
            .execute();
        rs.forEach(rr -> System.out.println(rr));

        printRecordStream("Single read expression", session.query(set.id(1))
            .bin("ageIn20Years").selectFrom("$.age + 20")
            .execute());

        printRecordStream("Batch read expression", session.query(set.ids(1,2,3))
            .bin("ageIn20Years").selectFrom("$.age + 20")
            .execute());

        printRecordStream("Query read expression", session.query(set)
            .bin("ageIn20Years").selectFrom("$.age + 20")
            .execute());
    }
}
