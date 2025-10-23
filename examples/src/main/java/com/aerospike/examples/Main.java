package com.aerospike.examples;

import java.util.Optional;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.dsl.Dsl;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.util.Util;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        ClusterDefinition def = new ClusterDefinition("localhost", 3100);

        try (Cluster cluster = def.connect()) {
            System.out.println("Connected");

            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet set = DataSet.of("test", "set");

            session.upsert(set.ids(1,2,3))
	            .bins("name", "age")
	            .values("Tim", 312)
	            .values("Bob", 25)
	            .values("Jane", 46)
	            .execute();

            System.out.println("Upsert success");

            RecordStream rs = session.query(set.ids(1)).execute();

            if (rs.hasNext()) {
            	Record rec = rs.next().recordOrThrow();
            	System.out.println("Record = " + rec);
            }
            else {
            	System.out.println("Error: No records returned");
            }
            
            // Test filtering out
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
            
            

            /* Does not work yet.
            rs = session.query(set.ids(1,2)).execute();

            if (rs.hasNext()) {
            	Record rec = rs.next().recordOrThrow();
            	System.out.println("Record = " + rec);
            }
            else {
            	System.out.println("Error: No records returned");
            }*/
        }
        catch (Throwable t) {
       		System.out.println("Error: " + Util.getErrorMessage(t));
        }
    }
}
