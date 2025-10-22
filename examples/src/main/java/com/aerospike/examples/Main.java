package com.aerospike.examples;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.util.Util;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        ClusterDefinition def = new ClusterDefinition("db11", 3000);

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
