package com.aerospike.examples;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

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
       }
    }
}
