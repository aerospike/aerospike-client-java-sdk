package com.aerospike.examples;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        ClusterDefinition def = new ClusterDefinition("db11", 3000);

        try (Cluster cluster = def.connect()) {
            //Session session = cluster.createSession(Behavior.DEFAULT);
            // Do Stuff
            System.out.println("Connected");
       }
    }
}
