package com.aerospike.examples;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class BatchExample extends Example {
    public BatchExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        Session session = cluster.createSession(Behavior.DEFAULT);
        
        System.out.println("*************");
        System.out.println("* Batch tests");
        System.out.println("*************");
        DataSet set = DataSet.of("test", "set");
        
        session.truncate(set);
        
        System.out.println("Batch Insert:");
        session.insert(set.ids(1,2,3,4,5))
            .bin("name").setTo("Fred")
            .bin("age").setTo(30)
            .bin("value").setTo(10)
            .execute();
        session.query(set).execute().forEach(rec -> System.out.println(rec));

        System.out.println("Batch Modify:");
        session
            .insert(set.ids(6,7,8)) 
                .bin("name").setTo("Wilma")
                .bin("age").setTo(33)
                .bin("value").setTo(20)
            .update(set.id(2))
                .bin("value").add(5)
            .delete(set.id(1))
            .execute();
        
        session.query(set).execute().forEach(rec -> System.out.println(rec));
    }

}
