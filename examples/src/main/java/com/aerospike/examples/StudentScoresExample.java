package com.aerospike.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;

public class StudentScoresExample {
    private static final String[] SUBJECTS = {"math", "english", "science", "history", "art"};
    private static final Random random = new Random(42);   // fixed seed for reproducibility
    private static Map<String, Integer> generateScores(int studentNum) {
        Map<String, Integer> scores = new HashMap<>();
        for (String subject : SUBJECTS) {
            scores.put(subject, 55 + random.nextInt(46)); // scores between 55 and 100
        }
        return scores;
    }    
    
    public static void main(String[] args) {
        // -- Connect --
        try (Cluster cluster = new ClusterDefinition("localhost", 3100)
                .withNativeCredentials("admin", "admin123")
                .connect()) {
            
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet class10a = DataSet.of("test", "class10a");
            // -- Write 30 student records --
            for (int i = 1; i <= 30; i++) {
                session.upsert(class10a.id("student-" + i))
                    .bin("name").setTo("Student " + i)
                    .bin("scores").setTo(generateScores(i))
                    .execute();
            }
            
            // -- Query: students with any score >= 90 --
            session.query(class10a)
                .where("$.scores.{=90:}.count() > 0")
                .execute()
                .forEach(r -> System.out.printf("%s: %s%n",
                    r.recordOrThrow().getString("name"),
                    r.recordOrThrow().getMap("scores")));
            
        }
    }
}
