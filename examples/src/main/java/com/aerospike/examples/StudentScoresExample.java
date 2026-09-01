package com.aerospike.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;

public class StudentScoresExample extends Example {
    private static final String[] SUBJECTS = {"math", "english", "science", "history", "art"};

    @Override
    protected boolean requiresStringAel() {
        return true;
    }

    private static Map<String, Integer> generateScores(Random random) {
        Map<String, Integer> scores = new HashMap<>();
        for (String subject : SUBJECTS) {
            scores.put(subject, 55 + random.nextInt(46)); // scores between 55 and 100
        }
        return scores;
    }

    @Override
    public void runExample() {
        Session session = cluster().createSession(Behavior.DEFAULT);
        DataSet class10a = dataSet("class10a");
        Random random = new Random(42);

        // -- Write 30 student records --
        for (int i = 1; i <= 30; i++) {
            session.upsert(class10a.id("student-" + i))
                .bin("name").setTo("Student " + i)
                .bin("scores").setTo(generateScores(random))
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
