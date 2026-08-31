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

import java.util.Arrays;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.VectorExp;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.query.Order;
import com.aerospike.client.sdk.query.OrderByType;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/**
 * Walkthrough of native vector bins and a hybrid predicate-cohort + Top-K similarity query.
 */
public class VectorTopKQueryExample extends Example {

    @Override
    public void runExample() throws Exception {
        Session session = cluster().createSession(Behavior.DEFAULT);
        DataSet products = dataSet("vector_topk_demo");

        seedCatalog(session, products);
        runHybridTopKQuery(session, products);
    }

    private void seedCatalog(Session session, DataSet products) {
        console.info("--- 1) Native vector bins: write and read back ---");

        upsertProduct(session, products, "sku-1", "wireless mouse", "electronics", 42,
            new float[] {0.12f, 0.98f, 0.44f, 0.05f});
        upsertProduct(session, products, "sku-2", "mechanical keyboard", "electronics", 0,
            new float[] {0.31f, 0.66f, 0.29f, 0.71f});
        upsertProduct(session, products, "sku-3", "garden hose", "outdoor", 15,
            new float[] {0.88f, 0.02f, 0.10f, 0.42f});

        Record rec = session.query(products.id("sku-1")).execute().getFirstRecord();
        console.info("Read back sku-1 embedding: " + Arrays.toString(rec.getVector("embedding").getFloat32Data()));
    }

    private void upsertProduct(Session session, DataSet products, String id, String name, String category,
            int stock, float[] embedding) {
        session.upsert(products.id(id))
            .bin("name").setTo(name)
            .bin("category").setTo(category)
            .bin("stock").setTo(stock)
            .bin("embedding").setTo(Vector.ofFloat32(embedding))
            .execute();
    }

    private void runHybridTopKQuery(Session session, DataSet products) {
        console.info("--- 2) Hybrid Top-K similarity query: predicate cohort + vector distance ranking ---");

        // "wireless mouse for gaming", stand-in for a real embedding model's output.
        Vector queryVector = Vector.ofFloat32(new float[] {0.10f, 0.95f, 0.40f, 0.08f});

        RecordStream rs = session.query(products)
            .where(Exp.and(
                Exp.eq(Exp.stringBin("category"), Exp.val("electronics")),
                Exp.gt(Exp.intBin("stock"), Exp.val(0))))
            .bin("similarity").selectFrom(
                VectorExp.distance(VectorDistanceMetric.COSINE, queryVector, Exp.vectorBin("embedding")))
            .readingOnlyBins("name", "stock", "similarity")
            .orderBy("similarity", OrderByType.DOUBLE, Order.DESC)   // COSINE: higher = closer
            .topK(10)
            .execute();

        try {
            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                console.info("%s (similarity=%.4f)", rec.getString("name"), rec.getDouble("similarity"));
            }
        }
        finally {
            rs.close();
        }
    }
}
