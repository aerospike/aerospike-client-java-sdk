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

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;

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
