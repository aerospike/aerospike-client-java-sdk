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
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.StringExp;
import com.aerospike.client.sdk.operation.StringOperation;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.util.Version;

/**
 * Server string read/modify operations (Aerospike 8.1.3+): fluent {@code BinBuilder},
 * {@link com.aerospike.client.sdk.ChainableOperationBuilder#appendOperations} with
 * {@link StringOperation}, and {@code selectFrom} with {@link StringExp}.
 *
 * <p>If the cluster is older than 8.1.3, this example logs a skip message and exits.</p>
 *
 * @see com.aerospike.examples.StringOperationsExample runnable sample ({@code ./run_examples StringOperationsExample})
 */
public class StringOperationsExample extends Example {

    public StringOperationsExample(Console console) {
        super(console);
    }

    @Override
    public void runExample(Cluster cluster, Args args) throws Exception {
        Version v = cluster.getRandomNode().getVersion();
        if (!v.isGreaterOrEqual(Version.SERVER_VERSION_8_1_3)) {
            console.info("StringOperationsExample: skipping (server is " + v
                + "; string ops require 8.1.3+).");
            return;
        }

        Session session = cluster.createSession(Behavior.DEFAULT);
        DataSet set = DataSet.of(args.namespace, "string-ops-demo");
        session.truncate(set);
        Key key = set.id("row1");

        console.info("--- 1) Fluent BinBuilder: strlen, substr [1,4), substr from 3, find, upper ---");
        session.upsert(key)
            .bin("message").setTo("hello")
            .execute();

        RecordStream rs = session.upsert(key)
            .bin("message").strlen()
            .bin("message").substr(1, 4)
            .bin("message").substr(3)
            .bin("message").find("ll")
            .bin("message").upper()
            .bin("message").get()
            .execute();

        Record rec = rs.getFirst().orElseThrow().recordOrThrow();
        console.info("strlen -> " + rec.operationResult(0).getLong());
        console.info("substr(1,4) [1,4) -> " + rec.operationResult(1).getString());
        console.info("substr(3) suffix -> " + rec.operationResult(2).getString());
        console.info("find ll -> " + rec.operationResult(3).getLong());
        console.info("upper result -> " + rec.operationResult(4).getString());
        console.info("get bin after upper -> " + rec.operationResult(5).getString());

        console.info("--- 2) appendOperations(StringOperation.*): same reads on fresh value ---");
        session.upsert(key)
            .bin("message").setTo("hello")
            .execute();

        rs = session.upsert(key)
            .appendOperations(
                StringOperation.strlen("message"),
                StringOperation.substr("message", 1, 4),
                StringOperation.find("message", "ll"))
            .execute();
        rec = rs.getFirst().orElseThrow().recordOrThrow();
        console.info("strlen / substr / find via Operation list -> "
            + rec.operationResult(0).getLong() + ", "
            + rec.operationResult(1).getString() + ", "
            + rec.operationResult(2).getLong());

        console.info("--- 3) Query: selectFrom(StringExp) projection bins ---");
        session.upsert(key)
            .bin("message").setTo("hello")
            .execute();

        rs = session.query(key)
            .bin("slen").selectFrom(StringExp.strlen(Exp.stringBin("message")))
            .bin("stail").selectFrom(StringExp.substr(Exp.val(3), Exp.stringBin("message")))
            .bin("atLl").selectFrom(StringExp.find(Exp.val("ll"), Exp.stringBin("message")))
            .execute();
        rec = rs.getFirst().orElseThrow().recordOrThrow();
        console.info("slen=" + rec.getLong("slen") + ", stail=" + rec.getString("stail")
            + ", find(ll)=" + rec.getLong("atLl"));
    }
}
