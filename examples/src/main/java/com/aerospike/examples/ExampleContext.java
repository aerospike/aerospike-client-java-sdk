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

/**
 * Shared state for an example run.
 */
public class ExampleContext {
    private final Cluster cluster;
    private final Args args;
    private final Console console;

    public ExampleContext(Cluster cluster, Args args, Console console) {
        this.cluster = cluster;
        this.args = args;
        this.console = console;
    }

    public Cluster cluster() {
        return cluster;
    }

    public Args args() {
        return args;
    }

    public Console console() {
        return console;
    }

    public Session session() {
        return cluster.createSession(Behavior.DEFAULT);
    }

    public DataSet dataSet() {
        return DataSet.of(args.namespace, args.set);
    }

    public DataSet dataSet(String set) {
        return DataSet.of(args.namespace, set);
    }
}
