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

/**
 * Abstract base class for all examples.
 *
 * <p>Concrete examples should extend this class and implement the {@link #runExample()} method.
 * The base class provides access to runner-managed configuration. Lifecycle logging and the
 * setup/verify/cleanup fixture flow are owned by {@link ExampleRunner}.
 */
public abstract class Example {
    protected Console console;
    private ExampleContext context;

    void initialize(ExampleContext context) {
        this.context = context;
        this.console = context.console();
    }

    /**
     * Initialize this example with the runner-managed context and run its body.
     *
     * @param context runner-managed example context
     * @throws Exception if the example fails
     */
    public void run(ExampleContext context) throws Exception {
        initialize(context);
        runExample();
    }

    protected Cluster cluster() {
        return context.cluster();
    }

    protected String namespace() {
        return context.args().namespace;
    }

    protected String host() {
        return context.args().host;
    }

    protected int port() {
        return context.args().port;
    }

    protected boolean useServicesAlternate() {
        return context.args().useServicesAlternate;
    }

    protected DataSet dataSet() {
        return context.dataSet();
    }

    protected DataSet dataSet(String set) {
        return context.dataSet(set);
    }

    /**
     * Run the example logic. Subclasses must implement this method.
     *
     * @throws Exception if the example fails
     */
    public abstract void runExample() throws Exception;
}

