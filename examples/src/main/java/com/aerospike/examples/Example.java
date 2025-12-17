/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.List;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;

/**
 * Abstract base class for all examples.
 *
 * <p>Concrete examples should extend this class and implement the {@link #runExample(Args)} method.
 * The base class handles example lifecycle (begin/end logging) and provides a console for output.
 */
public abstract class Example {
    private static final String PACKAGE_NAME = "com.aerospike.examples.";

    protected Console console;

    public Example(Console console) {
        this.console = console;
    }

    /**
     * Run one or more examples.
     *
     * @param console the console for output
     * @param args configuration parameters
     * @param examples list of example names to run
     * @throws Exception if an example fails
     */
    public static void runExamples(Console console, Args args, List<String> examples) throws Exception {
        ClusterDefinition def = new ClusterDefinition(args.host, args.port)
        	.clusterName(args.clusterName)
        	.withSystemSettings(builder -> builder
    	    	.circuitBreaker(ops -> ops.maximumErrorsInErrorWindow(200))
    	        .connections(conn -> conn
    	        	.minimumConnectionsPerNode(200)
    	            .maximumConnectionsPerNode(200)
    	         )
    	    );

        if (args.tlsName != null) {
            String certHome = System.getenv("CERT_HOME");

            if (certHome == null) {
                certHome = "";
            }

            String caFile = resolvePath(certHome, args.caFile);
            String clientCertFile = resolvePath(certHome, args.clientCertFile);
            String clientKeyFile = resolvePath(certHome, args.clientKeyFile);

            def.withTlsConfigOf()
            	.tlsName(args.tlsName)
	            .caFile(caFile)
	            .clientCertFile(clientCertFile)
	            .clientKeyFile(clientKeyFile)
	        	.done();
        }

        Cluster cluster = def.connect();

		try {
	    	for (String exampleName : examples) {
	            runExample(exampleName, cluster, args, console);
	        }
		}
		finally {
			cluster.close();
		}
    }

    private static String resolvePath(String dir, String path) {
        File file = new File(path);

        if (file.isAbsolute()) {
        	return path;
        }

        file = new File(dir, path);
        return file.getAbsolutePath();
    }

    /**
     * Run a single example by name using reflection.
     *
     * @param exampleName the simple name of the example class
     * @param cluster cluster
     * @param args configuration parameters
     * @param console the console for output
     * @throws Exception if the example fails or cannot be found
     */
    public static void runExample(
    	String exampleName, Cluster cluster, Args args, Console console
    ) throws Exception {
        String fullName =  PACKAGE_NAME + exampleName;
        Class<?> cls = Class.forName(fullName);

        if (Example.class.isAssignableFrom(cls)) {
            Constructor<?> ctor = cls.getDeclaredConstructor(Console.class);
            Example example = (Example) ctor.newInstance(console);
            example.run(cluster, args);
        } else {
            console.error("Invalid example: " + exampleName);
        }
    }

    /**
     * Run this example with lifecycle logging.
     *
     * @param cluster cluster
     * @param args configuration parameters
     * @throws Exception if the example fails
     */
    public void run(Cluster cluster, Args args) throws Exception {
        console.info(this.getClass().getSimpleName() + " Begin");
        runExample(cluster, args);
        console.info(this.getClass().getSimpleName() + " End");
    }

    /**
     * Run the example logic. Subclasses must implement this method.
     *
     * @param params configuration parameters
     * @throws Exception if the example fails
     */
    public abstract void runExample(Cluster cluster, Args args) throws Exception;
}

