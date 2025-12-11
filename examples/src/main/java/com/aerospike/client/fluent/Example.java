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
package com.aerospike.client.fluent;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Abstract base class for all examples.
 * 
 * <p>Concrete examples should extend this class and implement the {@link #runExample(Parameters)} method.
 * The base class handles example lifecycle (begin/end logging) and provides a console for output.
 */
public abstract class Example {
    
    protected Console console;
    
    public Example(Console console) {
        this.console = console;
    }
    
    /**
     * Run one or more examples.
     * 
     * @param console the console for output
     * @param params configuration parameters
     * @param examples list of example names to run
     * @throws Exception if an example fails
     */
    public static void runExamples(Console console, Parameters params, List<String> examples) throws Exception {
        for (String exampleName : examples) {
            runExample(exampleName, params, console);
        }
    }
    
    /**
     * Run a single example by name using reflection.
     * 
     * @param exampleName the simple name of the example class
     * @param params configuration parameters
     * @param console the console for output
     * @throws Exception if the example fails or cannot be found
     */
    public static void runExample(String exampleName, Parameters params, Console console) throws Exception {
        String fullName = "com.aerospike.client.fluent." + exampleName;
        Class<?> cls = Class.forName(fullName);
        
        if (Example.class.isAssignableFrom(cls)) {
            Constructor<?> ctor = cls.getDeclaredConstructor(Console.class);
            Example example = (Example) ctor.newInstance(console);
            example.run(params);
        } else {
            console.error("Invalid example: " + exampleName);
        }
    }
    
    /**
     * Run this example with lifecycle logging.
     * 
     * @param params configuration parameters
     * @throws Exception if the example fails
     */
    public void run(Parameters params) throws Exception {
        console.info(this.getClass().getSimpleName() + " Begin");
        runExample(params);
        console.info(this.getClass().getSimpleName() + " End");
    }
    
    /**
     * Run the example logic. Subclasses must implement this method.
     * 
     * @param params configuration parameters
     * @throws Exception if the example fails
     */
    public abstract void runExample(Parameters params) throws Exception;
}

