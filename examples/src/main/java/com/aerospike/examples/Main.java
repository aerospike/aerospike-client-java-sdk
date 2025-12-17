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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * Main entry point for running Aerospike Fluent Client examples.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * ./run_examples <ExampleName> [options]
 * ./run_examples all [options]
 * ./run_examples -u
 * }</pre>
 *
 * <h2>Available Examples</h2>
 * <ul>
 *   <li>{@code BehaviorHierarchicalExample} - Demonstrates hierarchical YAML configuration with dynamic reloading</li>
 *   <li>{@code BehaviorYamlExample} - Demonstrates loading behaviors from YAML files</li>
 *   <li>{@code YamlConfigConnectionExample} - Demonstrates connecting to a cluster with YAML configuration</li>
 * </ul>
 */
public class Main {

    private static final String[] EXAMPLE_NAMES = new String[] {
        "BehaviorHierarchicalExample",
        "BehaviorYamlExample",
        "CommonExample",
        "CompleteYamlConfigExample",
        "YamlConfigConnectionExample"
    };

    public static String[] getAllExampleNames() {
        return EXAMPLE_NAMES;
    }

    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        try {
            Options options = new Options();
            Args.addCommonOptions(options);

            CommandLineParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args, false);

            if (args.length == 0 || cl.hasOption("u")) {
                logUsage(options);
                return;
            }

            Args arguments = new Args(cl);
            String[] exampleNames = cl.getArgs();

            if (exampleNames.length == 0) {
                logUsage(options);
                return;
            }

            // Check for "all"
            for (String exampleName : exampleNames) {
                if (exampleName.equalsIgnoreCase("all")) {
                    exampleNames = EXAMPLE_NAMES;
                    break;
                }
            }

            Console console = new Console();
            runExamples(console, arguments, exampleNames);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Write usage to console.
     */
    private static void logUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String syntax = Main.class.getName() + " [<options>] all|(<example1> <example2> ...)";
        formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
        System.out.println("examples:");

        for (String name : EXAMPLE_NAMES) {
            System.out.println("  " + name);
        }
        System.out.println();
        System.out.println("All examples will be run if 'all' is specified as an example.");
    }

    /**
     * Run one or more examples.
     */
    public static void runExamples(Console console, Args args, String[] examples) throws Exception {
        List<String> exampleList = new ArrayList<>();
        for (String example : examples) {
            exampleList.add(example);
        }
        Example.runExamples(console, args, exampleList);
    }
}
