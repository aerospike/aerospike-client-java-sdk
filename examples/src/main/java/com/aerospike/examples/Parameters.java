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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Configuration data for running examples.
 * Contains connection parameters and other settings parsed from command line.
 */
public class Parameters {

    public final String host;
    public final int port;
    public final String clusterName;
    public final String namespace;
    public final String set;
    public final boolean useServicesAlternate;

    public Parameters(CommandLine cl) {
        this.host = cl.getOptionValue("h", "localhost");

        String portString = cl.getOptionValue("p", "3000");
        this.port = Integer.parseInt(portString);

        this.clusterName = cl.getOptionValue("c");
        this.namespace = cl.getOptionValue("n", "test");

        String setName = cl.getOptionValue("s", "demoset");

        if (setName.equals("empty")) {
            this.set = "";
        }
        else {
            this.set = setName;
        }

        this.useServicesAlternate = cl.hasOption("a");
    }

    /**
     * Add common options to the provided Options object.
     *
     * @param options the Options object to add common options to
     */
    public static void addCommonOptions(Options options) {
        options.addOption("h", "host", true,
                "List of seed hosts in format:\n" +
                "hostname1[:port1],...\n" +
                "Default: localhost");
        options.addOption("p", "port", true, "Server default port (default: 3000)");
        options.addOption("c", "cluster", true, "Cluster name (default: null)");
        options.addOption("n", "namespace", true, "Namespace (default: test)");
        options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
        options.addOption("a", "servicesAlternate", false, "Use services alternate for cluster discovery");
        options.addOption("u", "usage", false, "Print usage");
    }

    @Override
    public String toString() {
        return "Parameters{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", clusterName=" + clusterName + '\'' +
                ", namespace='" + namespace + '\'' +
                ", set='" + set + '\'' +
                ", useServicesAlternate=" + useServicesAlternate +
                '}';
    }
}

