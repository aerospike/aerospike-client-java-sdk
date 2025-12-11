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
    
    public String host;
    public int port;
    public String namespace;
    public String set;
    public boolean useServicesAlternate;
    
    protected Parameters(String host, int port, String namespace, String set, boolean useServicesAlternate) {
        this.host = host;
        this.port = port;
        this.namespace = namespace;
        this.set = set;
        this.useServicesAlternate = useServicesAlternate;
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
        options.addOption("n", "namespace", true, "Namespace (default: test)");
        options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
        options.addOption("a", "servicesAlternate", false, "Use services alternate for cluster discovery");
        options.addOption("u", "usage", false, "Print usage");
    }
    
    /**
     * Parse command line into Parameters.
     * 
     * @param cl the parsed CommandLine
     * @return Parameters instance with parsed values
     */
    public static Parameters parseParameters(CommandLine cl) {
        String host = cl.getOptionValue("h", "localhost");
        String portString = cl.getOptionValue("p", "3000");
        int port = Integer.parseInt(portString);
        String namespace = cl.getOptionValue("n", "test");
        String set = cl.getOptionValue("s", "demoset");
        boolean useServicesAlternate = cl.hasOption("a");
        
        if (set.equals("empty")) {
            set = "";
        }
        
        return new Parameters(host, port, namespace, set, useServicesAlternate);
    }
    
    @Override
    public String toString() {
        return "Parameters{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", namespace='" + namespace + '\'' +
                ", set='" + set + '\'' +
                ", useServicesAlternate=" + useServicesAlternate +
                '}';
    }
}

