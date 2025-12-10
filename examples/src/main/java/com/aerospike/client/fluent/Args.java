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

/**
 * Command line argument parser for Aerospike examples.
 * 
 * <p>Supports the following flags:
 * <ul>
 *   <li>{@code -h, --host <hostname>} - Aerospike server hostname (default: localhost)</li>
 *   <li>{@code -p, --port <port>} - Aerospike server port (default: 3000)</li>
 *   <li>{@code -u, --use-alternate} - Use alternate services for cluster discovery</li>
 *   <li>{@code --help} - Display usage information</li>
 * </ul>
 * 
 * <p>Example usage:
 * <pre>{@code
 * public static void main(String[] args) {
 *     Args arguments = Args.parse(args);
 *     
 *     ClusterDefinition clusterDef = new ClusterDefinition(arguments.getHost(), arguments.getPort());
 *     if (arguments.isUseAlternate()) {
 *         clusterDef.usingServicesAlternate();
 *     }
 * }
 * }</pre>
 */
public class Args {
    
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3000;
    
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private boolean useAlternate = false;
    
    /**
     * Parse command line arguments.
     * 
     * @param args command line arguments
     * @return parsed Args instance
     */
    public static Args parse(String[] args) {
        Args result = new Args();
        
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            
            switch (arg) {
                case "-h":
                case "--host":
                    if (i + 1 < args.length) {
                        result.host = args[++i];
                    } else {
                        System.err.println("Error: --host requires a hostname argument");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                    
                case "-p":
                case "--port":
                    if (i + 1 < args.length) {
                        try {
                            result.port = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: --port requires a valid integer");
                            printUsage();
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Error: --port requires a port number argument");
                        printUsage();
                        System.exit(1);
                    }
                    break;
                    
                case "-u":
                case "--use-alternate":
                    result.useAlternate = true;
                    break;
                    
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                    
                default:
                    if (arg.startsWith("-")) {
                        System.err.println("Error: Unknown option: " + arg);
                        printUsage();
                        System.exit(1);
                    }
                    // Ignore non-option arguments
                    break;
            }
        }
        
        return result;
    }
    
    /**
     * Print usage information to stdout.
     */
    public static void printUsage() {
        System.out.println("Usage: java <ExampleClass> [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --host <hostname>    Aerospike server hostname (default: localhost)");
        System.out.println("  -p, --port <port>        Aerospike server port (default: 3000)");
        System.out.println("  -u, --use-alternate      Use alternate services for cluster discovery");
        System.out.println("      --help               Display this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java Main -h db1.example.com -p 3000");
        System.out.println("  java Main --host 192.168.1.100 --port 3100 --use-alternate");
    }
    
    /**
     * Get the configured host.
     * 
     * @return hostname
     */
    public String getHost() {
        return host;
    }
    
    /**
     * Get the configured port.
     * 
     * @return port number
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Check if alternate services should be used.
     * 
     * @return true if alternate services should be used
     */
    public boolean isUseAlternate() {
        return useAlternate;
    }
    
    @Override
    public String toString() {
        return "Args{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", useAlternate=" + useAlternate +
            '}';
    }
}
