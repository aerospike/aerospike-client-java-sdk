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

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.aerospike.client.sdk.policy.AuthMode;

/**
 * Configuration data for running examples.
 * Contains connection parameters and other settings parsed from command line.
 */
public class Args {

    public final String host;
    public final int port;
    public final String clusterName;
    public final String namespace;
    public final String set;
    public final String tlsName;
    public final String caFile;
    public final String clientCertFile;
    public final String clientKeyFile;
    public final AuthMode authMode;
    public final boolean useServicesAlternate;

    public Args(CommandLine cl) {
        this.host = cl.getOptionValue("h", "localhost");

        String portString = cl.getOptionValue("p", "3000");
        int portInt = Integer.parseInt(portString);

        if (cl.hasOption("tls")) {
            // Change port to default TLS port when normal default port is specified.
        	if (portInt == 3000) {
        		System.out.println("Change TLS port from 3000 to 4333");
                this.port = 4333;
        	}
        	else {
                this.port = portInt;
        	}

            this.tlsName = cl.getOptionValue("tls", "");
            this.caFile = cl.getOptionValue("caFile", "cacert.pem");
            this.clientCertFile = cl.getOptionValue("clientCertFile", "cert.pem");
            this.clientKeyFile = cl.getOptionValue("clientKeyFile", "key.pem");
        }
        else {
        	this.port = portInt;
        	this.tlsName = null;
        	this.caFile = null;
        	this.clientCertFile = null;
        	this.clientKeyFile = null;
       }

        this.clusterName = cl.getOptionValue("c");
        this.namespace = cl.getOptionValue("n", "test");

        String setName = cl.getOptionValue("s", "demoset");

        if (setName.equals("empty")) {
            this.set = "";
        }
        else {
            this.set = setName;
        }

		if (cl.hasOption("auth")) {
			this.authMode = AuthMode.valueOf(cl.getOptionValue("auth", "").toUpperCase());
		}
		else {
			this.authMode = AuthMode.INTERNAL;
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
		options.addOption("auth", true, "Authentication mode. Values: " + Arrays.toString(AuthMode.values()));
		options.addOption("tls", "tls", true, "TLS name. Enables TLS/SSL sockets");
		options.addOption("caFile", "caFile", true, "TLS CA certificate file path");
		options.addOption("clientCertFile", "clientCertFile", true, "TLS client certificate file path");
		options.addOption("clientKeyFile", "clientKeyFile", true, "TLS client key file path");
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

