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
package com.aerospike.client.fluent;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.fluent.command.Info;
import com.aerospike.client.fluent.policy.AuthMode;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.client.fluent.util.Version;

public class Args {
	public static Args Instance = new Args();

	public Session session;
	public String host;
	public int port;
	public String clusterName;
	public String namespace;
	public DataSet set;
	public AuthMode authMode = AuthMode.INTERNAL;
	public String user;
	public String password;
    public String tlsName;
    public String caFile;
    public String clientCertFile;
    public String clientKeyFile;
	public int indexTimeout = 10000;
	public boolean enterprise;
	public boolean hasTtl;
	public boolean scMode;
    public boolean useServicesAlternate;
	public Version serverVersion;
	public String containerNamePrefix;

	public Args() {
		host = "127.0.0.1";
		port = 3000;
		namespace = "test";
		set = DataSet.of(namespace, "test");

		String argString = System.getProperty("args");

		if (argString == null || argString.trim().length() == 0) {
			return;
		}

		try {
			String[] args = argString.split(" ");

			Options options = new Options();
			options.addOption("h", "host", true,
					"List of seed hosts in format:\n" +
					"hostname1[:tlsname][:port1],...\n" +
					"The tlsname is only used when connecting with a secure TLS enabled server. " +
					"If the port is not specified, the default port is used.\n" +
					"IPv6 addresses must be enclosed in square brackets.\n" +
					"Default: localhost\n" +
					"Examples:\n" +
					"host1\n" +
					"host1:3000,host2:3000\n" +
					"192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n"
					);
			options.addOption("p", "port", true, "Server default port (default: 3000)");
	        options.addOption("c", "cluster", true, "Cluster name (default: null)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: test)");
			options.addOption("auth", true, "Authentication mode. Values: " + Arrays.toString(AuthMode.values()));
			options.addOption("U", "user", true, "User name");
			options.addOption("P", "password", true, "Password");
			options.addOption("tls", true, "TLS name. Enables TLS/SSL sockets");
			options.addOption("caFile", true, "TLS CA certificate file path");
			options.addOption("clientCertFile", true, "TLS client certificate file path");
			options.addOption("clientKeyFile", true, "TLS client key file path");
	        options.addOption("a", "servicesAlternate", false, "Use services alternate for cluster discovery");
			options.addOption("d", "debug", false, "Run in debug mode.");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("container-prefix", true, "Container name prefix for node-controller/chaos tests (default: aerospike). Env: CONTAINER_NAME_PREFIX");

			CommandLineParser parser = new DefaultParser();
			CommandLine cl = parser.parse(options, args, false);

			if (cl.hasOption("u")) {
				logUsage(options);
				throw new AerospikeException("Terminate after displaying usage");
			}

			if (cl.hasOption("d")) {
				Log.setLevel(Log.Level.DEBUG);
			}

			host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			port = Integer.parseInt(portString);
	        clusterName = cl.getOptionValue("c");
			namespace = cl.getOptionValue("n", namespace);
			String setName = cl.getOptionValue("s", "test");
			set = DataSet.of(namespace, setName);

			if (cl.hasOption("auth")) {
				authMode = AuthMode.valueOf(cl.getOptionValue("auth", "").toUpperCase());
			}

			user = cl.getOptionValue("U");
			password = cl.getOptionValue("P");

			if (user != null && password == null) {
				java.io.Console console = System.console();

				if (console != null) {
					char[] pass = console.readPassword("Enter password:");

					if (pass != null) {
						password = new String(pass);
					}
				}
			}

			if (cl.hasOption("tls")) {
	            // Change port to default TLS port when normal default port is specified.
	        	if (port == 3000) {
	        		System.out.println("Change TLS port from 3000 to 4333");
	        		port = 4333;
	        	}

	            this.tlsName = cl.getOptionValue("tls", "");
	            this.caFile = cl.getOptionValue("caFile", "cacert.pem");
	            this.clientCertFile = cl.getOptionValue("clientCertFile", "cert.pem");
	            this.clientKeyFile = cl.getOptionValue("clientKeyFile", "key.pem");
			}

			containerNamePrefix = cl.getOptionValue("container-prefix", "aerospike");
	        useServicesAlternate = cl.hasOption("a");
		}
		catch (Exception ex) {
			throw new AerospikeException("Failed to parse args: " + argString);
		}
	}


	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = "mvn test [-Dargs='<options>']";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}

	/**
	 * Some database calls need to know how the server is configured.
	 */
	public void setServerSpecific(Cluster cluster) {
		Partitions partitions = cluster.partitionMap.get(namespace);

		if (partitions != null) {
			scMode = partitions.scMode;
		}

		Node node = cluster.getNodes()[0];
		serverVersion = node.getVersion();
		String editionFilter = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "release" : "edition";
		String namespaceFilter = "namespace/" + namespace;
		Map<String,String> map = Info.request(node, editionFilter, namespaceFilter);

		String editionToken = map.get(editionFilter);

		if (editionToken == null) {
			throw new AerospikeException(String.format(
				"Failed to get edition: host=%s port=%d", host, port));
		}

		if (editionToken.contains("Aerospike Enterprise Edition")) {
			enterprise = true;
		}

		String namespaceTokens = map.get(namespaceFilter);

		if (namespaceTokens == null) {
			throw new AerospikeException(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				host, port, namespace));
		}

		int nsup = parseInt(namespaceTokens, "nsup-period");

		if (nsup == 0) {
			hasTtl = parseBoolean(namespaceTokens, "allow-ttl-without-nsup");
		}
		else {
			hasTtl = true;
		}
	}

	private static int parseInt(String namespaceTokens, String name) {
		String s = parseString(namespaceTokens, name);
		return Integer.parseInt(s);
	}

	private static boolean parseBoolean(String namespaceTokens, String name) {
		String s = parseString(namespaceTokens, name);
		return Boolean.parseBoolean(s);
	}

	private static String parseString(String namespaceTokens, String name) {
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			throw new RuntimeException("Failed to find server config: " + name);
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}

		return namespaceTokens.substring(begin, end);
	}
}
