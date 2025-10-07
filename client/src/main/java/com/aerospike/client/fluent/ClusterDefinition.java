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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aerospike.client.fluent.Log.Callback;
import com.aerospike.client.fluent.Log.Level;
import com.aerospike.client.fluent.policy.AuthMode;

/**
 * Builder class for configuring and creating Aerospike cluster connections.
 *
 * <p>This class provides a fluent API for configuring various connection parameters
 * such as authentication, logging, rack awareness, and cluster validation before
 * establishing a connection to an Aerospike cluster.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * Cluster cluster = new ClusterDefinition("localhost", 3100)
 *     .withNativeCredentials("username", "password")
 *     .usingServicesAlternate()
 *     .preferredRacks(1, 2)
 *     .validateClusterNameIs("my-cluster")
 *     .connect();
 * }</pre>
 *
 * @see Cluster
 * @see Session
 */
public class ClusterDefinition {
	String clientVersion;
	String appId;
    String clusterName;
	String configPath;
    byte[] userName;
    byte[] password;
    byte[] passwordHash;
    int[] preferrredRacks;
    private Level logLevel = Level.INFO;
    private Callback callback = null;  // TODO Handle custom log callback.
	Map<String,String> ipMap;
    TlsBuilder tlsBuilder;
	AuthMode authMode = AuthMode.NONE;
	int minConnsPerNode;
	int maxConnsPerNode = 100;
	int connPoolsPerNode = 1;
	int configInterval = 60000;
	int tendInterval = 1000;
	int tendTimeout = 1000;
	int loginTimeout = 5000;
	int maxErrorRate = 100;
	int errorRateWindow = 1;
    boolean failIfNotConnected = true;
	boolean validateClusterName;
    boolean useServicesAlternate;

    private final Host[] hosts;

    /**
     * Creates a cluster definition for a single host.
     *
     * @param hostname the hostname or IP address of the Aerospike server
     * @param port the port number for the Aerospike server
     */
    public ClusterDefinition(String hostname, int port) {
        hosts = new Host[] { new Host(hostname, port) };
        setup();
    }

    /**
     * Creates a cluster definition for multiple hosts.
     *
     * @param hosts array of Host objects representing the Aerospike servers
     */
    public ClusterDefinition(Host ... hosts) {
        this.hosts = hosts;
        setup();
    }

    /**
     * Creates a cluster definition for multiple hosts.
     *
     * @param hosts list of Host objects representing the Aerospike servers
     */
    public ClusterDefinition(List<Host> hosts) {
        this.hosts = hosts.toArray(new Host[0]);
        setup();
    }

    /**
     * Copy constructor.
     */
    public ClusterDefinition(ClusterDefinition other) {
    	this.clientVersion = other.clientVersion;
		this.appId = other.appId;
		this.clusterName = other.clusterName;
		this.configPath = other.configPath;
		this.userName = other.userName;
		this.password = other.password;
		this.passwordHash = other.passwordHash;
		this.preferrredRacks = other.preferrredRacks;
		this.logLevel = other.logLevel;
		this.callback = other.callback;
		this.ipMap = other.ipMap;
		this.tlsBuilder = other.tlsBuilder;
		this.authMode = other.authMode;
		this.minConnsPerNode = other.minConnsPerNode;
		this.maxConnsPerNode = other.maxConnsPerNode;
		this.connPoolsPerNode = other.connPoolsPerNode;
		this.configInterval = other.configInterval;
		this.tendInterval = other.tendInterval;
		this.tendTimeout = other.tendTimeout;
		this.loginTimeout = other.loginTimeout;
		this.maxErrorRate = other.maxErrorRate;
		this.errorRateWindow = other.errorRateWindow;
		this.failIfNotConnected = other.failIfNotConnected;
		this.validateClusterName = other.validateClusterName;
		this.useServicesAlternate = other.useServicesAlternate;
		this.hosts = other.hosts;
    }

    private void setup() {
        Log.setLevel(logLevel);
        Log.setCallbackStandard();

		this.clientVersion = Optional.ofNullable(getClass().getPackage())
			.map(Package::getImplementationVersion)
			.orElse("n/a");
    }

    /**
     * Sets authentication credentials for the cluster connection.
     *
     * <p>This method configures username/password authentication using Aerospike's
     * internal authentication mode. Pass null for both parameters to disable authentication.</p>
     *
     * @param userName the username for authentication, or null to disable auth
     * @param password the password for authentication, or null to disable auth
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition withNativeCredentials(String userName, String password) {
    	return setCredentials(AuthMode.INTERNAL, userName, password);
    }

    public ClusterDefinition withExternalCredentials(String userName, String password) {
    	return setCredentials(AuthMode.EXTERNAL, userName, password);
    }

    public ClusterDefinition withExternalInsecureCredentials(String userName, String password) {
    	return setCredentials(AuthMode.EXTERNAL_INSECURE, userName, password);
    }

    private ClusterDefinition setCredentials(AuthMode mode, String userName, String password) {
    	if (userName == null || userName.isEmpty()) {
    		this.authMode = AuthMode.NONE;
    		this.userName = null;
    		this.password = null;
    	}
    	else {
    		this.authMode = mode;
    		this.userName = Buffer.stringToUtf8(userName);

			// Only store clear text password if external authentication is used.
			if (authMode != AuthMode.INTERNAL) {
				this.password = Buffer.stringToUtf8(password);
			}

			if (password == null) {
				password = "";
			}

			password = AdminCommand.hashPassword(password);
			this.passwordHash = Buffer.stringToUtf8(password);
    	}
		return this;
   }

    public ClusterDefinition withCertificateCredentials() {
    	this.userName = null;
		this.password = null;
		this.authMode = AuthMode.PKI;
		return this;
    }

    public boolean isAuthEnabled() {
    	return authMode != AuthMode.NONE;
    }

	/**
	 * Set application ID. Metrics are loosely tied to this.
	 * Changing the appId will not reset the metric counters.
	 */
    public ClusterDefinition appId(String appId) {
        this.appId = appId;
        return this;
    }

    /**
     * Validates that the cluster name matches the expected value.
     *
     * <p>This enables cluster name validation to ensure the client connects to
     * the expected cluster. If the actual cluster name doesn't match, the connection
     * will fail.</p>
     *
     * @param clusterName the expected cluster name to validate against
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition validateClusterNameIs(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    /**
     * Sets preferred racks for rack-aware operations.
     *
     * <p>This enables rack awareness and specifies which racks should be preferred
     * for read operations. Rack awareness helps improve performance by reading from
     * local racks when possible.</p>
     *
     * @param racks the rack IDs to prefer, in order of preference
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition preferringRacks(int ... racks) {
        this.preferrredRacks = racks;
        return this;
    }

    /**
     * Enables the use of alternate services for cluster discovery.
     *
     * <p>When enabled, the client will use alternate service endpoints for
     * cluster discovery, which can be useful in certain network configurations
     * or when using service mesh solutions.</p>
     *
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition usingServicesAlternate() {
        this.useServicesAlternate = true;
        return this;
    }

    /**
     * Sets the logging level for the Aerospike client.
     *
     * <p>This controls the verbosity of client-side logging. Available levels
     * include DEBUG, INFO, WARN, and ERROR. Setting to null disables logging.</p>
     *
     * @param logLevel the desired logging level, or null to disable logging
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition withLogLevel(Level logLevel) {
        // TODO: Need a new log level of NONE, so the callback is not over written by changing the log evel
        if (logLevel == null) {
            Log.setCallback(null);
            Log.debug("Setting log level to " + logLevel);
        }
        else {
            this.logLevel = logLevel;
            Log.setLevel(logLevel);
            Log.debug("Setting log level to " + logLevel);
        }
        return this;
    }

    /**
     * Sets a custom log callback for handling log messages.
     *
     * <p>This allows you to provide a custom implementation for handling
     * log messages from the Aerospike client. Pass null to use the standard
     * logging callback.</p>
     *
     * @param callback the custom log callback, or null for standard logging
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition useLogSink(Callback callback) {
        if (callback == null) {
            Log.setCallbackStandard();
        }
        else {
            Log.setCallback(callback);
        }
        return this;
    }

    /**
     * Begins TLS configuration using a fluent builder pattern.
     *
     * <p>This method returns a TlsBuilder that allows you to configure various
     * TLS settings such as TLS name, CA file, protocols, ciphers, and other
     * TLS-specific options. Call {@code done()} on the TlsBuilder to return
     * to this ClusterDefinition for further configuration.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * ClusterDefinition cluster = new ClusterDefinition("localhost", 3100)
     *     .withTlsConfigOf()
     *         .tlsName("myTlsName")
     *         .caFile("myCaFile")
     *         .protocols("TLSv1.2", "TLSv1.3")
     *     .done()
     *     .withNativeCredentials("myUser", "password");
     * }</pre>
     *
     * @return a TlsBuilder for configuring TLS settings
     */
    public TlsBuilder withTlsConfigOf() {
        return new TlsBuilder(this);
    }

    /**
     * Package-private setter for TlsBuilder to set itself when done() is called.
     */
    void setTlsBuilder(TlsBuilder tlsBuilder) {
        this.tlsBuilder = tlsBuilder;
    }

	/**
	 * Set whether cluster instantiation should fail if the client fails to connect to a seed or
	 * all the seed's peers.
	 * <p>
	 * If true, throw an exception if all seed connections fail or a seed is valid,
	 * but all peers from that seed are not reachable.
	 * <p>
	 * If false, a partial cluster will be created and the client will automatically connect
	 * to the remaining nodes when they become available.
	 * <p>
	 * Default: true
	 */
    public ClusterDefinition failIfNotConnected(boolean failIfNotConnected) {
        this.failIfNotConnected = failIfNotConnected;
        return this;
    }

	/**
	 * When validateClusterName is true and clusterName is populated, verify that
	 * clusterName matches the cluster-name field in the service section in each server
	 * configuration. This ensures that the specified seed nodes belong to the expected cluster on
	 * startup. If not, the client will refuse to add the node to the client's view of the cluster.
	 * <p>
	 * Default: false
	 */
    public ClusterDefinition validateClusterName(boolean validateClusterName) {
        this.validateClusterName = validateClusterName;
        return this;
    }

    /**
	 * Set dynamic configuration path. If not null, dynamic configuration is enabled.
	 * <p>
	 * Default: 60000
	 */
    public ClusterDefinition configPath(String configPath) {
        this.configPath = configPath;
        return this;
    }

    /**
	 * Set milliseconds between dynamic configuration check for file modifications.
	 * <p>
	 * Default: 60000
	 */
    public ClusterDefinition configInterval(int configInterval) {
        this.configInterval = configInterval;
        return this;
    }

    /**
	 * Set interval in milliseconds between cluster tends.
	 * <p>
	 * Default: 1000
	 */
    public ClusterDefinition tendInterval(int tendInterval) {
        this.tendInterval = tendInterval;
        return this;
    }

	/**
	 * Cluster tend info call timeout in milliseconds. The timeout when opening a connection
	 * to the server node for the first time and when polling each node for cluster status.
	 * <p>
	 * Default: 1000
	 */
    public ClusterDefinition tendTimeout(int tendTimeout) {
        this.tendTimeout = tendTimeout;
        return this;
    }

	/**
	 * Set login timeout in milliseconds.  The timeout is used when user authentication is enabled
	 * and a node login is being performed.
	 * <p>
	 * Default: 5000
	 */
    public ClusterDefinition loginTimeout(int loginTimeout) {
        this.loginTimeout = loginTimeout;
        return this;
    }

	/**
	 * Set IP translation table is used in cases where different clients use different server
	 * IP addresses.  This may be necessary when using clients from both inside and outside
	 * a local area network.  Default is no translation.
	 * <p>
	 * The key is the IP address returned from friend info requests to other servers.  The
	 * value is the real IP address used to connect to the server.
	 * <p>
	 * Default: null (no IP address translation)
	 */
    public ClusterDefinition ipMap(Map<String,String> ipMap) {
        this.ipMap = ipMap;
        return this;
    }

	/**
	 * Set maximum number of errors allowed per node per {@link #errorRateWindow} before backoff
	 * algorithm throws {@link com.aerospike.client.fluent.AerospikeException.Backoff} on database
	 * commands to that node. If maxErrorRate is zero, there is no error limit and
	 * the exception will never be thrown.
	 * <p>
	 * The counted error types are any error that causes the connection to close (socket errors
	 * and client timeouts) and {@link com.aerospike.client.ResultCode#DEVICE_OVERLOAD}.
	 * <p>
	 * Default: 100
	 */
    public ClusterDefinition maxErrorRate(int maxErrorRate) {
        this.maxErrorRate = maxErrorRate;
        return this;
    }

	/**
	 * Set number of cluster tend iterations that defines the window for {@link #maxErrorRate}.
	 * One tend iteration is defined as {@link #tendInterval} plus the time to tend all nodes.
	 * At the end of the window, the error count is reset to zero and backoff state is removed
	 * on all nodes.
	 * <p>
	 * Default: 1
	 */
    public ClusterDefinition errorRateWindow(int errorRateWindow) {
        this.errorRateWindow = errorRateWindow;
        return this;
    }

	/**
	 * Set number of synchronous connection pools used for each node.  Machines with 8 cpu cores or
	 * less usually need just one connection pool per node.  Machines with a large number of cpu
	 * cores may have their synchronous performance limited by contention for pooled connections.
	 * Contention for pooled connections can be reduced by creating multiple mini connection pools
	 * per node.
	 * <p>
	 * Default: 1
	 */
    public ClusterDefinition connPoolsPerNode(int connPoolsPerNode) {
        this.connPoolsPerNode = connPoolsPerNode;
        return this;
    }

	/**
	 * Set minimum number of synchronous connections allowed per server node. Preallocate min connections
	 * on client node creation.  The client will periodically allocate new connections if count falls
	 * below min connections.
	 * <p>
	 * Server proto-fd-idle-ms and client {@link ClientPolicy#maxSocketIdle} should be set to zero
	 * (no reap) if minConnsPerNode is greater than zero.  Reaping connections can defeat the purpose
	 * of keeping connections in reserve for a future burst of activity.
	 * <p>
	 * Default: 0
	 */
    public ClusterDefinition minConnsPerNode(int minConnsPerNode) {
        this.minConnsPerNode = minConnsPerNode;
        return this;
    }

	/**
	 * Set maximum number of synchronous connections allowed per server node.  Commands will go
	 * through retry logic and potentially fail with "ResultCode.NO_MORE_CONNECTIONS" if the maximum
	 * number of connections would be exceeded.
	 * <p>
	 * The number of connections used per node depends on concurrent commands in progress
	 * plus sub-commands used for parallel multi-node commands (batch, scan, and query).
	 * One connection will be used for each command.
	 * <p>
	 * Default: 100
	 */
    public ClusterDefinition maxConnsPerNode(int maxConnsPerNode) {
        this.maxConnsPerNode = maxConnsPerNode;
        return this;
    }

    /**
     * Establishes a connection to the Aerospike cluster.
     *
     * <p>This method creates and returns a Cluster instance using the configured
     * parameters. The returned Cluster should be closed when no longer needed
     * to properly release resources.</p>
     *
     * <p>Example with try-with-resources:</p>
     * <pre>{@code
     * try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
     *     Session session = cluster.createSession(Behavior.DEFAULT);
     *     // Use the session...
     * }
     * }</pre>
     *
     * @return a connected Cluster instance
     * @see Cluster
     * @see Cluster#close()
     */
    public Cluster connect() {
        ClusterDefinition def = new ClusterDefinition(this);
        Host[] seeds = def.getEffectiveHosts();
    	return new Cluster(def, seeds);
    }

    /**
     * Gets the effective hosts array, potentially creating new Host instances with TLS names
     * if TLS is configured and the existing hosts don't have TLS names set.
     */
    private Host[] getEffectiveHosts() {
        // If no TLS configuration or no TLS name specified, return original hosts
        if (tlsBuilder == null || !tlsBuilder.isTlsEnabled() || tlsBuilder.getTlsName() == null) {
            return hosts;
        }

        String tlsName = tlsBuilder.getTlsName();
        Host[] newHosts = new Host[hosts.length];

        for (int i = 0; i < hosts.length; i++) {
            Host originalHost = hosts[i];

            // Only create new Host with TLS name if the existing host doesn't have one
            if (originalHost.tlsName == null) {
                newHosts[i] = new Host(originalHost.name, tlsName, originalHost.port);
            } else {
                // Keep the existing host as-is if it already has a TLS name
                newHosts[i] = originalHost;
            }
        }

        return newHosts;
    }

    Host[] getHosts() {
    	return hosts;
    }

    boolean isRackAware() {
    	return preferrredRacks != null;
    }
}