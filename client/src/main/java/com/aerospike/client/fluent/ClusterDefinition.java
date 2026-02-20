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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.aerospike.client.fluent.Log.Callback;
import com.aerospike.client.fluent.Log.Level;
import com.aerospike.client.fluent.command.AdminCommand;
import com.aerospike.client.fluent.command.Buffer;
import com.aerospike.client.fluent.policy.AuthMode;
import com.aerospike.client.fluent.policy.Behavior;

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
	private static final String CONFIG_PATH_ENV = "AEROSPIKE_CLIENT_CONFIG_URL";

	private SystemSettings userSuppliedSystemSettings;
	String clientVersion;
	String appId;
    String clusterName;
	String configPath;
    byte[] userName;
    byte[] password;
    byte[] passwordHash;
    int[] rackIds;
	Log.Context context;
    private Level logLevel = Level.INFO;
    private Callback callback = null;  // TODO Handle custom log callback.
	Map<String,String> ipMap;
    TlsBuilder tlsBuilder;
	AuthMode authMode = AuthMode.NONE;
	long maxSocketIdleNanosTrim = TimeUnit.SECONDS.toNanos(55);
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
    boolean useServicesAlternate;
	boolean forceSingleNode;

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
     * Copy constructor that creates a new ClusterDefinition with the same configuration
     * as the provided ClusterDefinition.
     *
     * <p>This creates a deep copy of all configuration settings including hosts,
     * authentication credentials, TLS settings, and connection parameters. The new
     * instance can be modified independently without affecting the original.</p>
     *
     * @param other the ClusterDefinition to copy from
     */
    public ClusterDefinition(ClusterDefinition other) {
    	this.userSuppliedSystemSettings = other.userSuppliedSystemSettings;
    	this.clientVersion = other.clientVersion;
		this.appId = other.appId;
		this.clusterName = other.clusterName;
		this.configPath = other.configPath;
		this.userName = other.userName;
		this.password = other.password;
		this.passwordHash = other.passwordHash;
		this.rackIds = other.rackIds;
		this.context = other.context;
		this.logLevel = other.logLevel;
		this.callback = other.callback;
		this.ipMap = other.ipMap;
		this.tlsBuilder = other.tlsBuilder;
		this.authMode = other.authMode;
		this.maxSocketIdleNanosTrim = other.maxSocketIdleNanosTrim;
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
		this.useServicesAlternate = other.useServicesAlternate;
		this.forceSingleNode = other.forceSingleNode;
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

    /**
     * Sets authentication credentials for the cluster connection using external authentication.
     *
     * <p>This method configures username/password authentication using Aerospike's
     * external authentication mode. External authentication uses clear text passwords
     * and is typically used with LDAP or other external authentication systems.</p>
     *
     * @param userName the username for authentication
     * @param password the password for authentication
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition withExternalCredentials(String userName, String password) {
    	return setCredentials(AuthMode.EXTERNAL, userName, password);
    }

    /**
     * Sets authentication credentials for the cluster connection using external insecure authentication.
     *
     * <p>This method configures username/password authentication using Aerospike's
     * external insecure authentication mode. This mode is similar to external authentication
     * but does not require TLS encryption. Use with caution as credentials are transmitted
     * in clear text.</p>
     *
     * @param userName the username for authentication
     * @param password the password for authentication
     * @return this ClusterDefinition for method chaining
     */
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

    /**
     * Configures the cluster connection to use certificate-based (PKI) authentication.
     *
     * <p>This method enables Public Key Infrastructure (PKI) authentication, which uses
     * client certificates instead of username/password credentials. When using PKI
     * authentication, the client certificate must be configured via TLS settings.</p>
     *
     * @return this ClusterDefinition for method chaining
     * @see #withTlsConfigOf()
     */
    public ClusterDefinition withCertificateCredentials() {
    	this.userName = null;
		this.password = null;
		this.authMode = AuthMode.PKI;
		return this;
    }

    /**
     * Checks whether authentication is enabled for this cluster definition.
     *
     * @return true if authentication is enabled (any mode other than NONE), false otherwise
     */
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
     * Set cluster name. This field is used to identify the cluster in metrics and logs.
     * This field is also used to validate that the server cluster name matches on initial
     * contact with the server node. If the actual cluster name doesn't match, the node
     * will not be added to the client's view of the cluster.
     *
     * @param clusterName the assigned cluster name
     * @return this ClusterDefinition for method chaining
     */
    public ClusterDefinition clusterName(String clusterName) {
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
        this.rackIds = racks;
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
     * Sets system settings for this cluster using a pre-built SystemSettings instance.
     *
     * <p>System settings control cluster-wide behavior such as connection pool size,
     * circuit breaker configuration, and cluster refresh intervals. These settings
     * are applied at connection time and can be dynamically updated.</p>
     *
     * <p><b>Priority:</b> Code-provided settings (Level 2) override hard-coded defaults
     * but are overridden by YAML default and cluster-specific settings.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * SystemSettings settings = SystemSettings.builder()
     *     .connections(ops -> ops
     *         .minimumConnectionsPerNode(150)
     *         .maximumConnectionsPerNode(500)
     *     )
     *     .build();
     *
     * new ClusterDefinition("localhost", 3000)
     *     .withSystemSettings(settings)
     *     .connect();
     * }</pre>
     *
     * @param settings the system settings to apply
     * @return this ClusterDefinition for method chaining
     * @see SystemSettings
     * @see #withSystemSettings(Consumer)
     */
    public ClusterDefinition withSystemSettings(SystemSettings settings) {
        this.userSuppliedSystemSettings = settings;
//        this.minConnsPerNode = settings.getMinimumConnectionsPerNode();
//        this.maxConnsPerNode = settings.getMaximumConnectionsPerNode();
//        this.maxErrorRate = settings.getMaximumErrorsInErrorWindow();
//        this.errorRateWindow = settings.getNumTendIntervalsInErrorWindow();
//        this.tendInterval = (int)settings.getTendInterval().toMillis();
//        this.maxSocketIdleNanosTrim = settings.getMaximumSocketIdleTime().toNanos();
        return this;
    }

    /**
     * Sets system settings for this cluster using a lambda configurator.
     *
     * <p>This is the recommended approach for inline configuration, consistent with
     * the Behavior API. It allows concise, fluent configuration without requiring
     * explicit builder management.</p>
     *
     * <p><b>Priority:</b> Code-provided settings (Level 2) override hard-coded defaults
     * but are overridden by YAML default and cluster-specific settings.</p>
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * new ClusterDefinition("localhost", 3000)
     *     .withSystemSettings(builder -> builder
     *         .connections(ops -> ops
     *             .minimumConnectionsPerNode(150)
     *             .maximumConnectionsPerNode(500)
     *         )
     *         .circuitBreaker(ops -> ops
     *             .maximumErrorsInErrorWindow(200)
     *         )
     *     )
     *     .connect();
     * }</pre>
     *
     * @param configurator lambda to configure system settings
     * @return this ClusterDefinition for method chaining
     * @see SystemSettings
     * @see #withSystemSettings(SystemSettings)
     */
    public ClusterDefinition withSystemSettings(Consumer<SystemSettings.Builder> configurator) {
        SystemSettings.Builder builder = SystemSettings.builder();
        configurator.accept(builder);
        SystemSettings settings = builder.build();
        return withSystemSettings(settings);
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
	 * For testing purposes only.
	 * <p>
	 * Should the AerospikeClient instance communicate with the first seed node only
	 * instead of using the data partition map to determine which node to send the
	 * database command.
	 * <p>
	 * Default: false
	 */
    public ClusterDefinition forceSingleNode(boolean forceSingleNode) {
        this.forceSingleNode = forceSingleNode;
        return this;
    }

    /**
     * Establishes a connection to the Aerospike cluster.
     *
     * <p>This method creates and returns a Cluster instance using the configured
     * parameters. The returned Cluster should be closed when no longer needed
     * to properly release resources.</p>
     *
     * <p><b>System Settings:</b> The cluster will use system settings based on
     * a 4-level priority hierarchy. If {@code validateClusterName()} was called,
     * cluster-specific settings from YAML will be used if available.</p>
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
        def.context = new Log.Context(def.clusterName);

		String configPath = System.getenv(CONFIG_PATH_ENV);

		if (configPath != null && !configPath.isEmpty()) {
			try {
				Behavior.startMonitoring(configPath);
			}
			catch (Throwable t) {
				throw new AerospikeException("Failed to read " + configPath +
					" specified in environment variable " + CONFIG_PATH_ENV, t);
			}
		}

		if (tlsBuilder != null) {
			tlsBuilder.createSslContext();
		}

        // Apply system settings to policy (4-level hierarchy)
        SystemSettings effectiveSettings = SystemSettingsRegistry.getInstance()
            .getEffectiveSettings(clusterName, userSuppliedSystemSettings);

        if (Log.debugEnabled()) {
            Log.debug("System Settings: " + effectiveSettings);
        }

        System.out.println("MaximumConnectionsPerNode = " + effectiveSettings.getMaximumConnectionsPerNode());
    	Cluster cluster = new Cluster(def, effectiveSettings);

        // Register with registry for dynamic updates
        SystemSettingsRegistry.getInstance()
            .registerCluster(cluster, clusterName, effectiveSettings);

        return cluster;
    }

    /**
     * Gets the effective hosts array, potentially creating new Host instances with TLS names
     * if TLS is configured and the existing hosts don't have TLS names set.
     */
    public Host[] getEffectiveHosts() {
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

    boolean isRackAware() {
    	return rackIds != null;
    }

	/**
	 * Gets the client version string.
	 *
	 * @return the client version, or "n/a" if not available
	 */
	public String getClientVersion() {
		return clientVersion;
	}

	/**
	 * Gets the application ID.
	 *
	 * @return the application ID, or null if not set
	 */
	public String getAppId() {
		return appId;
	}

	/**
	 * Gets the cluster name.
	 *
	 * @return the cluster name, or null if not set
	 */
	public String getClusterName() {
		return clusterName;
	}

	/**
	 * Gets the dynamic configuration file path.
	 *
	 * @return the configuration file path, or null if dynamic configuration is disabled
	 */
	public String getConfigPath() {
		return configPath;
	}

	/**
	 * Gets the username for authentication as a UTF-8 byte array.
	 *
	 * @return the username bytes, or null if authentication is not enabled
	 */
	public byte[] getUserName() {
		return userName;
	}

	/**
	 * Gets the password for authentication as a UTF-8 byte array.
	 *
	 * <p>Note: This returns the clear text password only for external authentication modes.
	 * For internal authentication, the password is not stored in clear text.</p>
	 *
	 * @return the password bytes, or null if authentication is not enabled or using internal auth
	 */
	public byte[] getPassword() {
		return password;
	}

	/**
	 * Gets the hashed password for authentication as a UTF-8 byte array.
	 *
	 * @return the hashed password bytes, or null if authentication is not enabled
	 */
	public byte[] getPasswordHash() {
		return passwordHash;
	}

	/**
	 * Gets the preferred rack IDs for rack-aware operations.
	 *
	 * @return an array of rack IDs, or null if rack awareness is not enabled
	 */
	public int[] getRackIds() {
		return rackIds;
	}

	/**
	 * Gets the logging context for this cluster definition.
	 *
	 * @return the log context, or null if not set
	 */
	public Log.Context getContext() {
		return context;
	}

	/**
	 * Gets the current logging level.
	 *
	 * @return the logging level
	 */
	public Level getLogLevel() {
		return logLevel;
	}

	/**
	 * Gets the custom log callback.
	 *
	 * @return the log callback, or null if using standard logging
	 */
	public Callback getCallback() {
		return callback;
	}

	/**
	 * Gets the IP address translation map.
	 *
	 * @return the IP map, or null if no IP translation is configured
	 */
	public Map<String, String> getIpMap() {
		return ipMap;
	}

	/**
	 * Gets the TLS builder configuration.
	 *
	 * @return the TLS builder, or null if TLS is not configured
	 */
	public TlsBuilder getTlsBuilder() {
		return tlsBuilder;
	}

	/**
	 * Gets the authentication mode.
	 *
	 * @return the authentication mode
	 */
	public AuthMode getAuthMode() {
		return authMode;
	}

	/**
	 * Gets the maximum socket idle time before trimming connections.
	 *
	 * @return the maximum socket idle time as a Duration
	 */
	public Duration getMaximumSocketIdleTime() {
		return Duration.ofNanos(maxSocketIdleNanosTrim);
	}

	/**
	 * Gets the minimum number of connections per node.
	 *
	 * @return the minimum connections per node
	 */
	public int getMinimumConnectionsPerNode() {
		return minConnsPerNode;
	}

	/**
	 * Gets the maximum number of connections per node.
	 *
	 * @return the maximum connections per node
	 */
	public int getMaximumConnectionsPerNode() {
		return maxConnsPerNode;
	}

	/**
	 * Gets the number of connection pools per node.
	 *
	 * @return the number of connection pools per node
	 */
	public int getConnPoolsPerNode() {
		return connPoolsPerNode;
	}

	/**
	 * Gets the interval in milliseconds between dynamic configuration checks.
	 *
	 * @return the configuration check interval in milliseconds
	 */
	public int getConfigInterval() {
		return configInterval;
	}

	/**
	 * Gets the cluster tend interval in milliseconds.
	 *
	 * @return the tend interval in milliseconds
	 */
	public int getTendInterval() {
		return tendInterval;
	}

	/**
	 * Gets the cluster tend timeout in milliseconds.
	 *
	 * @return the tend timeout in milliseconds
	 */
	public int getTendTimeout() {
		return tendTimeout;
	}

	/**
	 * Gets the login timeout in milliseconds.
	 *
	 * @return the login timeout in milliseconds
	 */
	public int getLoginTimeout() {
		return loginTimeout;
	}

	/**
	 * Gets the maximum number of errors allowed in the error window.
	 *
	 * @return the maximum errors in the error window
	 */
	public int getMaximumErrorsInErrorWindow() {
		return maxErrorRate;
	}

	/**
	 * Gets the number of tend intervals that make up the error window.
	 *
	 * @return the number of tend intervals in the error window
	 */
	public int getNumTendIntervalsInErrorWindow() {
		return errorRateWindow;
	}

	/**
	 * Checks whether cluster instantiation should fail if connection fails.
	 *
	 * @return true if cluster creation should fail on connection failure, false otherwise
	 */
	public boolean isFailIfNotConnected() {
		return failIfNotConnected;
	}

	/**
	 * Checks whether alternate services are enabled for cluster discovery.
	 *
	 * @return true if alternate services are enabled, false otherwise
	 */
	public boolean isUseServicesAlternate() {
		return useServicesAlternate;
	}

	/**
	 * Checks whether single node mode is forced (for testing purposes).
	 *
	 * @return true if single node mode is forced, false otherwise
	 */
	public boolean isForceSingleNode() {
		return forceSingleNode;
	}

	/**
	 * Gets the array of host definitions for this cluster.
	 *
	 * @return the array of Host objects
	 */
	public Host[] getHosts() {
		return hosts;
	}
}
