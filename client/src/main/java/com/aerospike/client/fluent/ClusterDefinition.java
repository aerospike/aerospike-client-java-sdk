package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.Log.Callback;
import com.aerospike.client.fluent.Log.Level;

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
    private String userName;
    private String password;
    private String clusterName;
    private int[] preferrredRacks;
    private boolean useServicesAlternate = false;
    private Level logLevel = Level.WARN;
    private Callback callback = null;
    TlsBuilder tlsBuilder = null;

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

    private void setup() {
        Log.setLevel(logLevel);
        Log.setCallbackStandard();
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
        this.userName = userName == null || userName.isEmpty() ? null : userName;
        this.password = password;
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

/* TODO Remove
    private ClientPolicy getPolicy() {
        ClientPolicy policy = new ClientPolicy();
        policy.user = userName;
        policy.password = password;
        policy.authMode = AuthMode.INTERNAL;

        policy.useServicesAlternate = useServicesAlternate;
        if (preferrredRacks != null && preferrredRacks.length > 0) {
            policy.rackAware = true;
            policy.rackIds = new ArrayList<>();
            for (int thisRack : preferrredRacks) {
                policy.rackIds.add(thisRack);
            }
        }

        if (clusterName != null) {
            policy.clusterName = clusterName;
            policy.validateClusterName = true;
        }

        if (tlsBuilder != null && tlsBuilder.isTlsEnabled()) {
            policy.tlsPolicy = new TlsPolicy();


            // Use custom SSLContext if available (from PEM files, key stores, etc.)
            javax.net.ssl.SSLContext sslContext = tlsBuilder.createSslContext();
            if (sslContext != null) {
                policy.tlsPolicy.context = sslContext;
            }

            // Configure additional TLS policy settings
            if (tlsBuilder.getProtocols() != null) {
                policy.tlsPolicy.protocols = tlsBuilder.getProtocols();
            }
            if (tlsBuilder.getCiphers() != null) {
                policy.tlsPolicy.ciphers = tlsBuilder.getCiphers();
            }
            policy.tlsPolicy.forLoginOnly = tlsBuilder.isForLoginOnly();

            // Set revocation certificates if configured
            if (tlsBuilder.getRevokeCertificates() != null) {
                policy.tlsPolicy.revokeCertificates = tlsBuilder.getRevokeCertificates();
            }
            // Note: revokeCertificatesForLoginOnly field not available in current TlsPolicy
            // policy.tlsPolicy.revokeCertificatesForLoginOnly = tlsBuilder.isRevokeCertificatesForLoginOnly();
        }
        return policy;
    }
*/
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
        Host[] seeds = getEffectiveHosts();
    	return new Cluster(this, seeds);
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
}