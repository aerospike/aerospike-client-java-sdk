package com.aerospike.benchmarks;

import picocli.CommandLine.Spec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

import java.util.Arrays;

import com.aerospike.client.sdk.policy.AuthMode;

/**
 * Represents connection options for the Aerospike database client. This class provides
 * configuration options for host connection, authentication, TLS/SSL security, connection pooling,
 * and various timeouts.
 *
 * <p>Each option is annotated with the picocli {@link Option} annotation, allowing these options to
 * be specified via command-line arguments. Default values are provided for most options when
 * appropriate.
 *
 * <p>Connection options include:
 *
 * <ul>
 *   <li>Host configuration (hosts, port)
 *   <li>Authentication settings (user, password, authMode)
 *   <li>TLS/SSL security options (tlsEnable, tlsProtocols, etc.)
 *   <li>Connection pooling parameters (connPoolsPerNode, minConnsPerNode, etc.)
 *   <li>Various timeout configurations for different operations (connectTimeout, socketTimeout,
 *       etc.)
 *   <li>Cluster management settings (tendInterval, servicesAlternate, etc.)
 * </ul>
 *
 * <p>This class provides getter methods for all options and setter methods for options that require
 * validation.
 */
public class ConnectionOptions {

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-h", "-hosts", "--hosts"},
            description =
                    "List of seed hosts in format: hostname1[:tlsname][:port1],...\n"
                            + "\tThe tlsname is only used when connecting with a secure TLS enabled server.\n"
                            + "\tIf the port is not specified, the default port is used.\n"
                            + "\tIPv6 addresses must be enclosed in square brackets.\n"
                            + "Default: localhost:3000\n"
                            + "Examples:\n"
                            + "\thost1\n"
                            + "\thost1:3000,host2:3000\n"
                            + "\t192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n")
    private String hosts = "localhost:3000";

    /**
     * Set the default port number for connecting to Aerospike server.
     *
     * @param value The port number, must be between 1 and 65535.
     * @throws picocli.CommandLine.ParameterException If the port number is out of valid range (1-65535).
     */
    @Option(
            names = {"-p", "-port", "--port"},
            description = "Set the default port on which to connect to Aerospike. Default: 3000\n",
            defaultValue = "3000")
    public void setPort(int value) throws ParameterException {
        if (value < 1 || value > 65535) {
            throw new ParameterException(
                    spec.commandLine(), String.format(Constants.INVALID_PORT_MESSAGE, value));
        }
        port = value;
    }

    private Integer port = 3000;

    @Option(
            names = {"-U", "-user", "--user"},
            description = "User name used for authentication.")
    private String user;

    @Option(
            names = {"-P", "-password", "--password"},
            description = "Password used for authentication.",
            interactive = true,
            arity = "0..1")
    private char[] password;

    @Option(
            names = {"-sa", "-servicesAlternate", "--servicesAlternate"},
            description =
                    "Set to enable use of services-alternate instead of services in info request during"
                            + " cluster tending. Default: false")
    private boolean servicesAlternate;


    /**
     * Set the authentication mode for connecting to the Aerospike server.
     *
     * <p>This method validates that the provided authentication mode is one of the supported values
     * defined in the AuthMode enum.
     *
     * @param value The authentication mode to set. Valid values are INTERNAL, EXTERNAL,
     *              EXTERNAL_INSECURE, PKI, PKI_INSECURE (case-insensitive).
     * @throws ParameterException If the provided authentication mode is not a valid AuthMode value.
     */
    @Option(
            names = {"-auth", "-authMode", "--authMode"},
            description =
                    "Authentication mode.\n"
                            + "Values:  INTERNAL, EXTERNAL, EXTERNAL_INSECURE, PKI, PKI_INSECURE")
    public void setAuthMode(String value) throws ParameterException {
        // check only allowed values AuthMode.values()
        String valueUpper = value.toUpperCase();
        try {
            AuthMode.valueOf(valueUpper);
        } catch (IllegalArgumentException e) {
            throw new ParameterException(
                    spec.commandLine(),
                    String.format(
                            Constants.INVALID_AUTH_MODE_MESSAGE, value, Arrays.toString(AuthMode.values())));
        }
        authMode = valueUpper;
    }

    private String authMode;

    @Option(
            names = {"-c", "-clusterName", "--clusterName"},
            description =
                    "Expected cluster name (logs and server validation). "
                            + "If omitted, the benchmark also checks env AEROSPIKE_CLUSTER_NAME.")
    private String clusterName;

    @Option(
            names = {"-lt", "-loginTimeout", "--loginTimeout"},
            description =
                    "Set expected loginTimeout in milliseconds. The timeout is used when user "
                            + "authentication is enabled and a node login is being performed. Default: 5000")
    private Integer loginTimeout;

    @Option(
            names = {"-tt", "-tendTimeout", "--tendTimeout"},
            description = "Set cluster tend info call timeout in milliseconds. Default: 1000")
    private Integer tendTimeout;

    @Option(
            names = {"-ti", "-tendInterval", "--tendInterval"},
            description = "Interval between cluster tends in milliseconds. Default: 1000")
    private Integer tendInterval;

    @Option(
            names = {"-maxSocketIdle"},
            description =
                    "Maximum socket idle in seconds. Socket connection pools will discard sockets that have"
                            + " been idle longer than the maximum. Default: 0")
    private Integer maxSocketIdle;

    @Option(
            names = {"-maxErrorRate"},
            description = "Maximum number of errors allowed per node per tend iteration. Default: 100")
    private Integer maxErrorRate;

    @Option(
            names = {"-errorRateWindow"},
            description =
                    "Number of cluster tend iterations that defines the window for maxErrorRate. Default: 1")
    private Integer errorRateWindow;

    @Option(
            names = {"Y", "-connPoolsPerNode", "--connPoolsPerNode"},
            description = "Number of synchronous connection pools per node.  Default 1.")
    private Integer connPoolsPerNode;

    @Option(
            names = {"-minConnsPerNode"},
            description = "Minimum number of sync connections pre-allocated per server node. Default: 0")
    private Integer minConnectionsPerNode;

    @Option(
            names = {"-maxConnsPerNode"},
            description = "Maximum number of sync connections allowed per server node. Default: 100")
    private Integer maxConnectionsPerNode;

    public String getHosts() {
        return hosts;
    }

    public Integer getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public char[] getPassword() {
        return password;
    }

    public boolean isServicesAlternate() {
        return servicesAlternate;
    }

    public String getAuthMode() {
        return authMode;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Integer getLoginTimeout() {
        return loginTimeout;
    }

    public Integer getTendTimeout() {
        return tendTimeout;
    }

    public Integer getTendInterval() {
        return tendInterval;
    }

    public Integer getMaxSocketIdle() {
        return maxSocketIdle;
    }

    public Integer getMaxErrorRate() {
        return maxErrorRate;
    }

    public Integer getErrorRateWindow() {
        return errorRateWindow;
    }

    public Integer getConnPoolsPerNode() {
        return connPoolsPerNode;
    }

    public Integer getMinConnectionsPerNode() {
        return minConnectionsPerNode;
    }

    public Integer getMaxConnectionsPerNode() {
        return maxConnectionsPerNode;
    }


}
