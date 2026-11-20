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
package com.aerospike.client.sdk.tend;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.TlsBuilder;
import com.aerospike.client.sdk.command.AdminCommand;
import com.aerospike.client.sdk.command.AdminCommand.LoginCommand;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.command.Connection;
import com.aerospike.client.sdk.command.Info;
import com.aerospike.client.sdk.util.Crypto;
import com.aerospike.client.sdk.util.Util;
import com.aerospike.client.sdk.util.Version;

public final class NodeValidator {
    private static final Logger log = LoggerFactory.getLogger(Loggers.TEND);
    private static final int STAGGER_DELAY = 100;
    private static final int CONNECTED = 0;
    private static final int PENDING = 1;
    private static final int FAILED = 2;

    public Node fallback;
    public String name;
    public String hostname;
    public Host primaryHost;
    public InetSocketAddress primaryAddress;
    public Connection primaryConn;
    public byte[] sessionToken;
    public long sessionExpiration;
    public int features;
    public Version version;

    /**
     * Return first valid node referenced by seed host aliases. In most cases, aliases
     * reference a single node.  If round robin DNS configuration is used, the seed host
     * may have several addresses that reference different nodes in the cluster.
     */
    public Node seedNode(Cluster cluster, Host host, Peers peers) throws Throwable {
        name = null;
        hostname = null;
        primaryHost = null;
        primaryAddress = null;
        primaryConn = null;
        sessionToken = null;
        sessionExpiration = 0;
        features = 0;

        InetAddress[] addresses = getAddresses(host);
        Throwable exception = null;

        for (InetAddress address : addresses) {
            try {
                validateAddress(cluster, address, host.tlsName, host.port, true, 0);
                setHostname(host, address);

                Node node = new Node(cluster, this);

                if (validatePeers(cluster, peers, node)) {
                    return node;
                }
            }
            catch (Throwable e) {
                // Log exception and continue to next alias.
                if (log.isDebugEnabled()) {
                    log.atDebug()
                        .addKeyValue(Cluster.CONTEXT, cluster.getClusterDefinition().getClusterName())
                        .log("Address " + address + ' ' + host.port + " failed: " +
                            Util.getErrorMessage(e));
                }

                if (exception == null) {
                    exception = e;
                }
            }
        }

        // Fallback signifies node exists, but is suspect.
        // Return null so other seeds can be tried.
        if (fallback != null) {
            return null;
        }

        // Exception can't be null here because getAddresses() will throw exception
        // if aliases length is zero.
        throw exception;
    }

    private boolean validatePeers(Cluster cluster, Peers peers, Node node) {
        if (peers == null) {
            return true;
        }

        try {
            peers.refreshCount = 0;
            node.refreshPeers(peers);
        }
        catch (Throwable e) {
            node.close();
            throw e;
        }

        if (node.getPeersCount() == 0) {
            // Node is suspect because multiple seeds are used and node does not have any peers.
            if (fallback == null) {
                fallback = node;
            }
            else {
                node.close();
            }
            return false;
        }

        // Node is valid. Drop fallback if it exists.
        if (fallback != null) {
            if (log.isInfoEnabled()) {
                log.atInfo()
                    .addKeyValue(Cluster.CONTEXT, cluster.getClusterDefinition().getClusterName())
                    .log("Skip orphan node: " + fallback);
            }
            fallback.close();
            fallback = null;
        }
        return true;
    }

    /**
     * Verify that a host alias references a valid node.
     */
    public void validateNode(Cluster cluster, Host host) throws Throwable {
        InetAddress[] addresses = getAddresses(host);

        if (addresses.length > 1) {
            validateAny(cluster, host, addresses);
            return;
        }

        Throwable exception = null;

        for (InetAddress address : addresses) {
            try {
                validateAddress(cluster, address, host.tlsName, host.port, false, 0);
                setHostname(host, address);
                cluster.setValidAddress(host, address);
                return;
            }
            catch (Throwable e) {
                // Log exception and continue to next alias.
                if (log.isDebugEnabled()) {
                    log.atDebug()
                        .addKeyValue(Cluster.CONTEXT, cluster.getClusterDefinition().getClusterName())
                        .log("Address " + address + ' ' + host.port + " failed: " +
                            Util.getErrorMessage(e));
                }

                if (exception == null) {
                    exception = e;
                }
            }
        }
        // Exception can't be null here because getAddresses() will throw exception
        // if aliases length is zero.
        throw exception;
    }

    /**
     * Validate the first address of a host with several addresses whose connect completes. Connects
     * are started one at a time, the last validated address first, then in DNS order, the next one
     * when the stagger delay passes or an earlier connect fails. All connects end by
     * min(connectTimeout, tendInterval). When the address fails validation, the remaining addresses
     * are tried the same way until that bound.
     */
    private void validateAny(Cluster cluster, Host host, InetAddress[] addresses) throws Throwable {
        InetAddress preferred = cluster.getValidAddress(host);
        List<InetAddress> remaining = new ArrayList<InetAddress>(addresses.length);

        if (preferred != null) {
            for (InetAddress address : addresses) {
                if (address.equals(preferred)) {
                    remaining.add(address);
                    break;
                }
            }
        }

        for (InetAddress address : addresses) {
            if (! address.equals(preferred)) {
                remaining.add(address);
            }
        }

        ClusterDefinition def = cluster.getClusterDefinition();
        int bound = (def.getTendTimeout() > 0) ? Math.min(def.getTendTimeout(), def.getTendInterval()) : def.getTendInterval();
        long delay = TimeUnit.MILLISECONDS.toNanos(Math.min(STAGGER_DELAY, bound / addresses.length));
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(bound);
        Throwable exception = null;

        while (true) {
            InetAddress address = connectFirst(def, host, remaining, delay, deadline);

            if (address == null) {
                break;
            }

            try {
                validateAddress(cluster, address, host.tlsName, host.port, false, 0);
                setHostname(host, address);
                cluster.setValidAddress(host, address);
                return;
            }
            catch (Throwable e) {
                if (log.isDebugEnabled()) {
                    log.atDebug()
                        .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                        .log("Address " + address + ' ' + host.port + " failed: " + Util.getErrorMessage(e));
                }

                if (exception == null) {
                    exception = e;
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
        throw new AerospikeException.Connection("No connection to " + host + " completed for " + addresses.length +
            " addresses within " + bound + "ms");
    }

    /**
     * Return the first address whose connect completes, or null when none completes by the deadline.
     * The returned address and the addresses whose connect failed are removed from the list.
     */
    private static InetAddress connectFirst(
        ClusterDefinition def, Host host, List<InetAddress> addresses, long delay, long deadline
    ) throws IOException {

        InetAddress connected = null;
        int next = 0;
        int pending = 0;

        try (Selector selector = Selector.open()) {
            try {
                long start = System.nanoTime();

                while (connected == null) {
                    long now = System.nanoTime();

                    if (now - deadline >= 0) {
                        break;
                    }

                    if (next < addresses.size() && now - start >= 0) {
                        InetAddress address = addresses.get(next);
                        int state = startConnect(def, selector, address, host.port);

                        if (state == CONNECTED) {
                            connected = address;
                            break;
                        }

                        if (state == PENDING) {
                            pending++;
                            next++;
                            start = now + delay;
                        }
                        else {
                            addresses.remove(next);
                        }
                        continue;
                    }

                    if (pending == 0) {
                        break;
                    }

                    long wait = ((next < addresses.size() && start - deadline < 0) ? start : deadline) - now;
                    selector.select(TimeUnit.NANOSECONDS.toMillis(wait + 999999));

                    for (SelectionKey key : selector.selectedKeys()) {
                        SocketChannel channel = (SocketChannel)key.channel();
                        InetAddress address = (InetAddress)key.attachment();

                        try {
                            if (! channel.finishConnect()) {
                                continue;
                            }

                            if (connected == null) {
                                connected = address;
                            }
                        }
                        catch (IOException e) {
                            if (log.isDebugEnabled()) {
                                log.atDebug()
                                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                                    .log("Address " + address + ' ' + host.port + " failed: " + Util.getErrorMessage(e));
                            }
                            addresses.remove(address);
                            next--;
                            start = System.nanoTime();
                        }
                        channel.close();
                        pending--;
                    }
                    selector.selectedKeys().clear();
                }
            }
            finally {
                for (SelectionKey key : selector.keys()) {
                    if (key.isValid()) {
                        key.channel().close();
                    }
                }
            }
        }

        if (connected != null) {
            addresses.remove(connected);
        }
        return connected;
    }

    private static int startConnect(
        ClusterDefinition def, Selector selector, InetAddress address, int port
    ) throws IOException {
        SocketChannel channel = SocketChannel.open();

        try {
            channel.configureBlocking(false);

            if (channel.connect(new InetSocketAddress(address, port))) {
                channel.close();
                return CONNECTED;
            }
            channel.register(selector, SelectionKey.OP_CONNECT, address);
            return PENDING;
        }
        catch (IOException e) {
            channel.close();

            if (log.isDebugEnabled()) {
                log.atDebug()
                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                    .log("Address " + address + ' ' + port + " failed: " + Util.getErrorMessage(e));
            }
            return FAILED;
        }
    }

    /**
     * Verify that one address of a host references a valid node. The connect, login and info
     * requests end within the timeout.
     */
    public void validateAlias(Cluster cluster, Host host, InetAddress address, int timeout) throws Exception {
        validateAddress(cluster, address, host.tlsName, host.port, false, timeout);
        setHostname(host, address);
    }

    private void setHostname(Host host, InetAddress address) {
        hostname = (isHostname(host.name) && address.equals(primaryAddress.getAddress())) ? host.name : null;
    }

    private static boolean isHostname(String name) {
        if (name.indexOf(':') >= 0) {
            return false;
        }

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);

            if (c != '.' && ! Character.isDigit(c)) {
                return true;
            }
        }
        return false;
    }

    private static InetAddress[] getAddresses(Host host) {
        InetAddress[] addresses;

        try {
            addresses = InetAddress.getAllByName(host.name);
        }
        catch (UnknownHostException uhe) {
            throw new AerospikeException.Connection("Invalid host: " + host);
        }

        if (addresses.length == 0) {
            throw new AerospikeException.Connection("Failed to find addresses for " + host);
        }
        return addresses;
    }

    private void validateAddress(
        Cluster cluster, InetAddress address, String tlsName, int port, boolean detectLoadBalancer,
        int timeout
    ) throws Exception {
        ClusterDefinition def = cluster.getClusterDefinition();
        TlsBuilder tls = def.getTlsBuilder();
        InetSocketAddress socketAddress = new InetSocketAddress(address, port);
        long deadline;

        if (timeout > 0) {
            deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        }
        else {
            deadline = 0;
            timeout = def.getTendTimeout();
        }

        Connection conn = (tls != null) ?
            new Connection(tls, tlsName, socketAddress, timeout, cluster) :
            new Connection(socketAddress, timeout, cluster);

        try {
            conn.setDeadline(deadline);

            if (def.isAuthEnabled()) {
                // Login
                LoginCommand admin = new LoginCommand(cluster, null, conn);
                sessionToken = admin.sessionToken;
                sessionExpiration = admin.sessionExpiration;

                if (def.getTlsBuilder() != null && def.getTlsBuilder().isForLoginOnly()) {
                    // Switch to using non-TLS socket.
                    SwitchClear sc = new SwitchClear(cluster, conn, tlsName, sessionToken, deadline);
                    conn.close();
                    address = sc.clearAddress;
                    socketAddress = sc.clearSocketAddress;
                    conn = sc.clearConn;

                    // Disable load balancer detection since non-TLS address has already
                    // been retrieved via service info command.
                    detectLoadBalancer = false;
                }
            }

            List<String> commands = new ArrayList<String>(5);
            commands.add("node");
            commands.add("partition-generation");
            commands.add("build");
            commands.add("cluster-name");

            String addressCommand = null;

            if (detectLoadBalancer && address.isLoopbackAddress()) {
                // Disable load balancer detection for localhost.
                detectLoadBalancer = false;
            }

            if (detectLoadBalancer || def.isUseServicesAlternate()) {
                // Seed may be load balancer with changing address. Determine real address.
                // The alternate address list must also contain the node itself.
                addressCommand = (tls != null)?
                    def.isUseServicesAlternate() ? "service-tls-alt" : "service-tls-std" :
                    def.isUseServicesAlternate() ? "service-clear-alt" : "service-clear-std";

                commands.add(addressCommand);
            }

            // Issue commands.
            HashMap<String,String> map = Info.request(conn, commands);

            // Node returned results.
            this.primaryHost = new Host(address.getHostAddress(), tlsName, port);
            this.primaryAddress = socketAddress;
            this.primaryConn = conn;

            validateNode(map);
            validatePartitionGeneration(map);
            validateServerBuildVersion(map);

            boolean sendUserAgent = version.isGreaterOrEqual(Version.SERVER_VERSION_8_1);
            if (sendUserAgent) {
                Info.request(conn, "user-agent-set:value=" + getB64userAgent(def));
            }

            processClusterName(def, map);

            if (def.isUseServicesAlternate()) {
                validateAlternateAddress(map, addressCommand);
            }

            if (detectLoadBalancer) {
                setAddress(cluster, map, addressCommand, tlsName);
            }

            if (deadline != 0) {
                conn.setDeadline(0);
                conn.setTimeout(def.getTendTimeout());
            }
        }
        catch (Throwable e) {
            conn.close();
            throw e;
        }
    }

    private String getB64userAgent(ClusterDefinition def) {
        String appIdValue;

        if (def.getAppId() != null) {
            appIdValue = def.getAppId();
        }
        else {
            byte[] userBytes = def.getUserName();

            if (userBytes != null && userBytes.length > 0) {
                appIdValue = Buffer.utf8ToString(userBytes, 0, userBytes.length);
            }
            else {
                appIdValue = "not-set";
            }
        }

        String userAgent = "1,java-sdk-" + def.getClientVersion() + "," + appIdValue;

        return Crypto.encodeBase64(userAgent.getBytes());
    }

    private void validateNode(HashMap<String,String> map) {
        this.name = map.get("node");

        if (this.name == null) {
            throw new AerospikeException.InvalidNode("Node name is null");
        }
    }

    private void validatePartitionGeneration(HashMap<String,String> map) {
        String genString = map.get("partition-generation");
        int gen;

        try {
            gen = Integer.parseInt(genString);
        }
        catch (Throwable e) {
            throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost +
                " returned invalid partition-generation: " + genString);
        }

        if (gen == -1) {
            throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost +
                " is not yet fully initialized");
        }
    }

    private void validateServerBuildVersion(HashMap<String,String> map) {
        String build = map.get("build");
        version = Version.convertStringToVersion(build, name, primaryAddress);

        if (version.isLessThan(Version.MIN_SERVER_VERSION)) {
            throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost +
                " version " + version + " must be >= " + Version.MIN_SERVER_VERSION);
        }
    }

    private void processClusterName(ClusterDefinition def, HashMap<String,String> map) {
        String name = map.get("cluster-name");

        if (def.getClusterName() == null || def.getClusterName().isEmpty()) {
            // User did not provide clusterName, so use server clusterName.
            def.clusterName(name);
        }
        else {
            // Ensure clusterName is consistent across client and all server nodes.
            if (name == null || !def.getClusterName().equals(name)) {
                throw new AerospikeException.InvalidNode("Node " + this.name + ' ' + this.primaryHost +
                    " expected cluster name '" + def.getClusterName() + "' received '" + name + "'");
            }
        }
    }

    private void validateAlternateAddress(HashMap<String,String> map, String addressCommand) {
        String result = map.get(addressCommand);

        if (result == null || result.length() == 0) {
            throw new AerospikeException.AlternateAddressMissing(this.name, this.primaryHost);
        }
    }

    private void setAddress(
        Cluster cluster, HashMap<String,String> map, String addressCommand, String tlsName
    ) {
        String result = map.get(addressCommand);

        if (result == null || result.length() == 0) {
            // Server does not support service level call (service-clear-std, ...).
            // Load balancer detection is not possible.
            return;
        }

        ClusterDefinition def = cluster.getClusterDefinition();
        List<Host> hosts = Host.parseServiceHosts(result);
        Host h;

        // Search real hosts for seed.
        for (Host host : hosts) {
            try {
                h = cluster.getClusterTend().translateAddress(this.name, tlsName, host.name, host.port);
            }
            catch (AerospikeException.TranslationMiss tm) {
                continue;
            }

            if (h.equals(this.primaryHost)) {
                // Found seed which is not a load balancer.
                return;
            }
        }

        // Seed not found, so seed is probably a load balancer.
        // Find first valid real host.
        for (Host host : hosts) {
            try {
                h = cluster.getClusterTend().translateAddress(this.name, tlsName, host.name, host.port);

                InetAddress[] addresses = InetAddress.getAllByName(h.name);

                for (InetAddress address : addresses) {
                    try {
                        InetSocketAddress socketAddress = new InetSocketAddress(address, h.port);
                        Connection conn = (def.getTlsBuilder() != null) ?
                            new Connection(def.getTlsBuilder(), tlsName, socketAddress, def.getTendTimeout(), cluster) :
                            new Connection(socketAddress, def.getTendTimeout(), cluster);

                        try {
                            if (this.sessionToken != null) {
                                if (! AdminCommand.authenticate(cluster, null, conn, this.sessionToken)) {
                                    throw new AerospikeException("Authentication failed");
                                }
                            }

                            // Authenticated connection.  Set real host.
                            this.primaryHost = new Host(address.getHostAddress(), tlsName, h.port);
                            this.primaryAddress = socketAddress;
                            this.primaryConn.close();
                            this.primaryConn = conn;
                            return;
                        }
                        catch (Throwable e) {
                            conn.close();
                        }
                    }
                    catch (Throwable e) {
                        // Try next address.
                    }
                }
            }
            catch (Throwable e) {
                // Try next host.
            }
        }

        // Failed to find a valid address. IP Address is probably internal on the cloud
        // because the server access-address is not configured.  Log warning and continue
        // with original seed.
        if (log.isInfoEnabled()) {
            log.atInfo()
                .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                .log("Invalid address " + result + ". access-address is probably not configured on server.");
        }
    }

    private static int timeout(ClusterDefinition def, long deadline) {
        if (deadline == 0) {
            return def.getTendTimeout();
        }

        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());

        if (remaining <= 0) {
            throw new AerospikeException.Connection("Validation did not complete in time");
        }
        return (int)remaining;
    }

    private static final class SwitchClear {
        private InetAddress clearAddress;
        private InetSocketAddress clearSocketAddress;
        private Connection clearConn;

        // Switch from TLS connection to non-TLS connection.
        private SwitchClear(
            Cluster cluster, Connection conn, String tlsName, byte[] sessionToken, long deadline
        ) throws Exception {
            // Obtain non-TLS addresses.
            ClusterDefinition def = cluster.getClusterDefinition();
            String command = def.isUseServicesAlternate() ? "service-clear-alt" : "service-clear-std";
            HashMap<String,String> map = Info.request(conn, command, "node");
            String result = map.get(command);
            String nodeName = map.get("node");
            List<Host> hosts = Host.parseServiceHosts(result);
            Host clearHost;

            // Find first valid non-TLS host.
            for (Host host : hosts) {
                try {
                    clearHost = cluster.getClusterTend().translateAddress(nodeName, tlsName, host.name, host.port);

                    if (def.getIpMap() != null) {
                        String alternativeHost = def.getIpMap().get(clearHost.name);

                        if (alternativeHost != null) {
                            clearHost = new Host(alternativeHost, clearHost.port);
                        }
                    }

                    InetAddress[] addresses = InetAddress.getAllByName(clearHost.name);

                    for (InetAddress ia : addresses) {
                        try {
                            clearAddress = ia;
                            clearSocketAddress = new InetSocketAddress(clearAddress, clearHost.port);
                            clearConn = new Connection(clearSocketAddress, timeout(def, deadline), cluster);

                            try {
                                clearConn.setDeadline(deadline);

                                if (sessionToken != null) {
                                    clearConn.setDeadline(deadline);

                                    if (! AdminCommand.authenticate(cluster, null, clearConn, sessionToken)) {
                                        throw new AerospikeException("Authentication failed");
                                    }
                                }
                                return;  // Authenticated clear connection.
                            }
                            catch (Throwable e) {
                                clearConn.close();
                            }
                        }
                        catch (Throwable e) {
                            // Try next address.
                        }
                    }
                }
                catch (Throwable e) {
                    // Try next host.
                }
            }
            throw new AerospikeException("Invalid non-TLS address: " + result);
        }
    }
}
