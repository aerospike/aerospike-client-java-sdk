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
package com.aerospike.client.fluent.tend;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.fluent.AdminCommand;
import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.Connection;
import com.aerospike.client.fluent.Host;
import com.aerospike.client.fluent.Info;
import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.TlsBuilder;
import com.aerospike.client.fluent.AdminCommand.LoginCommand;
import com.aerospike.client.fluent.command.Buffer;
import com.aerospike.client.fluent.util.Crypto;
import com.aerospike.client.fluent.util.Util;
import com.aerospike.client.fluent.util.Version;

public final class NodeValidator {
	public Node fallback;
	public String name;
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
				validateAddress(cluster.getClusterDefinition(), address, host.tlsName, host.port, true);

				Node node = new Node(cluster, this);

				if (validatePeers(peers, node)) {
					return node;
				}
			}
			catch (Throwable e) {
				// Log exception and continue to next alias.
				if (Log.debugEnabled()) {
					Log.debug(cluster.getLogContext(), "Address " + address + ' ' + host.port + " failed: " +
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

	private boolean validatePeers(Peers peers, Node node) {
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
			if (Log.infoEnabled()) {
				Log.info(node.getLogContext(), "Skip orphan node: " + fallback);
			}
			fallback.close();
			fallback = null;
		}
		return true;
	}

	/**
	 * Verify that a host alias references a valid node.
	 */
	public void validateNode(ClusterDefinition def, Host host) throws Throwable {
		InetAddress[] addresses = getAddresses(host);
		Throwable exception = null;

		for (InetAddress address : addresses) {
			try {
				validateAddress(def, address, host.tlsName, host.port, false);
				return;
			}
			catch (Throwable e) {
				// Log exception and continue to next alias.
				if (Log.debugEnabled()) {
					Log.debug(def.getContext(), "Address " + address + ' ' + host.port + " failed: " +
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
		ClusterDefinition def, InetAddress address, String tlsName, int port, boolean detectLoadBalancer
	) throws Exception {
		TlsBuilder tls = def.getTlsBuilder();

		InetSocketAddress socketAddress = new InetSocketAddress(address, port);
		Connection conn = (tls != null) ?
			new Connection(tls, tlsName, socketAddress, def.getTendTimeout()) :
			new Connection(socketAddress, def.getTendTimeout());

		try {
			if (def.isAuthEnabled()) {
				// Login
				LoginCommand admin = new LoginCommand(def, conn);
				sessionToken = admin.sessionToken;
				sessionExpiration = admin.sessionExpiration;

				if (def.getTlsBuilder() != null && def.getTlsBuilder().isForLoginOnly()) {
					// Switch to using non-TLS socket.
					SwitchClear sc = new SwitchClear(def, conn, sessionToken);
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

			if (detectLoadBalancer) {
				if (address.isLoopbackAddress()) {
					// Disable load balancer detection for localhost.
					detectLoadBalancer = false;
				}
				else {
					// Seed may be load balancer with changing address. Determine real address.
					addressCommand = (def.getTlsBuilder() != null)?
						def.isUseServicesAlternate() ? "service-tls-alt" : "service-tls-std" :
						def.isUseServicesAlternate() ? "service-clear-alt" : "service-clear-std";

					commands.add(addressCommand);
				}
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

			if (addressCommand != null) {
				setAddress(def, map, addressCommand, tlsName);
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

		String userAgent = "1,java-" + def.getClientVersion() + "," + appIdValue;

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

	private void setAddress(
		ClusterDefinition def, HashMap<String,String> map, String addressCommand, String tlsName
	) {
		String result = map.get(addressCommand);

		if (result == null || result.length() == 0) {
			// Server does not support service level call (service-clear-std, ...).
			// Load balancer detection is not possible.
			return;
		}

		List<Host> hosts = Host.parseServiceHosts(result);
		Host h;

		// Search real hosts for seed.
		for (Host host : hosts) {
			h = host;

			if (def.getIpMap() != null) {
				String alt = def.getIpMap().get(h.name);

				if (alt != null) {
					h = new Host(alt, h.port);
				}
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
				h = host;

				if (def.getIpMap() != null) {
					String alt = def.getIpMap().get(h.name);

					if (alt != null) {
						h = new Host(alt, h.port);
					}
				}

				InetAddress[] addresses = InetAddress.getAllByName(h.name);

				for (InetAddress address : addresses) {
					try {
						InetSocketAddress socketAddress = new InetSocketAddress(address, h.port);
						Connection conn = (def.getTlsBuilder() != null) ?
							new Connection(def.getTlsBuilder(), tlsName, socketAddress, def.getTendTimeout()) :
							new Connection(socketAddress, def.getTendTimeout());

						try {
							if (this.sessionToken != null) {
								if (! AdminCommand.authenticate(def, conn, this.sessionToken)) {
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
		if (Log.infoEnabled()) {
			Log.info(def.getContext(), "Invalid address " + result + ". access-address is probably not configured on server.");
		}
	}

	private static final class SwitchClear {
		private InetAddress clearAddress;
		private InetSocketAddress clearSocketAddress;
		private Connection clearConn;

		// Switch from TLS connection to non-TLS connection.
		private SwitchClear(ClusterDefinition def, Connection conn, byte[] sessionToken) throws Exception {
			// Obtain non-TLS addresses.
			String command = def.isUseServicesAlternate() ? "service-clear-alt" : "service-clear-std";
			String result = Info.request(conn, command);
			List<Host> hosts = Host.parseServiceHosts(result);
			Host clearHost;

			// Find first valid non-TLS host.
			for (Host host : hosts) {
				try {
					clearHost = host;

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
							clearConn = new Connection(clearSocketAddress, def.getTendTimeout());

							try {
								if (sessionToken != null) {
									if (! AdminCommand.authenticate(def, clearConn, sessionToken)) {
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
