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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;

/**
 * Addresses whose connects hang, for tend timing tests.
 */
final class HungAddress {
	/**
	 * Address whose connect to the port never completes: the loopback address with a listener whose
	 * accept backlog is full, where the kernel drops further SYNs (Linux), or else the given
	 * TEST-NET-1 address (RFC 5737), which is routed nowhere (macOS resets connects to a full backlog,
	 * and binds only 127.0.0.1 of the loopback range). The listener and the sockets that fill its
	 * backlog are added to {@code closeables}. The test is skipped when neither hangs.
	 */
	static InetAddress hung(List<AutoCloseable> closeables, InetAddress loopback, String testNet, int port)
		throws IOException {

		ServerSocket listener = null;

		try {
			listener = new ServerSocket(port, 1, loopback);
			closeables.add(listener);

			for (int i = 0; i < 64; i++) {
				Socket socket = new Socket();
				closeables.add(socket);
				socket.connect(new InetSocketAddress(loopback, port), 200);
			}
		}
		catch (SocketTimeoutException ste) {
			return loopback;
		}
		catch (IOException ioe) {
		}

		if (listener != null) {
			listener.close();
		}

		InetAddress address = InetAddress.getByName(testNet);

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(address, port), 200);
		}
		catch (SocketTimeoutException ste) {
			return address;
		}
		catch (IOException ioe) {
		}
		assumeTrue(false, "No connect hangs: the accept backlog of " + loopback + ' ' + port +
			" did not fill and " + testNet + " is not routed nowhere");
		return null;
	}

	private HungAddress() {
	}
}
