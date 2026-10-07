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
package com.aerospike.client.sdk.metrics;

import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.command.Pool;

/**
 * Connection metrics snapshot.
 */
public final class ConnectionMetricsSnapshot {
    /**
     * Active connections from connection pool(s) currently executing commands.
     */
    public final int inUse;

    /**
     * Initialized connections in connection pool(s) that are not currently active.
     */
    public final int inPool;

    /**
     * Total number of node connections opened since node creation.
     */
    public final int opened;

    /**
     * Total number of node connections closed since node creation.
     */
    public final int closed;

	/**
	 * Connection metrics constructor.
	 */
	public ConnectionMetricsSnapshot(Node node) {
        int inUse = 0;
        int inPool = 0;

        for (Pool pool : node.getConnectionPools()) {
            int tmp = pool.size();
            inPool += tmp;
            tmp = pool.getTotal() - tmp;

            // Timing issues may cause values to go negative. Adjust.
            if (tmp < 0) {
                tmp = 0;
            }
            inUse += tmp;
        }

        this.inUse = inUse;
		this.inPool = inPool;
		this.opened = node.getConnectionsOpened();
		this.closed = node.getConnectionsClosed();
	}

    @Override
    public String toString() {
        return "ConnectionMetricsSnapshot{"
            + "inUse=" + inUse
            + ", inPool=" + inPool
            + ", opened=" + opened
            + ", closed=" + closed
            + '}';
    }
}
