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

import com.aerospike.client.sdk.Cluster;

/**
 * Usage snapshot.
 */
public final class UsageMetricsSnapshot {
    public final long blocking;
    public final long deferred;
    public final long background;
    public final long transactions;

	public UsageMetricsSnapshot(Cluster cluster) {
		this.blocking = cluster.getBlockingCount();
		this.deferred = cluster.getDeferredCount();
		this.background = cluster.getBackgroundCount();
		this.transactions = cluster.getTranCount();
	}

    @Override
    public String toString() {
        return "UsageMetricsSnapshot{"
            + "blocking=" + blocking
            + ", deferred=" + deferred
            + ", background=" + background
            + '}';
    }
}
