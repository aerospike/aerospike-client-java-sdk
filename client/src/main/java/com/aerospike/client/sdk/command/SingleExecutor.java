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
package com.aerospike.client.sdk.command;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.metrics.LatencyType;

public abstract class SingleExecutor extends SyncExecutor {

    public SingleExecutor(Cluster cluster, Command cmd) {
        super(cluster, cmd);
    }

    @Override
    public final void execute() {
        if (cluster.isMetricsOperationalEnabled()) {
            LatencyType latencyType = getLatencyType();
            long begin = System.nanoTime();

            super.execute();

            long elapsed = System.nanoTime() - begin;
            cluster.addLatency(latencyType, elapsed);
        }
        else {
            super.execute();
        }
    }
}
