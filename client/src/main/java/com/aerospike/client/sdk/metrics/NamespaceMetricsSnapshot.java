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

import java.util.Arrays;

import com.aerospike.client.sdk.Node;

/**
 * Namespace metrics snapshot.
 */
public final class NamespaceMetricsSnapshot {
    public final String namespace;
    public final long errors;
    public final long clientTimeouts;
    // TODO Support new error counters.
    //public final long serverTimeouts;
    public final long keyBusy;
    //public final long recordTooBig;
    //public final long deviceOverload;
    public final long bytesIn;
    public final long bytesOut;
    public final LatencySnapshot[] latencies;

	public NamespaceMetricsSnapshot(Node node, String namespace) {

        this.namespace = namespace;
		this.errors = node.getErrorCount(namespace);
		this.clientTimeouts = node.getTimeoutCount(namespace);
        //this.serverTimeouts = serverTimeouts;
        this.keyBusy = node.getKeyBusyCount(namespace);
        //this.recordTooBig = recordTooBig;
        //this.deviceOverload = deviceOverload;
        this.bytesIn = node.getBytesIn(namespace);
        this.bytesOut = node.getBytesOut(namespace);

        NodeMetrics nodeMetrics = node.getMetrics();

        if (nodeMetrics != null) {
            LatencyBuckets[] latencyBuckets = nodeMetrics.getHistograms().getBuckets(namespace);
            LatencyType[] types = LatencyType.values();
            int max = LatencyType.getMax();

            this.latencies = new LatencySnapshot[max];

            for (int i = 0; i < max; i++) {
                this.latencies[i] = new LatencySnapshot(types[i], latencyBuckets[i]);
            }
        }
        else {
            this.latencies = new LatencySnapshot[0];
        }
	}

    @Override
    public String toString() {
        return "NamespaceMetricsSnapshot{"
            + "namespace=" + namespace
            + ", errors=" + errors
            + ", clientTimeouts=" + clientTimeouts
            //+ ", serverTimeouts=" + serverTimeouts
            + ", keyBusy=" + keyBusy
            //+ ", recordTooBig=" + recordTooBig
            //+ ", deviceOverload=" + deviceOverload
            + ", bytesIn=" + bytesIn
            + ", bytesOut=" + bytesOut
            + ", latencies=" + Arrays.toString(latencies)
            + '}';
    }
}
