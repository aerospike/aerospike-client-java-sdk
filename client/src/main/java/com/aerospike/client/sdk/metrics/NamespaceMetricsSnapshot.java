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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.sdk.Node;

/**
 * Namespace metrics snapshot.
 */
public final class NamespaceMetricsSnapshot {
    public final String namespace;
    public final long errors;
    public final long clientTimeouts;
    public final long serverTimeouts;
    public final long keyBusy;
    public final long recordTooBig;
    public final long deviceOverload;
    public final long bytesIn;
    public final long bytesOut;
    public final List<LatencySnapshot> latencies;

	public NamespaceMetricsSnapshot(Node node, String namespace) {
        this.namespace = namespace;
		this.errors = node.getErrorCount(namespace);
		this.clientTimeouts = node.getClientTimeoutCount(namespace);
        this.serverTimeouts = node.getServerTimeoutCount(namespace);
        this.keyBusy = node.getKeyBusyCount(namespace);
        this.recordTooBig = node.getRecordTooBig(namespace);
        this.deviceOverload = node.getDeviceOverload(namespace);
        this.bytesIn = node.getBytesIn(namespace);
        this.bytesOut = node.getBytesOut(namespace);

        NodeMetrics nodeMetrics = node.getMetrics();
        Histogram histogram = (nodeMetrics != null) ?
            nodeMetrics.getHistograms().getHistogram(namespace) : null;

        // Histogram is null when metrics are disabled or no command has completed
        // on this namespace yet.
        if (histogram != null) {
            this.latencies = new ArrayList<>(LatencyType.commandTypes.length);

            for (LatencyType type :  LatencyType.commandTypes) {
                LatencyBuckets buckets = histogram.getBuckets(type);

                if (buckets.getCount() > 0) {
                    LatencySnapshot ls = new LatencySnapshot(type, buckets);
                    this.latencies.add(ls);
                }
            }
        }
        else {
            this.latencies = new ArrayList<>(0);
        }
	}

    @Override
    public String toString() {
        return "NamespaceMetricsSnapshot{"
            + "namespace=" + namespace
            + ", errors=" + errors
            + ", clientTimeouts=" + clientTimeouts
            + ", serverTimeouts=" + serverTimeouts
            + ", keyBusy=" + keyBusy
            + ", recordTooBig=" + recordTooBig
            + ", deviceOverload=" + deviceOverload
            + ", bytesIn=" + bytesIn
            + ", bytesOut=" + bytesOut
            + ", latencies=" + latencies.toString()
            + '}';
    }
}
