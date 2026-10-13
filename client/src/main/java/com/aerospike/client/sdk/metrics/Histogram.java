/*
 * Copyright (c) 2012-2026 Aerospike, Inc.
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

import java.util.concurrent.TimeUnit;

/**
 * Latency histogram for a namespace, holding one {@link LatencyBuckets} per {@link LatencyType}.
 */
public class Histogram {
    private final LatencyBuckets[] buckets;

    public Histogram(TimeUnit latencyUnit, int latencyColumns, int latencyShift) {
        int maxType = LatencyType.getMax();

        buckets = new LatencyBuckets[maxType];

        for (int i = 0; i < maxType; i++) {
            buckets[i] = new LatencyBuckets(latencyUnit, latencyColumns, latencyShift);
        }
    }

    public void addLatency(LatencyType type, long elapsed) {
        buckets[type.ordinal()].add(elapsed);
    }

    /**
     * Return latency buckets for the given command type.
     */
    public LatencyBuckets getBuckets(LatencyType type) {
        return buckets[type.ordinal()];
    }
}
