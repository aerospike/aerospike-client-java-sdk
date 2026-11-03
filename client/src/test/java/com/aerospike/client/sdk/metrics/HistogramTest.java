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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.SystemSettings;

/**
 * The state the metrics snapshots see when metrics are enabled but no latency has been recorded.
 * {@link NamespaceMetricsSnapshot} and {@link MetricsSnapshot} both read a histogram and then call
 * {@link Histogram#getBuckets(LatencyType)} for every command type.
 */
public class HistogramTest {
    private static final MetricsOperational settings =
        SystemSettings.DEFAULT.getMetrics().getExtended().getOperational();

    @Test
    public void namespaceWithoutLatencyHasNoHistogram() {
        Histograms histograms = new Histograms(settings);

        // Metrics are enabled, but nothing has run on this namespace yet. This null is what
        // NamespaceMetricsSnapshot checks before reading buckets.
        assertNull(histograms.getHistogram("test"));
    }

    @Test
    public void newHistogramHasEmptyBucketsForEveryCommandType() {
        Histogram histogram = new Histogram(settings);

        // MetricsSnapshot reads the cluster histogram before any command completes.
        for (LatencyType type : LatencyType.commandTypes) {
            LatencyBuckets buckets = histogram.getBuckets(type);

            assertNotNull(buckets, type.getLabel());
            assertEquals(0, buckets.getCount(), type.getLabel());
        }
    }

    @Test
    public void latencyOnOneTypeLeavesOtherTypesEmpty() {
        Histograms histograms = new Histograms(settings);
        histograms.addLatency("test", LatencyType.READ, 1_000_000);

        Histogram histogram = histograms.getHistogram("test");
        assertNotNull(histogram);

        for (LatencyType type : LatencyType.commandTypes) {
            LatencyBuckets buckets = histogram.getBuckets(type);

            assertNotNull(buckets, type.getLabel());
            assertEquals(type == LatencyType.READ ? 1 : 0, buckets.getCount(), type.getLabel());
        }
    }
}
