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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Histograms {
    private final static String noNSLabel = "";

    private final ConcurrentHashMap<String, Histogram> histoMap = new ConcurrentHashMap<>();
    private final TimeUnit latencyUnit;
    private final int latencyColumns;
	private final int latencyShift;

	/**
	 * A Histograms object is a container for a map of namespaces to histograms (as defined by their associated
	 * LatencyBuckets) & their histogram properties
	 */
	public Histograms(MetricsOperational settings) {
        this.latencyUnit = settings.getLatencyUnit();
        this.latencyColumns = settings.getLatencyColumns();
		this.latencyShift = settings.getLatencyShift();
	}

	/**
	 * Increment count of bucket corresponding to the namespace & elapsed time in nanoseconds.
	 */
	public void addLatency(String namespace, LatencyType type, long elapsed) {
		if (namespace == null) {
			namespace = noNSLabel;
		}

		// Plain get() first: it is lock-free and covers nearly every call. On a miss,
		// computeIfAbsent() guarantees that concurrent first callers for the same
		// namespace share one histogram, so no latencies are lost.
		Histogram hist = histoMap.get(namespace);

		if (hist == null) {
			hist = histoMap.computeIfAbsent(namespace,
				ns -> new Histogram(latencyUnit, latencyColumns, latencyShift));
		}
		hist.addLatency(type, elapsed);
	}

	/**
	 * Return the histogram for the given namespace, or null if no latency has been
	 * recorded for that namespace yet.
	 */
	public Histogram getHistogram(String namespace) {
		return histoMap.get(namespace);
	}
}
