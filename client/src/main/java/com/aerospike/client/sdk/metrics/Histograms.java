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

import com.aerospike.client.sdk.MetricsSettings;

public class Histograms {
	private final ConcurrentHashMap<String, LatencyBuckets[]> histoMap = new ConcurrentHashMap<>();
    private final TimeUnit latencyUnit;
    private final int latencyColumns;
	private final int latencyShift;
	private final static String noNSLabel = "";
	private final int max;

	/**
	 * A Histograms object is a container for a map of namespaces to histograms (as defined by their associated
	 * LatencyBuckets) & their histogram properties
	 */
	public Histograms(MetricsSettings settings) {
        this.latencyUnit = settings.getLatencyUnit();
        this.latencyColumns = settings.getLatencyColumns();
		this.latencyShift = settings.getLatencyShift();
		max = LatencyType.getMax();
	}

	private LatencyBuckets[] createBuckets() {
		LatencyBuckets[] buckets = new LatencyBuckets[max];

		for (int i = 0; i < max; i++) {
			buckets[i] = new LatencyBuckets(latencyUnit, latencyColumns, latencyShift);
		}
		return buckets;
	}

	/**
	 * Increment count of bucket corresponding to the namespace & elapsed time in nanoseconds.
	 */
	public void addLatency(String namespace, LatencyType type, long elapsed) {
		if (namespace == null) {
			namespace = noNSLabel;
		}
		LatencyBuckets[] buckets = getBuckets(namespace);
		if (buckets == null) {
			buckets = createBuckets();
			LatencyBuckets[] finalBuckets = buckets;
			histoMap.computeIfAbsent(namespace, k -> finalBuckets);
		}
		buckets[type.ordinal()].add(elapsed);
	}

	/**
	 * Return the LatencyBuckets for a given namespace
	 */
	public LatencyBuckets[] getBuckets(String namespace) {
		return histoMap.get(namespace);
	}

	/**
	 * Return the histograms map
	 */
	public ConcurrentHashMap<String, LatencyBuckets[]> getMap() {
		return histoMap;
	}
}
