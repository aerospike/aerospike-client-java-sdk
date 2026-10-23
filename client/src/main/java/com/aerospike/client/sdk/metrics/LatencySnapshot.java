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

/**
 * Latency snapshot.
 */
public final class LatencySnapshot {
    /**
     * Latency command type.
     */
    public final LatencyType type;

    /**
     * Smallest latency value. Cumulative.
     */
    public final long min;

    /**
     * Largest latency value. Cumulative.
     */
    public final long max;

    /**
     * Sum of all latency values. Cumulative.
     */
    public final double sum;

    /**
     * Total number of entries in latency buckets. Cumulative.
     */
    public final long count;

    /**
     * Latency bucket count. Cumulative.
     */
    public final long[] bucketCounts;

	public LatencySnapshot(LatencyType type, LatencyBuckets buckets) {
		this.type = type;
		this.min = buckets.getMin();
		this.max = buckets.getMax();
        this.sum = buckets.getSum();
        this.count = buckets.getCount();

        int columns = buckets.getColumns();

        this.bucketCounts = new long[columns];

        for (int i = 0; i < columns; i++) {
            this.bucketCounts[i] = buckets.getBucket(i);
        }
	}

	public static LatencySnapshot connLatencySnapshot(LatencyBuckets buckets) {
	    return new LatencySnapshot(buckets);
	}

    private LatencySnapshot(LatencyBuckets buckets) {
        this.type = LatencyType.CONN;
        this.min = buckets.getMin();
        this.max = buckets.getMax();
        this.sum = buckets.getSum();
        this.count = buckets.getCount();

        int columns = buckets.getColumns();

        this.bucketCounts = new long[columns];

        for (int i = 0; i < columns; i++) {
            this.bucketCounts[i] = buckets.getBucket(i);
        }
    }

    @Override
    public String toString() {
        return "LatencySnapshot{"
            + "type=" + type
            + ", min=" + min
            + ", max=" + max
            + ", sum=" + sum
            + ", count=" + count
            + ", bucketCounts=" + Arrays.toString(bucketCounts)
            + '}';
    }
}
