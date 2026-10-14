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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Latency buckets for a command group (See {@link com.aerospike.client.metrics.LatencyType}).
 * Latency bucket counts and min/max/sum/count are cumulative and not reset on each
 * metrics snapshot interval.
 *
 * <p>{@link #add} is on the command hot path and is called concurrently by every command
 * thread, so each statistic is updated lock-free and independently:</p>
 * <ul>
 *   <li>Bucket counts and {@code count} use {@link LongAdder}, and {@code sum} uses
 *       {@link DoubleAdder}. Both stripe updates across cells and avoid CAS contention on a
 *       single shared counter. {@code sum} is a double so cumulative nanoseconds cannot
 *       overflow; precision loss is negligible relative to the total.</li>
 *   <li>{@code min} and {@code max} use {@link AtomicLong} with a CAS loop that exits
 *       without writing when the new value does not improve the current extreme. Once
 *       latencies settle, nearly every call is a single volatile read.</li>
 * </ul>
 *
 * <p>Each value is individually exact, but a reader is not guaranteed a mutually
 * consistent snapshot of all values (e.g. {@code count} may include a sample whose
 * {@code sum} contribution is not yet visible). That skew is bounded by in-flight
 * commands and is acceptable for cumulative metrics.</p>
 */
public final class LatencyBuckets {
	private final LongAdder[] buckets;
	private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
	private final DoubleAdder sum = new DoubleAdder();
	private final LongAdder count = new LongAdder();
	private final int latencyShift;
    private final TimeUnit latencyUnit;

	/**
	 * Initialize latency buckets.
	 *
	 * @param latencyColumns	number of latency buckets
	 * @param latencyShift		power of 2 multiple between each range bucket in latency histograms starting at bucket 3.
	 * 							The first 2 buckets are "&lt;=1ms" and "&gt;1ms".
	 */
	public LatencyBuckets(TimeUnit latencyUnit, int latencyColumns, int latencyShift) {
	    this.latencyUnit = latencyUnit;
		this.latencyShift = latencyShift;
		buckets = new LongAdder[latencyColumns];

		for (int i = 0; i < buckets.length; i++) {
			buckets[i] = new LongAdder();
		}
	}

	/**
	 * Return number of buckets.
	 */
	public int getColumns() {
		return buckets.length;
	}

	/**
	 * Return cumulative count of a bucket.
	 */
	public long getBucket(int i) {
		return buckets[i].longValue();
	}

	/**
	 * Increment count of bucket corresponding to the elapsed time in nanoseconds.
	 */
	public void add(long elapsedNanos) {
	    // Convert elapsed nanoseconds to latencyUnit.
        long elapsed = latencyUnit.convert(elapsedNanos, TimeUnit.NANOSECONDS);
        int index = getIndex(elapsed);
		buckets[index].increment();

		sum.add(elapsed);
		count.increment();

		// Only CAS when the value is a new extreme. AtomicLong.accumulateAndGet() is not
		// used because it always performs a CAS write, even when the value is unchanged.
		long cur = min.get();

		while (elapsed < cur) {
			if (min.compareAndSet(cur, elapsed)) {
				break;
			}
			cur = min.get();
		}

		cur = max.get();

		while (elapsed > cur) {
			if (max.compareAndSet(cur, elapsed)) {
				break;
			}
			cur = max.get();
		}
	}

	/**
	 * Return smallest latency in nanoseconds, or 0 if no latencies have been recorded.
	 */
	public long getMin() {
		long v = min.get();
		return v == Long.MAX_VALUE ? 0 : v;
	}

	/**
	 * Return largest latency in nanoseconds, or 0 if no latencies have been recorded.
	 */
	public long getMax() {
		long v = max.get();
		return v == Long.MIN_VALUE ? 0 : v;
	}

	/**
	 * Return sum of all latencies in nanoseconds.
	 */
	public double getSum() {
		return sum.sum();
	}

	/**
	 * Return number of latencies recorded.
	 */
	public long getCount() {
		return count.sum();
	}

	private int getIndex(long elapsed) {
		int lastBucket = buckets.length - 1;
		long limit = 1;

		for (int i = 0; i < lastBucket; i++) {
			if (elapsed <= limit) {
				return i;
			}
			limit <<= latencyShift;
		}
		return lastBucket;
	}
}
