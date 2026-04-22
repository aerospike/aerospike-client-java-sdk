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
package com.aerospike.benchmarks;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.aerospike.benchmarks.Constants.US;

public class LatencyManagerYcsb implements LatencyManager {

    private final AtomicInteger _buckets;
    private final AtomicLongArray histogram;
    private final AtomicLong histogramOverflow;
    private final AtomicInteger operations;
    private final AtomicLong totalLatency;
    private final AtomicInteger warmupCount;
    private final String name;

    //keep a windowed version of these stats for printing status
    private final AtomicInteger windowOperations;
    private final AtomicLong windowTotalLatency;

    private final AtomicLong min;
    private final AtomicLong max;


    public LatencyManagerYcsb(String name, int warmupCount) {
        this.name = name;
        this._buckets = new AtomicInteger(1000);
        this.histogram = new AtomicLongArray(_buckets.get());
        this.histogramOverflow = new AtomicLong(0);
        this.warmupCount = new AtomicInteger(warmupCount);
        this.operations = new AtomicInteger(0);
        this.totalLatency = new AtomicLong(0);
        windowOperations = new AtomicInteger(0);
        windowTotalLatency = new AtomicLong(0);
        min = new AtomicLong(-1);
        max = new AtomicLong(-1);
    }

    @Override
    public void add(long elapsed) {
        if (!isWarmUpCompleted()) {
            return;
        }
        // Latency is specified in ns
        long latencyUs = elapsed / 1000;
        long latencyMs = latencyUs / 1000;
        if (latencyMs >= _buckets.get()) {
            histogramOverflow.incrementAndGet();
        } else {
            histogram.incrementAndGet((int) latencyMs);
        }
        operations.incrementAndGet();
        totalLatency.addAndGet(latencyUs);
        windowOperations.incrementAndGet();
        windowTotalLatency.addAndGet(latencyUs);

        if (min.get() < 0 || latencyUs < min.get()) {
            min.set(latencyUs);
        }
        if ((max.get() < 0) || (latencyUs > max.get())) {
            max.set(latencyUs);
        }
    }

    @Override
    public void printHeader(PrintStream stream) {

    }

    @Override
    public void printResults(PrintStream exporter, String prefix) {
        if (!isWarmUpCompleted()) {
            int countRemaining = warmupCount.decrementAndGet();
            exporter.println("Warming up (" + countRemaining + " left)...");
            return;
        }
        StringBuilder buffer = new StringBuilder(1024);
        double avgLatency = (double) totalLatency.get() / (double)operations.get();
        double winAvgLatency = (double)windowTotalLatency.get() / (double)windowOperations.get();
        buffer.append(name).append(": Period[");
        buffer.append("Ops:").append(windowOperations.get());
        buffer.append(" Avg Latency:").append((long)winAvgLatency).append(US);

        buffer.append("] Total[Ops:").append(operations.get());
        buffer.append(" Latency:(avg:").append((long)avgLatency).append(US);
        buffer.append(" Min:").append(min.get()).append(US);
        buffer.append(" Max:").append(max.get()).append(US + ")");

        long opCounter = 0;
        boolean done95th = false;
        for (int i = 0; i < _buckets.get(); i++) {
            opCounter += histogram.get(i);
            double percentage = ((double) opCounter) / ((double) operations.get());
            if ((!done95th) && percentage >= 0.95) {
                buffer.append(" 95th% Latency:").append(i).append("ms");
                done95th = true;
            }
            if (percentage >= 0.99) {
                buffer.append(" 99th% Latency:").append(i).append("ms");
                break;
            }
        }
        buffer.append(']');
        exporter.println(buffer);
        windowOperations.set(0);
        windowTotalLatency.set(0);
    }

    @Override
    public void printSummaryHeader(PrintStream stream) {

    }

    @Override
    public void printSummary(PrintStream stream, String prefix) {

    }

    private boolean isWarmUpCompleted() {
        return this.warmupCount.get() <= 0;
    }
}
