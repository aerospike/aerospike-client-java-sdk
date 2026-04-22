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

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.util.RandomShift;
import com.aerospike.client.sdk.util.Util;

public class InsertTaskAsync extends InsertTask implements Runnable {

    private final long startKey;
    private final long endKey;
    private final String[] binArr;
    private final boolean useLatency;
    private final Session session;
    private long currentIndex;

    public InsertTaskAsync(Session session, Arguments args, CounterStore counters, long start, long keyCount) {
        super(args, counters);
        this.startKey = start;
        this.endKey = startKey + keyCount;
        this.binArr = args.getBinNames(true);
        this.useLatency = counters.write.latency != null;
        this.session = session;
    }

    private void doUpsert(Key key, Value[] values) {
        long begin = useLatency ? System.nanoTime() : 0;
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, binArr[i], values[i]);
        }
        try (var stream = builder.executeAsync((k, index, ae) -> {
            currentIndex--;
            writeFailure(ae);
        })) {
            RecordResult rec = null;
            while (stream.hasNext()) {
                rec = stream.next();
            }
            if (rec != null) {
                if (useLatency) {
                    counters.write.latency.add(System.nanoTime() - begin);
                }
                counters.write.count.getAndIncrement();
            }
        }
    }

    private void executeCommand(long keyIdx, RandomShift random) {
        Key key = new Key(args.getNamespace(), args.getSetName(), keyIdx);
        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, true, keyIdx);
        doUpsert(key, values);
    }


    @Override
    public void run() {
        try {
            RandomShift random = new RandomShift();
            currentIndex = startKey;
            while (currentIndex < endKey) {
                try {
                    executeCommand(currentIndex, random);
                } catch (Exception e) {
                    currentIndex--;
                    writeFailure(e);
                }
                currentIndex++;

                // Throttle throughput
                if (args.getThroughput() > 0) {
                    int transactions = counters.write.count.get();

                    if (transactions > args.getThroughput()) {
                        long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();

                        if (millis > 0) {
                            Util.sleep(millis);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println("Insert task error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
