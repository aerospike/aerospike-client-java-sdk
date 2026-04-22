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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.util.RandomShift;
import com.aerospike.client.sdk.util.Util;

public class InsertTaskSync extends InsertTask implements Runnable {

    private final Session session;
    private final long startKey;
    private final long endKey;
    private final String[] binArr;
    private final boolean useLatency;

    public InsertTaskSync(Session session, Arguments arguments, CounterStore counters, long start, long keyCount) {
        super(arguments, counters);
        this.startKey = start;
        this.endKey = startKey + keyCount;
        this.session = session;
        this.binArr = args.getBinNames(true);
        this.useLatency = counters.write.latency != null;
    }

    @Override
    public void run() {
        try {
            RandomShift random = new RandomShift();
            for (long i = startKey; i < endKey; i++) {
                try {
                    executeCommand(i, random);
                } catch (AerospikeException ae) {
                    i--;
                    writeFailure(ae);
                } catch (Exception e) {
                    i--;
                    writeFailure(e);
                }
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

    private void executeCommand(long keyIdentifier, RandomShift random) {
        Key key = new Key(args.getNamespace(), args.getSetName(), keyIdentifier);
        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, true, keyIdentifier);
        doUpsert(key, values);
    }

    private void doUpsert(Key key, Value[] values) {
        var builder = session.upsert(key);
        for (int i = 0; i < binArr.length; i++) {
            args.setBinFromValue(builder, binArr[i], values[i]);
        }
        long begin = System.nanoTime();
        builder.execute().next();
        if (useLatency) {
            long elapsed = begin - System.nanoTime();
            counters.write.latency.add(elapsed);
        }
        counters.write.count.getAndIncrement();
    }
}
