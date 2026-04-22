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

import com.aerospike.client.sdk.*;
import com.aerospike.client.sdk.util.RandomShift;
import com.aerospike.client.sdk.util.Util;

import java.util.ArrayList;
import java.util.List;

public class RWTaskAsync extends RWTask implements Runnable {

    private final Session session;
    private final boolean useLatency;
    List<RecordResult> records;

    public RWTaskAsync(Arguments args, CounterStore counters, Session session) {
        super(args, counters);
        this.session = session;
        this.useLatency = counters.read.latency != null;
        this.records = new ArrayList<>();
    }

    @Override
    protected void get(Key key, String binName) {
        RecordResult rec = null;
        long begin = useLatency ? System.nanoTime() : 0;
        try (var stream = session.query(key)
                .readingOnlyBins(binName)
                .executeAsync(ErrorStrategy.IN_STREAM)) {
            while (stream.hasNext()) {
                rec = stream.next();
            }
            if (rec == null || rec.exception() == null) {
                if (useLatency) {
                    counters.read.latency.add(System.nanoTime() - begin);
                }
                processRead(key, rec);
            } else {
                readFailure(rec.exception());
            }
        } catch (Exception e) {
            readFailure(e);
        }
    }

    @Override
    protected void get(Key key) {
        RecordResult rec = null;
        long begin = useLatency ? System.nanoTime() : 0;
        try (var stream = session.query(key).executeAsync(ErrorStrategy.IN_STREAM)) {
            while (stream.hasNext()) {
                rec = stream.next();
            }
            if (rec == null || rec.exception() == null) {
                if (useLatency) {
                    counters.read.latency.add(System.nanoTime() - begin);
                }
                processRead(key, rec);
            } else {
                readFailure(rec.exception());
            }
        } catch (Exception t) {
            readFailure(t);
        }
    }

    @Override
    protected void upsert(Key key, Value[] values, String... bins) {
        RecordResult rec = null;
        long begin = counters.write.latency != null ? System.nanoTime() : 0;
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, bins[i], values[i]);
        }
        try (var stream = builder.executeAsync((k, index, ae) -> writeFailure(ae))) {
            while (stream.hasNext()) {
                rec = stream.next();
            }
            if (rec != null) {
                if (useLatency) {
                    counters.write.latency.add(System.nanoTime() - begin);
                }
                counters.write.count.getAndIncrement();
            }
        } catch (Exception e) {
            writeFailure(e);
        }
    }

    @Override
    protected void createOrReplace(Key key, Value[] values, String... bins) {
        RecordResult rec = null;
        long begin = counters.write.latency != null ? System.nanoTime() : 0;
        var builder = session.replace(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, bins[i], values[i]);
        }
        try (var stream = builder.executeAsync((k, index, ae) -> writeFailure(ae))) {
            while (stream.hasNext()) {
                rec = stream.next();
            }
            if (rec != null) {
                if (useLatency) {
                    counters.write.latency.add(System.nanoTime() - begin);
                }
                counters.write.count.getAndIncrement();
            }
        } catch (Exception e) {
            writeFailure(e);
        }
    }

    @Override
    protected void doIncrement(Key key, int incrementedBy) {
        RecordResult result = null;
        long begin = System.nanoTime();
        RecordStream stream = session.insert(key)
                .bin("test-counter")
                .add(incrementedBy)
                .executeAsync((k, index, exp) -> writeFailure(exp));

        while (stream.hasNext()) {
            result = stream.next();
        }
        if (result != null) {
            if (useLatency) {
                counters.write.latency.add(System.nanoTime() - begin);
            }
            counters.write.count.getAndIncrement();
        }
    }

    @Override
    protected void get(List<Key> keys, String binName) {
        long elasped = 0;
        long begin = System.nanoTime();
        try (var recordStream = session.query(keys).readingOnlyBins(binName).executeAsync(ErrorStrategy.IN_STREAM)) {
            while (recordStream.hasNext()) {
                records.add(recordStream.next());
            }
            elasped = System.nanoTime() - begin;

            RecordResult errRecord = records.stream().filter(record -> record.exception() != null)
                    .findAny()
                    .orElse(null);

            if (errRecord != null) {
                // batch with partial failure are not accounted to successful reads
                readFailure(errRecord.exception());
            } else {
                if (useLatency) {
                    counters.read.latency.add(elasped);
                }
                counters.read.count.getAndIncrement();
            }
        } catch (Exception e) {
            readFailure(e);
        } finally {
            records.clear();
        }
    }

    @Override
    protected void get(List<Key> keys) {
        long elasped = 0;
        long begin = System.nanoTime();
        try (var recordStream = session.query(keys).executeAsync(ErrorStrategy.IN_STREAM)) {
            while (recordStream.hasNext()) {
                records.add(recordStream.next());
            }
            elasped = System.nanoTime() - begin;

            RecordResult errRecord = records.stream()
                    .filter(record -> record.exception() != null)
                    .findAny()
                    .orElse(null);

            if (errRecord != null) {
                // batch with partial failure are not accounted to successful reads
                readFailure(errRecord.exception());
            } else {
                if (useLatency) {
                    counters.read.latency.add(elasped);
                }
                counters.read.count.getAndIncrement();
            }
        } catch (Exception e) {
            readFailure(e);
        } finally {
            records.clear();
        }
    }

    @Override
    public void run() {
        RandomShift random = new RandomShift();
        while (!isStopped) {
            runCommand(random);
            // Throttle throughput
            if (args.getThroughput() > 0) {
                int transactions;
                if (counters.transaction.latency != null) {
                    // Measure the transactions as per one "business" transaction
                    transactions = counters.transaction.count.get();
                } else {
                    transactions = counters.write.count.get() + counters.read.count.get();
                }

                if (transactions > args.getThroughput()) {
                    long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();
                    if (millis > 0) {
                        Util.sleep(millis);
                    }
                }
            }
        }
    }
}
