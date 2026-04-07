package com.aerospike.benchmarks;


import com.aerospike.client.sdk.*;
import com.aerospike.client.sdk.util.RandomShift;
import com.aerospike.client.sdk.util.Util;

import java.util.ArrayList;
import java.util.List;

public class RWTaskSync extends RWTask implements Runnable {

    private final Session session;
    private final boolean useLatency;
    List<RecordResult> records;

    public RWTaskSync(Arguments args, CounterStore counters, Session session) {
        super(args, counters);
        this.session = session;
        this.useLatency = counters.read.latency != null;
        this.records = new ArrayList<>();
    }


    @Override
    protected void get(Key key, String binName) {
        long begin = System.nanoTime();
        RecordResult record = session.query(key).readingOnlyBins(binName).execute().next();
        if (useLatency) {
            long elapsed = System.nanoTime() - begin;
            counters.read.latency.add(elapsed);
        }
        processRead(key, record);
    }

    @Override
    protected void get(Key key) {
        long begin = System.nanoTime();
        RecordResult record = session.query(key).execute().next();
        if (useLatency) {
            long elapsed = System.nanoTime() - begin;
            counters.read.latency.add(elapsed);
        }
        processRead(key, record);
    }

    @Override
    protected void upsert(Key key, Value[] values, String... nameArr) {
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, nameArr[i], values[i]);
        }
        long begin = System.nanoTime();
        builder.execute();
        if (useLatency) {
            long elapsed = System.nanoTime() - begin;
            counters.write.latency.add(elapsed);
        }
        counters.write.count.getAndIncrement();
    }

    @Override
    protected void createOrReplace(Key key, Value[] values, String... nameArr) {
        var builder = session.replace(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, nameArr[i], values[i]);
        }
        long begin = System.nanoTime();
        builder.execute();
        if (useLatency) {
            long elapsed = System.nanoTime() - begin;
            counters.write.latency.add(elapsed);
        }
        counters.write.count.getAndIncrement();
    }

    @Override
    protected void doIncrement(Key key, int incrementedBy) {
        RecordResult result = null;
        long begin = System.nanoTime();

        RecordStream stream = session.insert(key)// insert fail if rec exist
                .bin("test-counter")
                .add(incrementedBy)
                .execute();

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
        try (var recordStream = session.query(keys).bins(binName).execute()) {
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
    protected void get(List<Key> keys) {
        long elasped = 0;
        long begin = System.nanoTime();
        try (var recordStream = session.query(keys).execute()) {
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
