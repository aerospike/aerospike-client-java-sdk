package com.aerospike.benchmarks;

import java.util.List;

import com.aerospike.client.sdk.*;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.util.RandomShift;
import com.aerospike.client.sdk.util.Util;

public class RWTaskSync extends RWTask implements Runnable {

    private final Session session;
    private final boolean useLatency;

    public RWTaskSync(Arguments args, CounterStore counters, Session session) {
        super(args, counters);
        this.session = session;
        this.useLatency = counters.read.latency != null;
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
    protected void getBinsAndIncrement(Key key, int incrementedBy) {
        Record rec = readRecordForUpdate(key);
        incrementCounter(key, rec, incrementedBy);
    }

    @Override
    protected void get(List<Key> keys, String binName) {
        RecordStream recs;
        long begin = System.nanoTime();
        recs = session.query(keys).bins(binName).execute();
        if (useLatency) {
            long elapsed = System.nanoTime() - begin;
            counters.read.latency.add(elapsed);
        }
        RecordStream failedRecs = recs.failures();
        if (failedRecs.stream().findAny().isPresent()) {
            readFailure(failedRecs.failures().next().exception());
        } else {
            // batch with partial failure are not accounted to successful reads
            counters.read.count.getAndIncrement();
        }
    }

    @Override
    protected void get(List<Key> keys) {
        RecordStream recs;
        long begin = System.nanoTime();
        recs = session.query(keys).execute();
        if (useLatency) {
            long elapsed = System.nanoTime() - begin;
            counters.read.latency.add(elapsed);
        }
        RecordStream failedRecs = recs.failures();
        if (failedRecs.stream().findAny().isPresent()) {
            readFailure(failedRecs.failures().next().exception());
        } else {
            // batch with partial failure are not accounted to successful reads
            counters.read.count.getAndIncrement();
        }
    }

    private Record readRecordForUpdate(Key key) {
        try {
            long begin = System.nanoTime();
            RecordResult rec = session.query(key).execute().next();
            if (useLatency) {
                counters.read.latency.add(System.nanoTime() - begin);
            }
            processRead(key, rec);
            return rec != null ? rec.recordOrNull() :  null;
        } catch (AerospikeException ae) {
            readFailure(ae);
            return null;
        } catch (Exception e) {
            readFailure(e);
            return null;
        }
    }

    private void incrementCounter(Key key, Record rec, int incrementedBy) {
        try {
            int generation = (rec != null) ? rec.generation : 0;
            long begin = System.nanoTime();
            var cmd = session.upsert(key)
                    .bin("test-counter").add(incrementedBy);
            if (generation > 0) {
                cmd.ensureGenerationIs(generation);
            }
            cmd.execute();
            if (useLatency) {
                counters.write.latency.add(System.nanoTime() - begin);
            }
            counters.write.count.getAndIncrement();
        } catch (AerospikeException ae) {
            writeFailure(ae);
        } catch (Exception e) {
            writeFailure(e);
        }
    }

    @Override
    public void run() {
        RandomShift random = new RandomShift();
        while (!shouldStop) {
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
