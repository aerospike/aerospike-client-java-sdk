package com.aerospike.benchmarks;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.RandomShift;
import com.aerospike.client.fluent.util.Util;

public class RWTaskSync extends RWTask implements Runnable {

    private final Session session;

    public RWTaskSync(Arguments args, CounterStore counters, Session session) {
        super(args, counters);
        this.session = session;
    }


    @Override
    protected void get(Key key, String binName) {
        RecordStream record;
        if (counters.read.latency != null) {
            long begin = System.nanoTime();
            record = session.query(key).readingOnlyBins(binName).execute();
            long elapsed = System.nanoTime() - begin;
            counters.read.latency.add(elapsed);
        } else {
            record = session.query(key).readingOnlyBins(binName).execute();
        }
        processRead(key, record);
    }

    @Override
    protected void get(Key key) {
        RecordStream record;
        if (counters.read.latency != null) {
            long begin = System.nanoTime();
            record = session.query(key).execute();
            long elapsed = System.nanoTime() - begin;
            counters.read.latency.add(elapsed);
        } else {
            record = session.query(key).execute();
        }
        processRead(key, record);
    }

    @Override
    protected void upsert(Key key, Value[] values, String... nameArr) {
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, nameArr[i], values[i]);
        }
        if (counters.write.latency != null) {
            long begin = System.nanoTime();
            builder.execute();
            long elapsed = System.nanoTime() - begin;
            counters.write.count.getAndIncrement();
            counters.write.latency.add(elapsed);
        }
        else {
            builder.execute();
            counters.write.count.getAndIncrement();
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
