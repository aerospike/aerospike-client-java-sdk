package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.util.RandomShift;
import com.aerospike.client.fluent.util.Util;

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
