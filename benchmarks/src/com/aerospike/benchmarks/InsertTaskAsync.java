package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.util.RandomShift;
import com.aerospike.client.fluent.util.Util;

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
