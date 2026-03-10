package com.aerospike.benchmarks;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.RandomShift;

public class InsertTaskSync extends InsertTask implements Runnable {

    private final Session session;
    private final long startKey;
    private final long endKey;
    private String firstBin;
    private String[] restBins = new String[0];

    public InsertTaskSync(Session session, Arguments arguments, CounterStore counters, long start, long keyCount) {
        super(arguments, counters);
        this.startKey = start;
        this.endKey = startKey + keyCount;
        this.session = session;
        String[] binNamesArr = args.getBinNames(true);
        this.firstBin = binNamesArr[0];
        if (binNamesArr.length > 1) {
            this.restBins = new String[binNamesArr.length - 1];
            System.arraycopy(binNamesArr, 1, restBins, 0, restBins.length);
        }
    }

    @Override
    public void run() {
        RandomShift random = new RandomShift();
        for (long i = startKey; i < endKey; i++) {
            executeCommand(i, random);
        }
    }
    
    private void executeCommand(long keyIdentifier, RandomShift random) {
        Key key = new Key(args.getNamespace(), args.getSetName(), keyIdentifier);
        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, true, keyIdentifier);
        doUpsert(key, values);
    }

    private void doUpsert(Key key, Value[] values) {
        if (counters.write.latency != null) {
            long begin = System.nanoTime();
            session.upsert(key)
                    .bins(firstBin, restBins)
                    .values((Object) values)
                    .execute();
            long elapsed = System.nanoTime() - begin;
            counters.write.count.getAndIncrement();
            counters.write.latency.add(elapsed);
        } else {
            session.upsert(key)
                    .bins(firstBin, restBins)
                    .values((Object) values)
                    .execute();
            counters.write.count.getAndIncrement();
        }

    }
}
