package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.util.RandomShift;

import java.util.List;
import java.util.concurrent.Semaphore;

public class InsertTaskAsync extends InsertTask {

    private final long startKey;
    private final long endKey;
    private final String[] binArr;
    private final boolean useLatency;
    private final Session session;
    private final RandomShift random;
    private long currentKey;

    public InsertTaskAsync(Session session, Arguments args, CounterStore counters, long start, long keyCount) {
        super(args, counters);
        this.startKey = start;
        this.endKey = startKey + keyCount;
        this.binArr = args.getBinNames(true);
        this.useLatency = counters.write.latency != null;
        this.session = session;
        this.random = new RandomShift();
    }

    public void runCommand() {
        long keyIdx = startKey + this.currentKey;
        Key key = new Key(args.getNamespace(), args.getSetName(), keyIdx);
        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, true, keyIdx);
        doUpsert(key, values);
    }

    private void doUpsert(Key key, Value[] values) {
        long begin = useLatency ? System.nanoTime() : 0;
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, binArr[i], values[i]);
        }
        var handle = builder.executeAsync(ErrorStrategy.IN_STREAM);
        handle.asCompletableFuture()
                .thenAccept(results -> {
                    if (useLatency) {
                        counters.write.latency.add(System.nanoTime() - begin);
                    }
                    processSingleKeyWriteRecord(results);
                    if (++currentKey < endKey) {
                        // Try next command.
                        runCommand();
                    } else {
                        System.out.println("Stopped ..Insert");
                    }

                }).whenComplete((r, ex) -> {
                    if (ex != null) {
                        if (ex instanceof AerospikeException) {
                            writeFailure((AerospikeException) ex);
                        } else if (ex instanceof Exception) {
                            writeFailure((Exception) ex);
                        }
                        currentKey--;
                        runCommand();
                    }
                });
    }

    private void processSingleKeyWriteRecord(List<RecordResult> results) {
        if (results.isEmpty()) {
            return;
        }
        RecordResult first = results.getFirst();
        if (first.isOk()) {
            counters.write.count.getAndIncrement();
        }
    }
}
