package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.util.RandomShift;

import java.util.List;
import java.util.concurrent.Semaphore;

public class InsertTaskAsync extends InsertTask implements Runnable {

    private final long startKey;
    private final long endKey;
    private final String[] binArr;
    private final boolean useLatency;
    private final Semaphore semaphore;
    private final Session session;
    private long currentKey;

    public InsertTaskAsync(Session session, Arguments args, CounterStore counters, Semaphore semaphore,long start, long keyCount) {
        super(args, counters);
        this.startKey = start;
        this.endKey = startKey + keyCount;
        this.binArr = args.getBinNames(true);
        this.useLatency = counters.write.latency != null;
        this.semaphore = semaphore;
        this.session = session;
    }


    @Override
    public void run() {
        RandomShift random = new RandomShift();
        while (currentKey + startKey < endKey) {
            executeCommand(currentKey + startKey, random);
            currentKey++;
        }
    }

    private void executeCommand(long keyIdx, RandomShift random) {
        Key key = new Key(args.getNamespace(), args.getSetName(), keyIdx);
        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, true, keyIdx);
        acquire();
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
                }).whenComplete((r, ex) -> {
                    handle.close();
                    release();
                    if (ex != null) {
                        if (ex instanceof AerospikeException) {
                            writeFailure((AerospikeException) ex);
                        } else if (ex instanceof Exception) {
                            writeFailure((Exception) ex);
                        }
                        currentKey--;
                    }
                });
    }

    private void acquire() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for async permit", e);
        }
    }

    private void release() {
        semaphore.release();
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
