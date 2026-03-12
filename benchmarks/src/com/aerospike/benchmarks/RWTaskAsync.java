package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.util.RandomShift;

import java.util.List;
import java.util.concurrent.Semaphore;

public class RWTaskAsync extends RWTask implements Runnable {

    private final Session session;
    private final boolean useLatency;
    private final Semaphore inFlightRequest;


    public RWTaskAsync(Arguments args, CounterStore counters, Session session, Semaphore inFlight) {
        super(args, counters);
        this.session = session;
        this.useLatency = counters.read.latency != null;
        this.inFlightRequest = inFlight;
    }

    private void acquire() {
        try {
            inFlightRequest.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for async permit", e);
        }
    }

    private void release() {
        inFlightRequest.release();
    }


    @Override
    protected void get(Key key, String binName) {
        acquire();
        long begin = useLatency ? System.nanoTime() : 0;
        var handle = session.query(key)
                .readingOnlyBins(binName)
                .executeAsync(ErrorStrategy.IN_STREAM);

        handle.asCompletableFuture()
                .thenAccept(results -> {
                    if (useLatency) {
                        counters.read.latency.add(System.nanoTime() - begin);
                    }
                    processSingleKeyReadRecord(key, results);
                })
                .whenComplete((r, ex) -> {
                    release();
                    handle.close();
                    if (ex != null) {
                        if (ex instanceof AerospikeException) {
                            readFailure((AerospikeException) ex);
                        } else if (ex instanceof Exception) {
                            readFailure((Exception) ex);
                        }
                    }
                });
    }

    @Override
    protected void get(Key key) {
        acquire();
        long begin = useLatency ? System.nanoTime() : 0;
        var handle = session.query(key)
                .executeAsync(ErrorStrategy.IN_STREAM);
        handle.asCompletableFuture()
                .thenAccept(results -> {
                    if (useLatency) {
                        counters.read.latency.add(System.nanoTime() - begin);
                    }
                    processSingleKeyReadRecord(key, results);
                })
                .whenComplete((r, ex) -> {
                    release();
                    handle.close();
                    if (ex != null) {
                        if (ex instanceof AerospikeException) {
                            readFailure((AerospikeException) ex);
                        } else if (ex instanceof Exception) {
                            readFailure((Exception) ex);
                        }
                    }
                });
    }

    @Override
    protected void upsert(Key key, Value[] values, String... bins) {
        acquire();
        long begin = counters.write.latency != null ? System.nanoTime() : 0;
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, bins[i], values[i]);
        }
        var handle = builder.executeAsync(ErrorStrategy.IN_STREAM);
        handle.asCompletableFuture()
                .thenAccept(results -> {
                    if (useLatency) {
                        counters.write.latency.add(System.nanoTime() - begin);
                    }
                    processSingleKeyWriteRecord(results);
                }).whenComplete((r, ex) -> {
                    release();
                    handle.close();
                    if (ex != null) {
                        if (ex instanceof AerospikeException) {
                            writeFailure((AerospikeException) ex);
                        } else if (ex instanceof Exception) {
                            writeFailure((Exception) ex);
                        }
                    }
                });
    }

    @Override
    public void run() {
        RandomShift random = new RandomShift();
        while (!shouldStop) {
            runCommand(random);
        }
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

    private void processSingleKeyReadRecord(Key key, List<RecordResult> results) {
        if (results.isEmpty()) {
            if (args.isReportNotFound()) {
                counters.readNotFound.getAndIncrement();
            } else {
                counters.read.count.getAndIncrement();
            }
            return;
        }
        RecordResult first = results.getFirst();
        if (first.isOk()) {
            Record record = first.recordOrNull();
            if (args.isReportNotFound() && record == null) {
                counters.readNotFound.getAndIncrement();
            } else {
                counters.read.count.getAndIncrement();
            }
        }
    }
}
