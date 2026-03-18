package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.util.RandomShift;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class RWTaskAsync extends RWTask implements Runnable {

    private final Session session;
    private final boolean useLatency;
    private final int maxRequestInFlight;
    private final Semaphore inflightReqs;

    public RWTaskAsync(Arguments args, CounterStore counters,
                       Session session, Semaphore semaphore, int maxRequestInFlight) {
        super(args, counters);
        this.session = session;
        this.useLatency = counters.read.latency != null;
        this.inflightReqs = semaphore;
        this.maxRequestInFlight = maxRequestInFlight;
    }

    private void acquire() {
        try {
            inflightReqs.acquire();   // blocks if limit reached
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void release() {
        inflightReqs.release();
    }

    /**
     * Wait for all in-flight operations to complete.
     * Call after stop() for graceful shutdown.
     */
    public boolean awaitDrain(long timeoutMs) throws InterruptedException {
        boolean acquired = inflightReqs.tryAcquire(maxRequestInFlight, timeoutMs, TimeUnit.MILLISECONDS);
        if (acquired) {
            inflightReqs.release(maxRequestInFlight);
        }
        return acquired;
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
                    handle.close();
                    release();
                    if (ex != null) handleReadException(ex);
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
                    handle.close();
                    release();
                    if (ex != null) handleReadException(ex);
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
                    handle.close();
                    release();
                    if (ex != null) handleWriteException(ex);
                });
    }

    @Override
    protected void createOrReplace(Key key, Value[] values, String... bins) {
        acquire();
        long begin = counters.write.latency != null ? System.nanoTime() : 0;
        var builder = session.replace(key);
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
                    handle.close();
                    release();
                    if (ex != null) handleWriteException(ex);
                });
    }


    @Override
    protected void getBinsAndIncrement(Key key, int incrementedBy) {
        acquire();
        readRecordForUpdateAsync(key, rec -> incrementCounterAsync(key, rec, incrementedBy));
    }

    @Override
    protected void get(List<Key> keys, String binName) {
        acquire();
        long begin = System.nanoTime();
        var handle = session.query(keys).bins(binName)
                .executeAsync(ErrorStrategy.IN_STREAM);
        handle.asCompletableFuture()
                .thenAccept(results -> {
                    if (useLatency) {
                        long elapsed = System.nanoTime() - begin;
                        counters.read.latency.add(elapsed);
                    }
                    handleBatchReadResult(results);
                })
                .whenComplete((r, ex) -> {
                    handle.close();
                    release();
                    if (ex != null) handleReadException(ex);
                });
    }

    @Override
    protected void get(List<Key> keys) {
        acquire();
        long begin = System.nanoTime();
        var handle = session.query(keys)
                .executeAsync(ErrorStrategy.IN_STREAM);
        handle.asCompletableFuture()
                .thenAccept(results -> {
                    if (useLatency) {
                        long elapsed = System.nanoTime() - begin;
                        counters.read.latency.add(elapsed);
                    }
                    handleBatchReadResult(results);
                })
                .whenComplete((r, ex) -> {
                    handle.close();
                    release();
                    if (ex != null) handleReadException(ex);
                });
    }

    @Override
    public void run() {
        RandomShift random = new RandomShift();
        while (!shouldStop) {
            runCommand(random);
        }
    }

    private void handleReadException(Throwable ex) {
        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
        if (ex instanceof AerospikeException) {
            readFailure((AerospikeException) cause);
        } else {
            readFailure((Exception) cause);
        }
    }

    private void handleWriteException(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
        if (cause instanceof AerospikeException) {
            writeFailure((AerospikeException) cause);
        } else if (cause instanceof Exception) {
            writeFailure((Exception) cause);
        }
    }

    private void handleBatchReadResult(List<RecordResult> results) {
        AerospikeException firstFailure = null;
        for (RecordResult r : results) {
            if (r.exception() != null) {
                firstFailure = r.exception();
                break;
            }
        }
        if (firstFailure == null) {
            counters.read.count.incrementAndGet();
        } else {
            readFailure(firstFailure);
        }
    }

    private void readRecordForUpdateAsync(Key key, Consumer<Record> onSuccess) {
        long readBegin = useLatency ? System.nanoTime() : 0;
        var readHandle = session.query(key).executeAsync(ErrorStrategy.IN_STREAM);
        readHandle.asCompletableFuture()
                .whenComplete((results, readEx) -> {
                    readHandle.close();
                    Record rec = (results == null || results.isEmpty()) ? null : results.getFirst().recordOrNull();
                    if (readEx != null) {
                        if (readEx instanceof AerospikeException) {
                            readFailure((AerospikeException) readEx);
                        } else if (readEx instanceof Exception) {
                            readFailure((Exception) readEx);
                        }
                    } else {
                        if (useLatency) {
                            counters.read.latency.add(System.nanoTime() - readBegin);
                        }
                        processSingleKeyReadRecord(key, results);
                    }
                    onSuccess.accept(rec);
                });
    }

    private void incrementCounterAsync(Key key, Record rec, int incrementedBy) {
        int generation = (rec != null) ? rec.generation : 0;
        long writeBegin = useLatency ? System.nanoTime() : 0;
        var cmd = session.upsert(key)
                .bin("test-counter").add(incrementedBy);
        if (generation > 0) {
            cmd.ensureGenerationIs(generation);
        }
        var writeHandle = cmd.executeAsync(ErrorStrategy.IN_STREAM);
        writeHandle.asCompletableFuture()
                .thenAccept(writeResults -> {
                    if (useLatency) {
                        counters.write.latency.add(System.nanoTime() - writeBegin);
                    }
                    processSingleKeyWriteRecord(writeResults);
                })
                .whenComplete((r, ex) -> {
                    writeHandle.close();
                    release();
                    if (ex != null) {
                        handleWriteException(ex);
                    }
                });
    }

    private void processSingleKeyWriteRecord(List<RecordResult> results) {
        if (results.isEmpty()) {
            return;
        }
        RecordResult first = results.getFirst();
        if (first.exception() != null) {
            throw new AerospikeException(first.exception());
        }
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
        if (first.exception() != null) {
            throw new AerospikeException(first.exception());
        }
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
