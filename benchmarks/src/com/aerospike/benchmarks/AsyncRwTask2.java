package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.util.RandomShift;
import com.aerospike.client.fluent.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

public class AsyncRwTask2 extends RWTask implements Runnable {


    private final Session session;
    private final boolean useLatency;
    private final RandomShift random;

    public AsyncRwTask2(Arguments args, CounterStore counters, Session session) {
        super(args, counters);
        this.session = session;
        this.useLatency = counters.read.latency != null;
        this.random = new RandomShift();
    }

    @Override
    protected void get(Key key, String binName) {

    }

    @Override
    protected void get(Key key) {
        long begin = useLatency ? System.nanoTime() : 0;
        try (var stream = session.query(key).executeAsync(ErrorStrategy.IN_STREAM)) {
            List<RecordResult> results = new ArrayList<>();
            while (stream.hasNext()) {
                results.add(stream.next());
            }
            if (useLatency) {
                counters.read.latency.add(System.nanoTime() - begin);
            }
            processSingleKeyReadRecord(key, results);
        } catch (Throwable t) {
            handleReadException(t);
        }
    }

    @Override
    protected void upsert(Key key, Value[] values, String... bins) {
        long begin = counters.write.latency != null ? System.nanoTime() : 0;
        var builder = session.upsert(key);
        for (int i = 0; i < values.length; i++) {
            args.setBinFromValue(builder, bins[i], values[i]);
        }
        try (var stream = builder.executeAsync(ErrorStrategy.IN_STREAM)) {
            List<RecordResult> results = new ArrayList<>();
            while (stream.hasNext()) {
                results.add(stream.next());
            }
            if (useLatency) {
                counters.write.latency.add(System.nanoTime() - begin);
            }
            processSingleKeyWriteRecord(results);
        } catch (Throwable t) {
            handleWriteException(t);
        }
    }

    @Override
    protected void createOrReplace(Key random, Value[] key, String... bins) {

    }

    @Override
    protected void getBinsAndIncrement(Key key, int incrementedBy) {

    }

    @Override
    protected void get(List<Key> keys, String number) {

    }

    @Override
    protected void get(List<Key> keys) {

    }

    private void handleReadException(Throwable ex) {
        recordReadError(ex);
    }

    private void recordReadError(Throwable ex) {
        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
        if (cause instanceof AerospikeException) {
            readFailure((AerospikeException) cause);
        } else if (cause instanceof Exception) {
            readFailure((Exception) cause);
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
            recordReadError(first.exception());
            return;
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

    private void processSingleKeyWriteRecord(List<RecordResult> results) {
        if (results.isEmpty()) {
            return;
        }
        RecordResult first = results.getFirst();
        if (first.exception() != null) {
            recordWriteError(first.exception());
            return;
        }
        if (first.isOk()) {
            counters.write.count.getAndIncrement();
        }
    }

    private void handleWriteException(Throwable ex) {
        recordWriteError(ex);
    }

    private void recordWriteError(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
        if (cause instanceof AerospikeException) {
            writeFailure((AerospikeException) cause);
        } else if (cause instanceof Exception) {
            writeFailure((Exception) cause);
        }
    }

    @Override
    public void run() {
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
