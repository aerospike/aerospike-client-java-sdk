package com.aerospike.benchmarks;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.benchmarks.Constants.*;


public class LatencyManagerAerospike implements LatencyManager {

    private final Bucket[] buckets;
    private final int bitShift;
    private String header;
    private final int lastBucket;
    private final String units;


    public LatencyManagerAerospike(int columns, int bitShift, String units) {
        this.lastBucket = columns - 1;
        this.buckets = new Bucket[columns];
        this.bitShift = bitShift;
        this.units = units;
        for (int i = 0; i < columns; i++) {
            buckets[i] = new Bucket();
        }
        header = formHeader();
    }

    private String formHeader() {
        int limit = 1;
        StringBuilder s = new StringBuilder(64);
        s.append("      <=1").append(units).append(" >1").append(units);
        for (int i = 2; i < buckets.length; i++) {
            limit <<= bitShift;
            s.append(" >").append(limit).append(units);
        }
        return s.toString();
    }

    @Override
    public void add(long elapsed) {
        int index = getIndex(elapsed);
        buckets[index].count.incrementAndGet();
    }

    private int getIndex(long elapsedNanos) {
        long limit = 1L;
        long divisor;

        if (US.equalsIgnoreCase(units)) {
            divisor = NS_TO_US;
        } else {
            divisor = NS_TO_MS;
        }

        long elapsed = elapsedNanos / divisor;
        // Round up to nearest unit.
        if ((elapsedNanos - (elapsed * divisor)) > 0) {
            elapsed++;
        }

        for (int i = 0; i < lastBucket; i++) {
            if (elapsed <= limit) {
                return i;
            }
            limit <<= bitShift;
        }
        return lastBucket;
    }

    public void printHeader(PrintStream stream) {
        stream.println(header);
    }

    /**
     * Print latency percents for specified cumulative ranges.
     * This function is not absolutely accurate for a given time slice because this method
     * is not synchronized with the add() method.  Some values will slip into the next iteration.
     * It is not a good idea to add extra locks just to measure performance since that actually
     * affects performance.  Fortunately, the values will even out over time
     * (ie. no double counting).
     */
    @Override
    public void printResults(PrintStream stream, String prefix) {

        int[] array = new int[buckets.length];
        int runningSum = 0;
        int count;

        for (int i = buckets.length - 1; i >= 1 ; i--) {
            count = buckets[i].reset();
            array[i] = count + runningSum;
            runningSum += count;
        }

        // The first bucket (<=1ms) does not need a cumulative adjustment.
        count = buckets[0].reset();
        array[0] = count;
        runningSum += count;

        stream.print(prefix);
        int spaces = 6 - prefix.length();

        for (int j = 0; j < spaces; j++) {
            stream.print(' ');
        }


        printColumn(stream, 1, runningSum, array[0]);
        printColumn(stream, 1, runningSum, array[1]);

        int limit = 1;
        for (int i = 2; i < array.length; i++) {
            limit <<= bitShift;
            printColumn(stream, limit, runningSum, array[i]);
        }
        stream.println();
    }

    private void printColumn(PrintStream stream, int limit, double sum, int value) {
        long percent = 0;

        if (value > 0) {
            percent = Math.round((double)value * 100.0 / sum);
        }
        String percentString = Long.toString(percent) + "%";
        int spaces = Integer.toString(limit).length() + 4 - percentString.length();

        for (int j = 0; j < spaces; j++) {
            stream.print(' ');
        }
        stream.print(percentString);
    }

    @Override
    public void printSummaryHeader(PrintStream stream) {
        stream.println("Latency Summary");
        stream.println(header);
    }

    @Override
    public void printSummary(PrintStream stream, String prefix) {
        int[] array = new int[buckets.length];
        int runningSum = 0;
        int count;

        for (int i = buckets.length - 1; i >= 1 ; i--) {
            count = buckets[i].sum;
            array[i] = count + runningSum;
            runningSum += count;
        }
        // The first bucket (<=1ms) does not need a cumulative adjustment.
        count = buckets[0].sum;
        array[0] = count;
        runningSum += count;

        // Print cumulative results.
        stream.print(prefix);
        int spaces = 6 - prefix.length();

        for (int j = 0; j < spaces; j++) {
            stream.print(' ');
        }

        printColumn(stream, 1, runningSum, array[0]);
        printColumn(stream, 1, runningSum, array[1]);

        int limit = 1;
        for (int i = 2; i < array.length; i++) {
            limit <<= bitShift;
            printColumn(stream, limit, runningSum, array[i]);
        }
        stream.println();
    }

    private static class Bucket {
        final AtomicInteger count = new AtomicInteger();
        int sum = 0;

        int reset() {
            int c = count.getAndSet(0);
            sum += c;
            return c;
        }
    }
}
