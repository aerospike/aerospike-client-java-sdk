package com.aerospike.benchmarks;

import java.io.PrintStream;

public interface LatencyManager {


    void add(long elapsed);

    void printHeader(PrintStream stream);

    /**
     * Print latency percents for specified cumulative ranges.
     * This function is not absolutely accurate for a given time slice because this method
     * is not synchronized with the add() method.  Some values will slip into the next iteration.
     * It is not a good idea to add extra locks just to measure performance since that actually
     * affects performance.  Fortunately, the values will even out over time
     * (ie. no double counting).
     */
    void printResults(PrintStream stream, String prefix);

    void printSummaryHeader(PrintStream stream);

    void printSummary(PrintStream stream, String prefix);
}
