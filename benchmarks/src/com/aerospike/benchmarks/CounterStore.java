package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CounterStore {

    Current write = new Current();
    Current read = new Current();
    AtomicLong periodBegin = new AtomicLong();

    public static class Current {
        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger timeouts = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        LatencyManager latency;

    }

}
