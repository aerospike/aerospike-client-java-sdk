/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.command.PartitionTracker.NodePartitions;
import com.aerospike.client.sdk.util.RandomShift;
import com.aerospike.client.sdk.util.Util;

public final class QueryExecutor implements IQueryExecutor {

    private final Cluster cluster;
    private final QueryCommand cmd;
    private final PartitionTracker tracker;
    private final List<QueryThread> threads;
    private final AtomicInteger completedCount;
    private final AtomicBoolean done;
    private volatile Throwable exception;
    private int maxConcurrentThreads;
    private final AsyncRecordStream stream;
    private RandomShift random;

    public QueryExecutor(
        Cluster cluster,
        QueryCommand cmd,
        int nodeCapacity,
        PartitionTracker tracker,
        AsyncRecordStream stream
    ) {
        this.cluster = cluster;
        this.cmd = cmd;
        this.tracker = tracker;
        this.stream = stream;
        this.threads = new ArrayList<QueryThread>(nodeCapacity);
        this.completedCount = new AtomicInteger();
        this.done = new AtomicBoolean();
        this.random = new RandomShift();

        cluster.addCommandCount();
    }

    public void execute() {
        long taskId = random.nextLong();

        while (true) {
            List<NodePartitions> list = tracker.assignPartitionsToNodes(cluster, cmd.namespace);

            // Initialize maximum number of nodes to query in parallel.
            maxConcurrentThreads = (cmd.maxConcurrentNodes == 0 || cmd.maxConcurrentNodes >= list.size())?
                list.size() : cmd.maxConcurrentNodes;

            boolean parallel = maxConcurrentThreads > 1 && list.size() > 1;
            ExecutorService es = null;

            synchronized(threads) {
                // RecordSet thread may have aborted query, so check done under lock.
                if (done.get()) {
                    break;
                }

                threads.clear();

                if (parallel) {
                    es = cluster.getExecutorService();

                    for (NodePartitions nodePartitions : list) {
                        QueryNodeExecutor exec = new QueryNodeExecutor(cluster, cmd, taskId, tracker, nodePartitions, stream);
                        threads.add(new QueryThread(exec));
                    }

                    for (int i = 0; i < maxConcurrentThreads; i++) {
                        es.execute(threads.get(i));
                    }
                }
            }

            if (parallel) {
                // Wait till virtual threads complete.
                es.close();
            }
            else {
                for (NodePartitions nodePartitions : list) {
                    QueryNodeExecutor exec = new QueryNodeExecutor(cluster, cmd, taskId, tracker, nodePartitions, stream);
                    exec.execute();
                }
            }

            if (exception != null) {
                break;
            }

            // Set done to false so RecordSet thread has chance to close early again.
            done.set(false);

            if (tracker.isComplete(cluster, cmd)) {
                // All partitions received.
                stream.complete();
                break;
            }

            if (cmd.sleepBetweenRetries > 0) {
                // Sleep before trying again.
                Util.sleep(cmd.sleepBetweenRetries);
            }

            completedCount.set(0);
            exception = null;

            // taskId must be reset on next pass to avoid server duplicate query detection.
            taskId = random.nextLong();
        }
    }

    private final void threadCompleted() {
        int finished = completedCount.incrementAndGet();

        if (finished < threads.size()) {
            int next = finished + maxConcurrentThreads - 1;

            // Determine if a new command needs to be started.
            if (next < threads.size() && ! done.get()) {
                // Start new command in existing thread.
                threads.get(next).run();
            }
        }
    }

    @Override
    public final void stopThreads(Throwable cause) {
        // There is no need to stop threads if all threads have already completed.
        if (done.compareAndSet(false, true)) {
            exception = cause;

            // Send stop signal to threads.
            // Must synchronize here because this method can be called from the main
            // RecordSet thread (user calls close() before retrieving all records)
            // which may conflict with the parallel QueryPartitionExecutor thread.
            synchronized(threads) {
                for (QueryThread thread : threads) {
                    thread.stop();
                }
            }
            stream.complete();
        }
    }

    @Override
    public final void checkForException() {
        // Throw an exception if an error occurred.
        if (exception != null) {
            AerospikeException ae;

            if (exception instanceof AerospikeException) {
                ae = (AerospikeException)exception;
            }
            else {
                ae = new AerospikeException(exception);
            }
            tracker.partitionError();
            ae.setIteration(tracker.iteration);
            throw ae;
        }
    }

    private final class QueryThread implements Runnable {
        private final QueryNodeExecutor exec;

        public QueryThread(QueryNodeExecutor exec) {
            this.exec = exec;
        }

        public void run() {
            try {
                if (exec.isValid()) {
                    exec.execute();
                }
                threadCompleted();
            }
            catch (Throwable e) {
                // Terminate other query threads.
                stopThreads(e);
            }
        }

        /**
         * Send stop signal to each thread.
         */
        public void stop() {
            exec.stop();
        }
    }
}
