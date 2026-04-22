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
package com.aerospike.client.sdk;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.query.RecordStreamImpl;
import com.aerospike.client.sdk.query.SingleItemRecordStream;

public class RecordStream implements Iterator<RecordResult>, Closeable {
    private final RecordStreamImpl impl;

    // ========================================
    // Unconsumed error stream detection (disabled)
    // ========================================
    // At millions of TPS, Cleaner registration per-stream is too costly.
    // This implements lazy registration: only streams that actually contain
    // errors register with the Cleaner. If such a stream is GC'd without
    // being iterated, a warning is logged -- similar to how CompletableFuture
    // reports unhandled exceptions.
    //
    // To enable:
    //   1. Uncomment the block below.
    //   2. Have builders call rs.markContainsErrors() when embedding error
    //      results into a stream under IN_STREAM disposition.
    //   3. Uncomment the markConsumed() call in hasNext().
    //
    // private static final java.lang.ref.Cleaner CLEANER = java.lang.ref.Cleaner.create();
    //
    // /**
    //  * Shared state between the RecordStream and its Cleaner action.
    //  * MUST be a separate object -- the cleaning action must never capture
    //  * a reference to the RecordStream itself, or it can never become
    //  * phantom-reachable and the cleaner will never fire.
    //  */
    // private static class CleanerState implements Runnable {
    //     volatile boolean consumed;
    //     final String creationSite;
    //
    //     CleanerState(String creationSite) {
    //         this.creationSite = creationSite;
    //     }
    //
    //     @Override
    //     public void run() {
    //         if (!consumed) {
    //             Log.warn("RecordStream with errors was garbage-collected without being " +
    //                 "consumed. Errors may have been silently lost. Created at: " + creationSite);
    //         }
    //     }
    // }
    //
    // private CleanerState cleanerState;
    //
    // /**
    //  * Mark this stream as containing error results. On the first call,
    //  * registers with the Cleaner so that a warning is logged if the stream
    //  * is GC'd without ever being iterated.
    //  */
    // public void markContainsErrors() {
    //     if (cleanerState == null) {
    //         StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
    //         cleanerState = new CleanerState(caller.toString());
    //         CLEANER.register(this, cleanerState);
    //     }
    // }
    //
    // private void markConsumed() {
    //     if (cleanerState != null) {
    //         cleanerState.consumed = true;
    //     }
    // }

    /**
     * Creates an empty RecordStream with no implementation.
     * This constructor is typically used internally or for creating an empty stream.
     */
    RecordStream() {impl = null;}

    /**
     * Creates a RecordStream containing a single RecordResult.
     *
     * @param rec the single record result to include in the stream
     */
    RecordStream(RecordResult rec) {
        impl = new SingleItemRecordStream(rec);
    }

    /**
     * Creates a RecordStream containing a single record from a key and record pair.
     *
     * @param key the key of the record
     * @param record the record data
     */
    RecordStream(Key key, Record record) {
        RecordResult rec = new RecordResult(key, record, 0); // Single item, index = 0
        impl = new SingleItemRecordStream(rec);
    }
    /**
     * Creates a RecordStream from a list of RecordResult objects.
     * This is typically used for batch query results.
     *
     * @param records the list of results
     * @param limit the maximum number of records to include (0 or negative means no limit)
     */
    RecordStream(List<RecordResult> records, long limit) {
        AsyncRecordStream asyncStream = new AsyncRecordStream(Math.max(100, records.size()));

        // Filter and limit records
        int count = 0;
        for (RecordResult record : records) {
            if (limit <= 0 || count < limit) {
                asyncStream.publish(record);
                count++;
            }
        }
        asyncStream.complete();
        impl = asyncStream;
    }
    /**
     * Creates a RecordStream from an AsyncRecordStream.
     * This constructor is used internally to wrap asynchronous record streams.
     *
     * @param asyncStream the asynchronous record stream to wrap
     */
    public RecordStream(AsyncRecordStream asyncStream) {
        impl = asyncStream;
    }

    /**
     * Creates a RecordStream for index/scan queries with server-side chunking.
     *
     * <p>This constructor is used for queries that stream results from the server in chunks.
     * For client-side sorting and pagination, use {@link #asNavigatableStream()} on the
     * returned stream.</p>
     *
     * @param stream first-chunk async buffer (see {@link ChunkedRecordStream})
     * @param cmd query command used to load each chunk
     * @param limit maximum records to yield; non-positive values are treated as unbounded
     * @param recordQueueSize async queue capacity per chunk
     */
    public RecordStream(AsyncRecordStream stream, QueryCommand cmd, long limit, int recordQueueSize) {
        if (limit <= 0) {
            limit = Long.MAX_VALUE;
        }

        impl = new ChunkedRecordStream(stream, cmd, limit, recordQueueSize);
    }

    /**
     * Checks if there are more chunks available from the server.
     *
     * <p>This method is used for server-side streaming pagination (chunks).
     * Returns true if more data chunks are available from the server.</p>
     *
     * <p><b>Note:</b> This is distinct from client-side pagination (pages) provided by
     * {@link NavigatableRecordStream}. Use {@link #asNavigatableStream()} for
     * client-side sorting and bi-directional pagination.</p>
     *
     * <p>This method does <b>not</b> close the stream.</p>
     *
     * @return true if more chunks are available, false otherwise
     */
    public boolean hasMoreChunks() {
        return impl == null ? false : impl.hasMoreChunks();
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * <p>Repeated calls return the same answer until {@link #next()} advances the stream.</p>
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        // markConsumed();  // uncomment to enable unconsumed-error-stream detection
        return impl == null ? false : impl.hasNext();
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     */
    @Override
    public RecordResult next() {
        return impl == null ? null : impl.next();
    }

    /**
     * Converts this RecordStream into a Java {@link Stream}. Note that this loses
     * pagination information; all records are accessible through the Stream.
     *
     * <p>This method does <b>not</b> close the stream immediately. Closing the returned
     * Java Stream (e.g. via try-with-resources) will close the underlying RecordStream.</p>
     *
     * @return a Stream of RecordResult backed by this RecordStream
     */
    public Stream<RecordResult> stream() {
        Stream<RecordResult> records = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        this, Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
        records.onClose(() -> {
            this.close();
        });
        return records;
    }

    // ========================================
    // CompletableFuture adapters
    // ========================================

    /**
     * Drains this stream into a {@link CompletableFuture} that completes with all results
     * as a list. The draining happens on a virtual thread, so this method returns immediately.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when draining completes
     * or an exception occurs.</p>
     *
     * <p>Best suited for point lookups and batch operations with bounded result sets.
     * For index queries that may return millions of records, prefer {@link #asPublisher()}
     * or iterate the stream directly.</p>
     *
     * <pre>
     * CompletableFuture&lt;List&lt;RecordResult&gt;&gt; future =
     *     session.query(dataSet.id("k1", "k2")).executeAsync(ErrorStrategy.IN_STREAM)
     *            .asCompletableFuture();
     *
     * future.thenAccept(results -&gt; results.forEach(r -&gt; System.out.println(r.key())));
     * </pre>
     *
     * @return a CompletableFuture that completes with all results from this stream
     */
    public CompletableFuture<List<RecordResult>> asCompletableFuture() {
        CompletableFuture<List<RecordResult>> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                List<RecordResult> results = new ArrayList<>();
                while (hasNext()) {
                    results.add(next());
                }
                future.complete(results);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                close();
            }
        });
        return future;
    }

    /**
     * Drains this stream, maps each record using the provided mapper, and completes the
     * returned {@link CompletableFuture} with the mapped list. Records with non-OK result
     * codes cause the future to complete exceptionally.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when draining completes
     * or an exception occurs.</p>
     *
     * <pre>
     * CompletableFuture&lt;List&lt;Customer&gt;&gt; future =
     *     session.query(customerDataSet.id("C001", "C002")).executeAsync(ErrorStrategy.IN_STREAM)
     *            .asCompletableFuture(customerMapper);
     * </pre>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert each record
     * @return a CompletableFuture that completes with the mapped results
     */
    public <T> CompletableFuture<List<T>> asCompletableFuture(RecordMapper<T> mapper) {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                List<T> results = new ArrayList<>();
                while (hasNext()) {
                    RecordResult rr = next();
                    Record rec = rr.recordOrThrow();
                    results.add(mapper.fromMap(rec.bins, rr.key(), rec.generation));
                }
                future.complete(results);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                close();
            }
        });
        return future;
    }

    // ========================================
    // Flow.Publisher adapter
    // ========================================

    /**
     * Adapts this stream into a {@link Flow.Publisher} for reactive consumption with
     * backpressure support. Uses only JDK types -- no dependency on Project Reactor or RxJava.
     *
     * <p>This publisher is unicast: only one subscriber is supported. Subsequent subscribers
     * receive {@code onError(IllegalStateException)}.</p>
     *
     * <p>Ideal for large or unbounded result sets (e.g. index queries) where collecting
     * everything into memory via {@link #asCompletableFuture()} is not practical.</p>
     *
     * <p><b>Raw JDK usage:</b></p>
     * <pre>
     * session.query(dataSet).where("age &gt; 21").executeAsync(ErrorStrategy.IN_STREAM)
     *     .asPublisher()
     *     .subscribe(new Flow.Subscriber&lt;&gt;() {
     *         Flow.Subscription sub;
     *         public void onSubscribe(Flow.Subscription s) { sub = s; s.request(100); }
     *         public void onNext(RecordResult item)        { process(item); sub.request(1); }
     *         public void onError(Throwable t)             { t.printStackTrace(); }
     *         public void onComplete()                     { System.out.println("done"); }
     *     });
     * </pre>
     *
     * <p><b>Project Reactor:</b></p>
     * <pre>
     * Flow.Publisher&lt;RecordResult&gt; publisher = session.query(dataSet)
     *     .where("age &gt; 21")
     *     .executeAsync(ErrorStrategy.IN_STREAM)
     *     .asPublisher();
     *
     * Flux.from(JdkFlowAdapter.flowPublisherToFlux(publisher))
     *     .filter(RecordResult::isOk)
     *     .map(r -&gt; r.recordOrThrow().getString("name"))
     *     .buffer(100)
     *     .subscribe(batch -&gt; saveBatch(batch));
     * </pre>
     *
     * <p><b>RxJava 3:</b></p>
     * <pre>
     * Flow.Publisher&lt;RecordResult&gt; publisher = session.query(dataSet)
     *     .where("age &gt; 21")
     *     .executeAsync(ErrorStrategy.IN_STREAM)
     *     .asPublisher();
     *
     * Flowable.fromPublisher(FlowAdapters.toPublisher(publisher))
     *     .filter(RecordResult::isOk)
     *     .map(r -&gt; r.recordOrThrow().getString("name"))
     *     .buffer(100)
     *     .subscribe(batch -&gt; saveBatch(batch));
     * </pre>
     *
     * <p>Both adapters ({@code JdkFlowAdapter} in Reactor, {@code FlowAdapters} in the JDK's
     * {@code java.util.concurrent} package) bridge between {@code Flow.Publisher} and the
     * Reactive Streams {@code org.reactivestreams.Publisher} that Reactor and RxJava expect.</p>
     *
     * <p>The stream is closed automatically when the subscription completes, errors, or is
     * cancelled. This method does <b>not</b> close the stream immediately.</p>
     *
     * @return a Flow.Publisher that streams results with backpressure
     */
    public Flow.Publisher<RecordResult> asPublisher() {
        return new RecordStreamPublisher(this);
    }

    private static class RecordStreamPublisher implements Flow.Publisher<RecordResult> {
        private final RecordStream source;
        private final AtomicBoolean subscribed = new AtomicBoolean(false);

        RecordStreamPublisher(RecordStream source) {
            this.source = source;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super RecordResult> subscriber) {
            if (subscriber == null) {
                throw new NullPointerException("Subscriber must not be null");
            }
            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long n) {}
                    @Override public void cancel() {}
                });
                subscriber.onError(new IllegalStateException(
                    "RecordStream is single-pass; only one subscriber is supported"));
                return;
            }
            new RecordStreamSubscription(source, subscriber).start();
        }
    }

    private static class RecordStreamSubscription implements Flow.Subscription {
        private final RecordStream source;
        private final Flow.Subscriber<? super RecordResult> subscriber;
        private final AtomicLong demand = new AtomicLong(0);
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private volatile Thread drainThread;

        RecordStreamSubscription(RecordStream source, Flow.Subscriber<? super RecordResult> subscriber) {
            this.source = source;
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                cancelled.set(true);
                subscriber.onError(new IllegalArgumentException(
                    "§3.9 spec violation: non-positive request: " + n));
                return;
            }
            demand.getAndUpdate(current -> {
                long result = current + n;
                return (result < 0) ? Long.MAX_VALUE : result;
            });
            Thread t = drainThread;
            if (t != null) {
                LockSupport.unpark(t);
            }
        }

        @Override
        public void cancel() {
            cancelled.set(true);
            source.close();
            Thread t = drainThread;
            if (t != null) {
                LockSupport.unpark(t);
            }
        }

        void start() {
            subscriber.onSubscribe(this);
            Thread.startVirtualThread(() -> {
                drainThread = Thread.currentThread();
                try {
                    while (!cancelled.get()) {
                        if (!source.hasNext()) {
                            subscriber.onComplete();
                            return;
                        }
                        while (demand.get() <= 0 && !cancelled.get()) {
                            LockSupport.park();
                        }
                        if (cancelled.get()) {
                            return;
                        }
                        demand.decrementAndGet();
                        subscriber.onNext(source.next());
                    }
                } catch (Throwable t) {
                    if (!cancelled.get()) {
                        subscriber.onError(t);
                    }
                } finally {
                    source.close();
                }
            });
        }
    }

    /**
     * Filter the stream to return only failed operations. A failed operation is one where
     * the result code is not {@link ResultCode#OK}.
     *
     * <p>This is a <b>terminal operation</b> that consumes and closes this stream, returning
     * a new RecordStream containing only the records with non-OK result codes.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * RecordStream results = session.update(keys).bin("name").setTo("value").execute();
     * RecordStream failures = results.failures();
     * failures.forEach(failure -&gt; {
     *     System.err.println("Failed for key: " + failure.key() +
     *                        ", reason: " + failure.message());
     * });
     * </pre>
     *
     * @return a new RecordStream containing only records with resultCode != OK. The returned
     *         stream holds all records in memory and does not require closing, though calling
     *         {@link #close()} on it is harmless.
     */
    public RecordStream failures() {
        try {
            List<RecordResult> failedRecords = new ArrayList<>();

            while (this.hasNext()) {
                RecordResult result = this.next();
                if (result.resultCode() != ResultCode.OK) {
                    failedRecords.add(result);
                }
            }

            return new RecordStream(failedRecords, 0L);
        } finally {
            close();
        }
    }

    /**
     * Consumes all records in the stream and returns them as a list of mapped objects.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when all records have
     * been consumed or an exception occurs.</p>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert each record to the target type
     * @return a list of mapped objects
     * @throws AerospikeException if any element has a non-OK result code
     */
    public <T> List<T> toObjectList(RecordMapper<T> mapper) {
        try {
            List<T> result = new ArrayList<>();
            while (hasNext()) {
                RecordResult keyRecord = next();
                Record rec = keyRecord.recordOrThrow();
                result.add(mapper.fromMap(rec.bins, keyRecord.key(), rec.generation));
            }
            return result;
        } finally {
            close();
        }
    }

    /**
     * Converts this RecordStream into a NavigatableRecordStream for in-memory sorting and pagination.
     *
     * <p>This is a <b>terminal operation</b> that reads all records from the current stream
     * into memory and closes it, returning a NavigatableRecordStream that provides builder-style
     * APIs for sorting and pagination. This is useful when you need to sort results after
     * fetching them from the database, or when you want to paginate through results in a
     * different way than the original query.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * RecordStream results = session.query(customerDataSet).execute();
     * NavigatableRecordStream navigatable = results.asNavigatableStream()
     *     .pageSize(20)
     *     .sortBy(List.of(
     *         SortProperties.ascending("name"),
     *         SortProperties.descending("age")
     *     ));
     *
     * // Iterate through pages
     * while (navigatable.hasMorePages()) {
     *     while (navigatable.hasNext()) {
     *         RecordResult record = navigatable.next();
     *         // Process record
     *     }
     * }
     * </pre>
     *
     * <p><b>Warning:</b> This method loads all records into memory. For large result sets,
     * consider using the limit parameter in {@link #asNavigatableStream(long)} to avoid
     * excessive memory usage.</p>
     *
     * @return a NavigatableRecordStream containing all records from this stream
     */
    public NavigatableRecordStream asNavigatableStream() {
        return new NavigatableRecordStream(this);
    }

    /**
     * Converts this RecordStream into a NavigatableRecordStream with a record limit.
     *
     * <p>This is a <b>terminal operation</b> that reads records from the current stream into
     * memory up to the specified limit and closes it, returning a NavigatableRecordStream that
     * provides builder-style APIs for sorting and pagination.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * RecordStream results = session.query(customerDataSet).execute();
     * NavigatableRecordStream navigatable = results.asNavigatableStream(1000)
     *     .pageSize(20)
     *     .sortBy(SortProperties.descending("age"));
     * </pre>
     *
     * @param limit the maximum number of records to load into memory (0 or negative means no limit)
     * @return a NavigatableRecordStream containing up to limit records from this stream
     */
    public NavigatableRecordStream asNavigatableStream(long limit) {
        return new NavigatableRecordStream(this, limit);
    }

    /**
     * Performs the given action for each element of the stream until all elements
     * have been processed or the action throws an exception.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when iteration
     * completes or an exception occurs.</p>
     *
     * @param consumer the action to be performed for each element
     */
    public void forEach(Consumer<RecordResult> consumer) {
        try {
            while (hasNext()) {
                consumer.accept(next());
            }
        } finally {
            close();
        }
    }

    /**
     * Maps each element in the stream using the provided mapper and passes the result to the
     * consumer. If any element has a non-OK result code, an exception is thrown.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when iteration
     * completes or an exception occurs.</p>
     *
     * <p>Example usage:</p>
     * <pre>
     * session.query(customerDataSet.id("C001", "C002")).execute()
     *     .forEach(customerMapper, customer -&gt; System.out.println(customer.getName()));
     * </pre>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert each record to the target type
     * @param consumer the action to be performed for each mapped element
     * @throws AerospikeException if any element has a non-OK result code
     */
    public <T> void forEach(RecordMapper<T> mapper, Consumer<T> consumer) {
        try {
            while (hasNext()) {
                RecordResult rr = next();
                Record rec = rr.recordOrThrow();
                consumer.accept(mapper.fromMap(rec.bins, rr.key(), rec.generation));
            }
        } finally {
            close();
        }
    }

    /**
     * Searches the stream for a record with the specified key and returns it if found.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after the search completes
     * (whether the key is found or not). Because the stream is closed, this operation cannot
     * be retried on the same stream instance.</p>
     *
     * @param key the key to search for
     * @return an Optional containing the record if found, or empty if not found
     * @throws AerospikeException if the record exists but the result code is not OK
     */
    public Optional<Record> get(Key key) {
        try {
            while (hasNext()) {
                RecordResult kr = next();
                if (kr.key().equals(key)) {
                    return Optional.of(kr.recordOrThrow());
                }
            }
            return Optional.empty();
        } finally {
            close();
        }
    }

    /**
     * Searches the stream for a record with the specified key and returns it mapped to the
     * target type, or {@code Optional.empty()} if the key doesn't exist.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after the search completes
     * (whether the key is found or not). Because the stream is closed, this operation cannot
     * be retried on the same stream instance.</p>
     *
     * @param <T> the type of the object to be returned
     * @param key the key of the record
     * @param mapper the mapper to use to convert the record to the class
     * @return an Optional containing the mapped data, or empty if the key was not found
     * @throws AerospikeException if the result code is not OK
     */
    public <T> Optional<T> get(Key key, RecordMapper<T> mapper) {
        try {
            while (hasNext()) {
                RecordResult thisRecord = next();
                if (thisRecord.key().equals(key)) {
                    Record rec = thisRecord.recordOrThrow();
                    return Optional.of(mapper.fromMap(rec.bins, thisRecord.key(), rec.generation));
                }
            }
            return Optional.empty();
        } finally {
            close();
        }
    }

    // ========================================
    // Pop methods (non-closing single-item retrieval)
    // ========================================

    /**
     * Removes and returns the next element from the stream. If the element has a non-OK result code,
     * an appropriate exception is thrown.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream after retrieving the first element, use {@link #getFirst()}.</p>
     *
     * @return an Optional containing the next element, or empty if the stream is exhausted
     * @throws AerospikeException if the element has a non-OK result code
     */
    public Optional<RecordResult> pop() {
        return pop(true);
    }

    /**
     * Removes and returns the next element from the stream.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream after retrieving the first element, use {@link #getFirst(boolean)}.</p>
     *
     * @param throwException if true and the element has a non-OK result code, an exception is thrown;
     *        if false, the caller must inspect {@link RecordResult#resultCode()} to check for errors
     * @return an Optional containing the next element, or empty if the stream is exhausted
     * @throws AerospikeException if throwException is true and the element has a non-OK result code
     */
    public Optional<RecordResult> pop(boolean throwException) {
        if (hasNext()) {
            return throwException
                ? Optional.of(next().orThrow())
                : Optional.of(next());
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the next element from the stream, mapped to an object using the
     * provided mapper.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream, use {@link #getFirst(RecordMapper)}.</p>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert the record to the target type
     * @return an Optional containing the mapped object, or empty if the stream is exhausted
     * @throws AerospikeException if the element has a non-OK result code
     */
    public <T> Optional<T> pop(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            return Optional.of(mapper.fromMap(rec.bins, item.key(), rec.generation));
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the next record from the stream, or {@code null} if the stream
     * is exhausted. If the element has a non-OK result code, an exception is thrown.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream, use {@link #getFirstRecord()}.</p>
     *
     * @return the next Record, or null if the stream is exhausted
     * @throws AerospikeException if the element has a non-OK result code
     */
    public Record popRecord() {
        return hasNext() ? next().recordOrThrow() : null;
    }

    /**
     * Removes and returns the next element from the stream, converted to a Boolean value.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream, use {@link #getFirstBoolean()}.</p>
     *
     * @return an Optional containing the next element as a Boolean, or empty if the stream is exhausted
     */
    public Optional<Boolean> popBoolean() {
        if (hasNext()) {
            return Optional.of(next().asBoolean());
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the UDF result from the next element in the stream.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream, use {@link #getFirstUdfResult()}.</p>
     *
     * @return an Optional containing the UDF result, or empty if the stream is exhausted
     * @throws AerospikeException if the UDF invocation failed
     */
    public Optional<Object> popUdfResult() {
        if (hasNext()) {
            return Optional.ofNullable(next().udfResultOrThrow());
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the UDF result from the next element in the stream, mapped to an
     * object using the provided mapper. The UDF must return a map.
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream, use {@link #getFirstUdfResult(RecordMapper)}.</p>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert the UDF result map to the target type
     * @return an Optional containing the mapped UDF result, or empty if the stream is exhausted
     * @throws AerospikeException with ResultCode = OP_NOT_APPLICABLE if the UDF return value is not a map
     */
    public <T> Optional<T> popUdfResult(RecordMapper<T> mapper) {
        if (hasNext()) {
            return Optional.ofNullable(next().udfResultAs(mapper));
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the next element from the stream, mapped to an object along with
     * its record metadata (generation and expiration).
     *
     * <p>This method does <b>not</b> close the stream. The caller is responsible for closing
     * the stream when done, or for fully consuming it. For a terminal variant that automatically
     * closes the stream, use {@link #getFirstWithMetadata(RecordMapper)}.</p>
     *
     * @param <T> the type of the object to be returned
     * @param mapper the mapper to convert the record to the target type
     * @return an Optional containing an ObjectWithMetadata, or empty if the stream is exhausted
     * @throws AerospikeException if the element has a non-OK result code
     */
    public <T> Optional<ObjectWithMetadata<T>> popWithMetadata(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            T object = mapper.fromMap(rec.bins, item.key(), rec.generation);
            return Optional.of(new ObjectWithMetadata<>(object, rec));
        }
        return Optional.empty();
    }

    // ========================================
    // GetFirst methods (terminal, auto-close)
    // ========================================

    /**
     * Gets the first element from the stream. If the element has a non-OK result code,
     * an appropriate exception is thrown.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #pop()}.</p>
     *
     * @return an Optional containing the first element, or empty if the stream is empty
     * @throws AerospikeException if the element has a non-OK result code
     */
    public Optional<RecordResult> getFirst() {
        return getFirst(true);
    }

    /**
     * Gets the first element from the stream.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #pop(boolean)}.</p>
     *
     * @param throwException if true and the element has a non-OK result code, an exception is thrown;
     *        if false, the caller must inspect {@link RecordResult#resultCode()} to check for errors
     * @return an Optional containing the first element, or empty if the stream is empty
     * @throws AerospikeException if throwException is true and the element has a non-OK result code
     */
    public Optional<RecordResult> getFirst(boolean throwException) {
        try {
            return pop(throwException);
        } finally {
            close();
        }
    }

    /**
     * Gets the first element from the stream, mapped to an object using the provided mapper.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #pop(RecordMapper)}.</p>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert the record to the target type
     * @return an Optional containing the mapped object, or empty if the stream is empty
     * @throws AerospikeException if the element has a non-OK result code
     */
    public <T> Optional<T> getFirst(RecordMapper<T> mapper) {
        try {
            return pop(mapper);
        } finally {
            close();
        }
    }

    /**
     * Gets the first record from the stream, or {@code null} if the stream is empty.
     * If the element has a non-OK result code, an exception is thrown.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #popRecord()}.</p>
     *
     * @return the first Record, or null if the stream is empty
     * @throws AerospikeException if the element has a non-OK result code
     */
    public Record getFirstRecord() {
        try {
            return popRecord();
        } finally {
            close();
        }
    }

    /**
     * Gets the first element from the stream and converts it to a Boolean value.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #popBoolean()}.</p>
     *
     * @return an Optional containing the first element as a Boolean, or empty if the stream is empty
     */
    public Optional<Boolean> getFirstBoolean() {
        try {
            return popBoolean();
        } finally {
            close();
        }
    }

    /**
     * Gets the first element from the stream and extracts the UDF result.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #popUdfResult()}.</p>
     *
     * @return an Optional containing the UDF result, or empty if the stream is empty
     * @throws AerospikeException if the UDF invocation failed
     */
    public Optional<Object> getFirstUdfResult() {
        try {
            return popUdfResult();
        } finally {
            close();
        }
    }

    /**
     * Gets the first element from the stream and extracts the UDF result, mapped to an object
     * using the provided mapper. The UDF must return a map.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #popUdfResult(RecordMapper)}.</p>
     *
     * @param <T> the target type
     * @param mapper the mapper to convert the UDF result map to the target type
     * @return an Optional containing the mapped UDF result, or empty if the stream is empty
     * @throws AerospikeException with ResultCode = OP_NOT_APPLICABLE if the UDF return value is not a map
     */
    public <T> Optional<T> getFirstUdfResult(RecordMapper<T> mapper) {
        try {
            return popUdfResult(mapper);
        } finally {
            close();
        }
    }

    /**
     * Domain object plus Aerospike record metadata from {@link #popWithMetadata(RecordMapper)}
     * and {@link #getFirstWithMetadata(RecordMapper)}.
     *
     * @param <T> mapped entity type
     */
    public static class ObjectWithMetadata<T> {
        private final int generation;
        private final int expiration;
        private final T object;

        /**
         * @param object mapped value from the record bins
         * @param rec    source record (generation and expiration are copied)
         */
        public ObjectWithMetadata(T object, Record rec) {
            this.object = object;
            this.generation = rec.generation;
            this.expiration = rec.expiration;
        }

        /**
         * @return the mapped domain object
         */
        public T get() {
            return object;
        }

        /**
         * @return record TTL / expiration as returned by the server for this result
         */
        public int getExpiration() {
            return expiration;
        }

        /**
         * @return record generation (version) from the server
         */
        public int getGeneration() {
            return generation;
        }
    }

    /**
     * Gets the first element from the stream, maps it to an object using the provided mapper,
     * and returns it along with its metadata (generation and expiration).
     *
     * <p>This is a <b>terminal operation</b> that closes the stream after retrieving the
     * first element. For a non-closing variant that allows continued iteration, use
     * {@link #popWithMetadata(RecordMapper)}.</p>
     *
     * @param <T> the type of the object to be returned
     * @param mapper the mapper to use to convert the record to the class
     * @return an Optional containing an ObjectWithMetadata with the mapped object and its metadata,
     *         or empty if the stream is empty
     * @throws AerospikeException if the result code is not OK
     */
    public <T> Optional<ObjectWithMetadata<T>> getFirstWithMetadata(RecordMapper<T> mapper) {
        try {
            return popWithMetadata(mapper);
        } finally {
            close();
        }
    }


    /**
     * Closes this stream, releasing any underlying resources.
     * After closing, the stream should not be used further.
     *
     * <p>This method is idempotent -- calling it multiple times has no additional effect.
     * Terminal operations (such as {@link #getFirst()}, {@link #forEach(Consumer)}, etc.)
     * call this method automatically.</p>
     */
    @Override
    public void close() {
        if (impl != null) {
            impl.close();
        }
    }
}
