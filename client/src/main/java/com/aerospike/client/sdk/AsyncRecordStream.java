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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.aerospike.client.sdk.query.RecordStreamImpl;

/**
 * A push-driven stream that supports backpressure and cancellation.
 *
 * <p>Supports passive drain into a {@link CompletableFuture} via {@link #asCompletableFuture()}
 * without starting a consumer virtual thread. Use {@link #withErrorHandler(ErrorHandler)} before
 * draining or publishing so actionable errors are routed at publish time. Drain registration is
 * single-pass: once registered, iteration and drain modes are mutually exclusive.</p>
 */
public final class AsyncRecordStream implements AutoCloseable, Iterable<RecordResult>, RecordStreamImpl {
    private static final Object END = new Object();
    private static final class Err { final Throwable t; Err(Throwable t){ this.t = t; } }

    private enum ConsumerMode { NONE, QUEUE, DRAIN }

    private final BlockingQueue<Object> queue;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean isFirstPage = new AtomicBoolean(true);
    private final AtomicReference<ConsumerMode> consumerMode = new AtomicReference<>(ConsumerMode.NONE);
    private final AtomicReference<DrainState> drainState = new AtomicReference<>();
    private volatile ErrorHandler streamErrorHandler;
    private Iterator<RecordResult> internalIterator = null;

    private static final class DrainState {
        final List<RecordResult> results = new ArrayList<>();
        final CompletableFuture<List<RecordResult>> future = new CompletableFuture<>();
        volatile boolean finalized;
    }

    // Optional: give producers a way to see if they should stop.
    private final BooleanSupplier cancelled = () -> closed.get() || completed.get();

    /**
     * @param capacity maximum number of {@link RecordResult} instances the queue holds before
     *                 {@link #publish} blocks (an extra slot is reserved for terminal markers)
     */
    public AsyncRecordStream(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        // Add one extra slot to reduce contention for END/Err marker.
        // The complete() and error() methods will retry until the marker is added,
        // but having extra capacity helps reduce wait time.
        this.queue = new ArrayBlockingQueue<>(capacity + 1);
    }

    /**
     * Routes actionable errors to the handler at publish time instead of enqueueing them.
     * Used by async query paths with {@link ErrorHandler} semantics.
     *
     * @param errorHandler handler for non-OK results (must not be null)
     * @return this stream for chaining
     */
    public AsyncRecordStream withErrorHandler(ErrorHandler errorHandler) {
        this.streamErrorHandler = errorHandler;
        return this;
    }

    /**
     * Registers passive collection into a {@link CompletableFuture}. Must be called before
     * iteration starts. When drain mode is active, published items are appended to an internal
     * list and are not enqueued (no backpressure blocking for CF-only consumption).
     *
     * <p>Publish-time error routing is controlled by {@link #withErrorHandler(ErrorHandler)}.
     * Completes when {@link #complete()} or {@link #error(Throwable)} is called.</p>
     *
     * @return future that completes with collected results
     * @throws IllegalStateException if iteration has already started or drain is already registered
     */
    public CompletableFuture<List<RecordResult>> asCompletableFuture() {
        if (!consumerMode.compareAndSet(ConsumerMode.NONE, ConsumerMode.DRAIN)) {
            if (consumerMode.get() == ConsumerMode.DRAIN) {
                DrainState existing = drainState.get();
                if (existing != null) {
                    return existing.future;
                }
            }
            throw new IllegalStateException(
                "Stream already consumed; drain and iteration are mutually exclusive");
        }
        DrainState state = new DrainState();
        drainState.set(state);
        drainQueuedResultsInto(state);
        if (completed.get()) {
            finalizeDrain(state, null);
        }
        return state.future;
    }

    /**
     * Convenience for single-result streams after key-count validation at the builder.
     *
     * @return future completing with zero or one result
     */
    public CompletableFuture<Optional<RecordResult>> asCompletableFutureSingle() {
        return asCompletableFuture().thenApply(AsyncExecutionSupport::singleAsOptional);
    }

    private void drainQueuedResultsInto(DrainState state) {
        Object item;
        while ((item = queue.poll()) != null) {
            if (item instanceof RecordResult rr) {
                if (routePublish(rr)) {
                    synchronized (state.results) {
                        state.results.add(rr);
                    }
                }
            } else if (item instanceof Err e) {
                finalizeDrainExceptionally(state, e.t);
                return;
            } else if (item == END) {
                completed.set(true);
                break;
            }
        }
    }

    private Iterator<RecordResult> getIterator() {
        if (!consumerMode.compareAndSet(ConsumerMode.NONE, ConsumerMode.QUEUE)) {
            if (consumerMode.get() != ConsumerMode.QUEUE) {
                throw new IllegalStateException(
                    "Stream already consumed; drain and iteration are mutually exclusive");
            }
        }
        if (internalIterator == null) {
            internalIterator = iterator();
        }
        return internalIterator;
    }

    private boolean routePublish(RecordResult result) {
        if (streamErrorHandler != null && AbstractFilterableBuilder.isActionableError(result.resultCode())) {
            AbstractFilterableBuilder.dispatchError(result, streamErrorHandler);
            return false;
        }
        return true;
    }

    /** For producers: push a result if we are still open. Blocks when backpressure applies. */
    public void publish(RecordResult result) {
        if (result == null) {
            return;
        }
        if (cancelled.getAsBoolean()) {
            return;
        }

        DrainState drain = drainState.get();
        if (drain != null) {
            if (routePublish(result)) {
                synchronized (drain.results) {
                    drain.results.add(result);
                }
            }
            return;
        }

        if (!routePublish(result)) {
            return;
        }
        // Block with backpressure, but wake up promptly if closed/completed
        while (true) {
            if (cancelled.getAsBoolean()) {
                return;
            }
            try {
                if (queue.offer(result, 50, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * For producers: signal a terminal error. The consumer will receive this as a runtime exception
     * when iterating.
     *
     * <p>This method sets the completed flag immediately, which stops any concurrent publishers
     * from adding more items. It then blocks until the error marker can be added to the queue.
     * If the queue is full, this method will block indefinitely until the consumer drains
     * enough items to make room, or until the stream is closed or the thread is interrupted.</p>
     *
     * <p>Safe to call multiple times - only the first call has any effect.</p>
     *
     * @param t the error to propagate to the consumer (if null, a generic RuntimeException is used)
     */
    public void error(Throwable t) {
        if (t == null) {
            t = new RuntimeException("Unknown error");
        }
        if (completed.compareAndSet(false, true)) {
            // Stop publishers first (they check completed via cancelled())
            DrainState drain = drainState.get();
            if (drain != null) {
                finalizeDrainExceptionally(drain, t);
                return;
            }
            Err err = new Err(t);
            // Keep trying until error is added - consumer will eventually drain
            // since publish() stops adding items when completed=true
            while (!closed.get()) {
                try {
                    if (queue.offer(err, 50, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * For producers: signal normal completion of the stream.
     *
     * <p>This method sets the completed flag immediately, which stops any concurrent publishers
     * from adding more items. It then blocks until the END marker can be added to the queue.
     * If the queue is full, this method will block indefinitely until the consumer drains
     * enough items to make room, or until the stream is closed or the thread is interrupted.</p>
     *
     * <p>Safe to call multiple times - only the first call has any effect.</p>
     *
     * @return this stream for method chaining
     */
    public AsyncRecordStream complete() {
        if (completed.compareAndSet(false, true)) {
            // Keep trying until END is added - consumer will eventually drain
            // since publish() stops adding items when completed=true
            DrainState drain = drainState.get();
            if (drain != null) {
                finalizeDrain(drain, null);
                return this;
            }
            while (!closed.get()) {
                try {
                    if (queue.offer(END, 50, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } else {
            DrainState drain = drainState.get();
            if (drain != null) {
                finalizeDrain(drain, null);
            }
        }
        return this;
    }

    private void finalizeDrain(DrainState drain, Throwable terminalError) {
        if (drain.finalized) {
            return;
        }
        drain.finalized = true;
        if (closed.get()) {
            drain.future.completeExceptionally(new CancellationException("RecordStream was closed"));
            return;
        }
        if (terminalError != null) {
            drain.future.completeExceptionally(terminalError);
            return;
        }
        List<RecordResult> snapshot;
        synchronized (drain.results) {
            snapshot = List.copyOf(drain.results);
        }
        drain.future.complete(snapshot);
    }

    private void finalizeDrainExceptionally(DrainState drain, Throwable t) {
        if (drain.finalized) {
            return;
        }
        drain.finalized = true;
        drain.future.completeExceptionally(t);
    }

    /** For consumers: a standard Java Stream view. Closing the stream cancels producers. */
    public Stream<RecordResult> stream() {
        return StreamSupport.stream(spliterator(), false).onClose(this::close);
    }

    /** A lightweight cancellation token for producers. */
    public BooleanSupplier cancelled() { return cancelled; }

    /** Cancel consumption and production early. Idempotent. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Drain quickly to keep memory bounded, then unblock consumer.
            DrainState drain = drainState.get();
            if (drain != null) {
                finalizeDrainExceptionally(drain, new CancellationException("RecordStream was closed"));
            }
            queue.clear();
            queue.offer(END);
        }
    }

    // --- Iterable / Spliterator plumbing so you can use for-each or stream() ---

    /**
     * {@inheritDoc}
     * <p>
     * Blocks on {@link BlockingQueue#take()} until each element arrives. Terminal {@link #error} is thrown as
     * an unchecked exception from {@link Iterator#next()}.
     */
    @Override
    public Iterator<RecordResult> iterator() {
        return new Iterator<>() {
            Object next = fetch();

            @Override public boolean hasNext() {
                return !(next == END || next instanceof Err);
            }

            @Override public RecordResult next() {
                if (next == END) {
                    throw new NoSuchElementException();
                }
                if (next instanceof Err e) {
                    // Propagate as unchecked
                    RuntimeException re = (e.t instanceof RuntimeException r) ? r : new RuntimeException(e.t);
                    // Advance to END so further calls behave
                    next = END;
                    throw re;
                }
                RecordResult rr = (RecordResult) next;
                next = fetch();
                return rr;
            }

            private Object fetch() {
                while (true) {
                    try {
                        Object o = queue.take();
                        if (o == END) {
                            // Ensure terminal state is visible to publishers
                            completed.set(true);
                            return END;
                        }
                        return o;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return END;
                    }
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     * <p>
     * Ordered, non-null elements; size unknown.
     */
    @Override
    public Spliterator<RecordResult> spliterator() {
        // Unknown size, ordered, non-null, concurrent-ish
        return Spliterators.spliteratorUnknownSize(iterator(),
                Spliterator.ORDERED | Spliterator.NONNULL);
    }

    // --- RecordStreamImpl methods ---

    /**
     * {@inheritDoc}
     * <p>
     * Returns {@code true} once (first chunk only), matching single-batch stream behavior.
     */
    @Override
    public boolean hasMoreChunks() {
        // Mirror SingleItemRecordStream behavior
        // Use compareAndSet for thread-safety
        return isFirstPage.compareAndSet(true, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return getIterator().hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordResult next() {
        return getIterator().next();
    }
}
