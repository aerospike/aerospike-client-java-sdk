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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A {@link RecordStream} of records for a single domain type {@code T}, using the session's
 * {@link RecordMappingFactory} for mapper-less materialization.
 *
 * <p>Implements {@link Iterable} for convenience; iteration is single-pass (same as the
 * underlying {@link RecordStream}).</p>
 *
 * <p>Factory-backed mapping (no {@link RecordMapper} argument from the caller) includes
 * {@link #toObjectList()}, {@link #forEachObject(Consumer)}, {@link #getFirstObject()},
 * {@link #popObject()}, {@link #getObject(Key)}, {@link #getFirstWithMetadata()}, and
 * {@link #popWithMetadata()}.</p>
 *
 * <p>Explicit {@link RecordMapper} overloads (e.g. {@link #toObjectList(RecordMapper)}) use the same
 * {@link RecordReadContext} as factory-backed reads so {@link RecordMapper#fromMap(java.util.Map, Key, int, RecordReadContext)}
 * receives the {@link Session}.</p>
 *
 * @param <T> entity type
 */
public final class TypedRecordStream<T> implements Iterator<RecordResult>, Iterable<RecordResult>, Closeable {

    private final Session session;
    private final Class<T> entityClass;
    private final RecordStream delegate;

    /**
     * Wraps an untyped {@link RecordStream} with typed mapping for {@code entityClass}.
     *
     * <p>Typically created by {@link TypedQueryBuilder#execute()} and related typed builder
     * methods rather than called directly.</p>
     *
     * @param session the session that owns mapping configuration
     * @param entityClass the domain type to map records into
     * @param delegate the underlying record stream
     */
    public TypedRecordStream(Session session, Class<T> entityClass, RecordStream delegate) {
        this.session = Objects.requireNonNull(session, "session");
        this.entityClass = Objects.requireNonNull(entityClass, "entityClass");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    private RecordMapper<T> requireMapper() {
        return MappingSupport.requireMapper(session.getRecordMappingFactory(), entityClass);
    }

    private RecordReadContext<T> mappingContext() {
        return new RecordReadContext<>(session, entityClass);
    }

    private T mapOk(RecordResult rr) {
        Record rec = rr.recordOrThrow();
        return requireMapper().fromMap(rec.bins, rr.getKey(), rec.generation, mappingContext());
    }

    /**
     * Returns an iterator over the underlying {@link RecordResult} elements.
     *
     * <p>Iteration is single-pass; the stream is consumed as the iterator advances.</p>
     *
     * @return this stream as an {@link Iterator} of {@link RecordResult}
     */
    @Override
    public Iterator<RecordResult> iterator() {
        return this;
    }

    /**
     * Returns the wrapped untyped {@link RecordStream}.
     *
     * <p>Use when you need untyped APIs (for example {@link RecordStream#asCompletableFuture(ErrorHandler)})
     * or to pass results to code that does not know {@code T}.</p>
     *
     * <pre>
     * RecordStream untyped = typedStream.asUntypedRecordStream();
     * untyped.asCompletableFuture(handler).thenAccept(results -&gt; { ... });
     * </pre>
     *
     * @return the delegate stream (same instance for the life of this wrapper)
     */
    public RecordStream asUntypedRecordStream() {
        return delegate;
    }

    /**
     * Checks whether more server-side chunks are available.
     *
     * <p>Delegates to {@link RecordStream#hasMoreChunks()}. For client-side sorting and pagination,
     * use {@link #asNavigatableStream()} instead.</p>
     *
     * <p>This method does <b>not</b> close the stream.</p>
     *
     * @return {@code true} if another chunk can be loaded from the server. This method should always return {@code true} on the first call.
     */
    public boolean hasMoreChunks() {
        return delegate.hasMoreChunks();
    }

    /**
     * Returns {@code true} if the iteration has more {@link RecordResult} elements.
     *
     * <p>Delegates to {@link RecordStream#hasNext()}. Repeated calls return the same answer until
     * {@link #next()} advances the stream.</p>
     *
     * @return {@code true} if {@link #next()} would return an element
     */
    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    /**
     * Returns the next {@link RecordResult} in the iteration.
     *
     * <p>Delegates to {@link RecordStream#next()}. Does not map to {@code T}; use
     * {@link #popObject()} or {@link #next()} followed by manual mapping when you need domain objects.</p>
     *
     * @return the next record result
     */
    @Override
    public RecordResult next() {
        return delegate.next();
    }

    /**
     * Converts this stream into a Java {@link Stream} of {@link RecordResult}.
     *
     * <p>Delegates to {@link RecordStream#stream()}. Closing the returned Java {@link Stream}
     * closes this typed stream.</p>
     *
     * <pre>
     * try (Stream&lt;RecordResult&gt; s = typedStream.stream()) {
     *     s.forEach(rr -&gt; System.out.println(rr.getKey()));
     * }
     * </pre>
     *
     * @return a {@link Stream} backed by this typed stream
     */
    public Stream<RecordResult> stream() {
        return delegate.stream();
    }

    /**
     * Drains this stream into a {@link CompletableFuture} of untyped {@link RecordResult} values.
     *
     * <p>Delegates to {@link RecordStream#asCompletableFuture()}. This is a <b>terminal operation</b>
     * that closes the stream when draining completes. For mapped domain objects, prefer
     * {@link #asCompletableFutureMapped()}.</p>
     *
     * <pre>
     * CompletableFuture&lt;List&lt;RecordResult&gt;&gt; future =
     *     session.query(customerDataSet.ids(1, 2)).executeAsync(ErrorStrategy.IN_STREAM)
     *         .asCompletableFuture();
     * </pre>
     *
     * @return a future that completes with all results from this stream
     */
    public CompletableFuture<List<RecordResult>> asCompletableFuture() {
        return delegate.asCompletableFuture();
    }

    /**
     * Drains this stream and maps each successful record to {@code T} using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when draining completes
     * or an exception occurs. Records with non-OK result codes cause the future to complete
     * exceptionally.</p>
     *
     * <pre>
     * CompletableFuture&lt;List&lt;Customer&gt;&gt; future =
     *     session.query(customerDataSet.ids(1, 2, 3)).executeAsync(ErrorStrategy.IN_STREAM)
     *         .asCompletableFutureMapped();
     * </pre>
     *
     * @return a future that completes with mapped domain objects
     */
    public CompletableFuture<List<T>> asCompletableFutureMapped() {
        return asCompletableFuture().thenApply(this::mapResultList);
    }

    /**
     * Drains this stream and maps each successful record with an explicit {@link RecordMapper}.
     *
     * <p>Unlike {@link #asCompletableFutureMapped()}, the mapper receives a {@link RecordReadContext}
     * so it can access the {@link Session} and {@link RecordMappingFactory} for dependent loads.</p>
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when draining completes
     * or an exception occurs.</p>
     *
     * <pre>
     * CompletableFuture&lt;List&lt;Customer&gt;&gt; future =
     *     session.query(customerDataSet.ids(1, 2)).executeAsync(ErrorStrategy.IN_STREAM)
     *         .asCompletableFuture(customerMapper);
     * </pre>
     *
     * @param mapper the mapper to convert each record (must not be null)
     * @return a future that completes with mapped domain objects
     */
    public CompletableFuture<List<T>> asCompletableFuture(RecordMapper<T> mapper) {
        RecordReadContext<T> ctx = mappingContext();
        return asCompletableFuture().thenApply(list -> {
            List<T> results = new ArrayList<>(list.size());
            for (RecordResult rr : list) {
                Record rec = rr.recordOrThrow();
                results.add(mapper.fromMap(rec.bins, rr.getKey(), rec.generation, ctx));
            }
            return results;
        });
    }

    /**
     * Drains a stream expected to hold zero or one successful result and maps it to {@code T}
     * using the session's {@link RecordMappingFactory}.
     *
     * <p>Same semantics as {@link RecordStream#asCompletableFutureSingle()}: empty if no records;
     * {@link IllegalStateException} if more than one result is present.</p>
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when draining completes
     * or an exception occurs.</p>
     *
     * <pre>
     * CompletableFuture&lt;Optional&lt;Customer&gt;&gt; future =
     *     session.query(customerDataSet.id(1)).executeAsync(ErrorStrategy.IN_STREAM)
     *         .asCompletableFutureMappedSingle();
     * </pre>
     *
     * @return a future completing with zero or one mapped object
     */
    public CompletableFuture<Optional<T>> asCompletableFutureMappedSingle() {
        return asCompletableFutureMapped().thenApply(AsyncExecutionSupport::singleMappedAsOptional);
    }

    private List<T> mapResultList(List<RecordResult> list) {
        List<T> results = new ArrayList<>(list.size());
        for (RecordResult rr : list) {
            results.add(mapOk(rr));
        }
        return results;
    }

    /**
     * Adapts this stream into a {@link Flow.Publisher} of {@link RecordResult} for reactive consumption.
     *
     * <p>Delegates to {@link RecordStream#asPublisher()}. Ideal for large or unbounded result sets
     * where collecting everything via {@link #asCompletableFutureMapped()} is not practical.
     * Map to {@code T} in the subscriber or use iteration APIs for typed consumption.</p>
     *
     * <pre>
     * session.query(customerDataSet).where("age &gt; 21").executeAsync(ErrorStrategy.IN_STREAM)
     *     .asPublisher()
     *     .subscribe(new Flow.Subscriber&lt;&gt;() { ... });
     * </pre>
     *
     * @return a unicast publisher backed by this stream
     */
    public Flow.Publisher<RecordResult> asPublisher() {
        return delegate.asPublisher();
    }

    /**
     * Extracts records whose result code is not {@link ResultCode#OK} into a new typed stream.
     *
     * <p>Delegates to {@link RecordStream#failures()} and re-wraps the result. This is a
     * <b>terminal operation</b> on the original stream. The returned stream holds failures
     * in memory; iterate it to inspect or map errors.</p>
     *
     * <pre>
     * TypedRecordStream&lt;Customer&gt; failures = results.failures();
     * failures.forEachObject(c -&gt; System.err.println("Unexpected: " + c));
     * </pre>
     *
     * @return a new typed stream containing only failed {@link RecordResult} entries
     */
    public TypedRecordStream<T> failures() {
        return new TypedRecordStream<>(session, entityClass, delegate.failures());
    }

    /**
     * Collects all remaining records into a list of {@code T} using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream. Records with non-OK result
     * codes cause an exception.</p>
     *
     * <pre>
     * List&lt;Customer&gt; customers = session.query(customerDataSet.ids(1, 2, 3))
     *     .execute()
     *     .toObjectList();
     * </pre>
     *
     * @return a list of mapped domain objects in stream order
     */
    public List<T> toObjectList() {
        try {
            List<T> result = new ArrayList<>();
            while (hasNext()) {
                result.add(mapOk(next()));
            }
            return result;
        } finally {
            close();
        }
    }

    /**
     * Collects all remaining records into a list of {@code T} using an explicit {@link RecordMapper}.
     *
     * <p>Passes the same {@link RecordReadContext} as {@link #toObjectList()} so the mapper can use
     * {@link Session} and {@link RecordReadContext#getRecordMappingFactory()} for dependent loads.</p>
     *
     * <p>This is a <b>terminal operation</b> that closes the stream.</p>
     *
     * <pre>
     * List&lt;Customer&gt; customers = session.query(customerDataSet.ids(1, 2))
     *     .execute()
     *     .toObjectList(customerMapper);
     * </pre>
     *
     * @param mapper the mapper to convert each record (must not be null)
     * @return a list of mapped domain objects in stream order
     */
    public List<T> toObjectList(RecordMapper<T> mapper) {
        try {
            List<T> result = new ArrayList<>();
            RecordReadContext<T> ctx = mappingContext();
            while (hasNext()) {
                RecordResult keyRecord = next();
                Record rec = keyRecord.recordOrThrow();
                result.add(mapper.fromMap(rec.bins, keyRecord.getKey(), rec.generation, ctx));
            }
            return result;
        } finally {
            close();
        }
    }

    /**
     * Loads all records into a {@link TypedNavigatableRecordStream} for client-side sorting and pagination.
     *
     * <p>Delegates to {@link RecordStream#asNavigatableStream()}. This is a <b>terminal operation</b>
     * on this stream. <b>Warning:</b> loads all records into memory; use
     * {@link #asNavigatableStream(long)} for large result sets.</p>
     *
     * <pre>
     * TypedNavigatableRecordStream&lt;Customer&gt; nav = session.query(customerDataSet)
     *     .execute()
     *     .asNavigatableStream()
     *     .pageSize(20)
     *     .sortBy(SortProperties.descending("age"));
     * </pre>
     *
     * @return a navigatable typed stream containing all records from this stream
     */
    public TypedNavigatableRecordStream<T> asNavigatableStream() {
        return new TypedNavigatableRecordStream<>(session, entityClass, delegate.asNavigatableStream());
    }

    /**
     * Loads up to {@code limit} records into a {@link TypedNavigatableRecordStream}.
     *
     * <p>Delegates to {@link RecordStream#asNavigatableStream(long)}. Non-positive limits are
     * treated as unbounded. This is a <b>terminal operation</b> on this stream.</p>
     *
     * <pre>
     * TypedNavigatableRecordStream&lt;Customer&gt; nav = session.query(customerDataSet)
     *     .execute()
     *     .asNavigatableStream(1000)
     *     .pageSize(20);
     * </pre>
     *
     * @param limit maximum number of records to load (0 or negative means no limit)
     * @return a navigatable typed stream containing up to {@code limit} records
     */
    public TypedNavigatableRecordStream<T> asNavigatableStream(long limit) {
        return new TypedNavigatableRecordStream<>(session, entityClass, delegate.asNavigatableStream(limit));
    }

    /**
     * Maps each successful record to {@code T} using the configured {@link RecordMappingFactory}
     * and passes it to the consumer.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream when iteration completes
     * or an exception occurs.</p>
     *
     * <pre>
     * session.query(customerDataSet.ids(1, 2)).execute()
     *     .forEachObject(customer -&gt; System.out.println(customer.getName()));
     * </pre>
     *
     * @param consumer the action to perform for each mapped object (must not be null)
     */
    public void forEachObject(Consumer<T> consumer) {
        try {
            while (hasNext()) {
                consumer.accept(mapOk(next()));
            }
        } finally {
            close();
        }
    }

    /**
     * Maps each successful record with an explicit {@link RecordMapper} and passes it to the consumer.
     *
     * <p>The mapper receives {@link RecordReadContext} for dependent loads. This is a
     * <b>terminal operation</b> that closes the stream.</p>
     *
     * <pre>
     * session.query(customerDataSet.ids(1, 2)).execute()
     *     .forEach(customerMapper, customer -&gt; System.out.println(customer.getName()));
     * </pre>
     *
     * @param mapper the mapper to convert each record (must not be null)
     * @param consumer the action to perform for each mapped object (must not be null)
     */
    public void forEach(RecordMapper<T> mapper, Consumer<T> consumer) {
        try {
            RecordReadContext<T> ctx = mappingContext();
            while (hasNext()) {
                RecordResult rr = next();
                Record rec = rr.recordOrThrow();
                consumer.accept(mapper.fromMap(rec.bins, rr.getKey(), rec.generation, ctx));
            }
        } finally {
            close();
        }
    }

    /**
     * Searches the stream for a record with the specified key and returns the raw {@link Record}.
     *
     * <p>Delegates to {@link RecordStream#get(Key)}. This is a <b>terminal operation</b> that
     * closes the stream after the search completes.</p>
     *
     * <pre>
     * Optional&lt;Record&gt; record = typedStream.get(customerKey);
     * </pre>
     *
     * @param key the key to search for (must not be null)
     * @return an {@link Optional} containing the record if found, or empty if not found
     */
    public Optional<Record> get(Key key) {
        return delegate.get(key);
    }

    /**
     * Searches the stream for a record with {@code key} and maps it using the session's
     * {@link RecordMappingFactory} for {@code T}.
     *
     * <p>Same mapping path as {@link #toObjectList()} (includes {@link RecordReadContext}).
     * This is a <b>terminal operation</b> that closes the stream.</p>
     *
     * <pre>
     * Optional&lt;Customer&gt; customer = session.query(customerDataSet.id(1))
     *     .execute()
     *     .getObject(customerDataSet.id(1).getKey());
     * </pre>
     *
     * @param key the key to search for (must not be null)
     * @return an {@link Optional} containing the mapped object if found, or empty if not found
     */
    public Optional<T> getObject(Key key) {
        try {
            while (hasNext()) {
                RecordResult thisRecord = next();
                if (thisRecord.getKey().equals(key)) {
                    return Optional.of(mapOk(thisRecord));
                }
            }
            return Optional.empty();
        } finally {
            close();
        }
    }

    /**
     * Searches the stream for a record with {@code key} and maps it with an explicit {@link RecordMapper}.
     *
     * <p>The mapper receives {@link RecordReadContext}. This is a <b>terminal operation</b> that
     * closes the stream after the search completes.</p>
     *
     * <pre>
     * Optional&lt;Customer&gt; customer = typedStream.get(customerKey, customerMapper);
     * </pre>
     *
     * @param key the key to search for (must not be null)
     * @param mapper the mapper to convert the record (must not be null)
     * @return an {@link Optional} containing the mapped object if found, or empty if not found
     */
    public Optional<T> get(Key key, RecordMapper<T> mapper) {
        try {
            RecordReadContext<T> ctx = mappingContext();
            while (hasNext()) {
                RecordResult thisRecord = next();
                if (thisRecord.getKey().equals(key)) {
                    Record rec = thisRecord.recordOrThrow();
                    return Optional.of(mapper.fromMap(rec.bins, thisRecord.getKey(), rec.generation, ctx));
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
     * Removes and returns the next {@link RecordResult} from the stream.
     *
     * <p>Delegates to {@link RecordStream#pop()}. Does <b>not</b> close the stream.
     * For a terminal first-element read, use {@link #getFirst()}.</p>
     *
     * @return an {@link Optional} containing the next result, or empty if the stream is exhausted
     */
    public Optional<RecordResult> pop() {
        return delegate.pop();
    }

    /**
     * Removes and returns the next {@link RecordResult} from the stream.
     *
     * <p>Delegates to {@link RecordStream#pop(boolean)}. Does <b>not</b> close the stream.
     * For a terminal first-element read, use {@link #getFirst(boolean)}.</p>
     *
     * @param throwException if {@code true}, non-OK result codes throw; if {@code false}, inspect
     *        {@link RecordResult#getResultCode()} on the returned value
     * @return an {@link Optional} containing the next result, or empty if the stream is exhausted
     */
    public Optional<RecordResult> pop(boolean throwException) {
        return delegate.pop(throwException);
    }

    /**
     * Removes and returns the next successful record mapped to {@code T} using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>Uses the same four-argument {@link RecordMapper#fromMap} path as {@link #toObjectList()}.
     * Does <b>not</b> close the stream. For a terminal first-element read, use {@link #getFirstObject()}.</p>
     *
     * <pre>
     * Optional&lt;Customer&gt; next = typedStream.popObject();
     * </pre>
     *
     * @return an {@link Optional} containing the mapped object, or empty if the stream is exhausted
     */
    public Optional<T> popObject() {
        return delegate.pop(true).map(this::mapOk);
    }

    /**
     * Removes and returns the next successful record mapped with an explicit {@link RecordMapper}.
     *
     * <p>Does <b>not</b> close the stream. For a terminal first-element read, use
     * {@link #getFirst(RecordMapper)}.</p>
     *
     * @param mapper the mapper to convert the record (must not be null)
     * @return an {@link Optional} containing the mapped object, or empty if the stream is exhausted
     */
    public Optional<T> pop(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            return Optional.of(mapper.fromMap(rec.bins, item.getKey(), rec.generation, mappingContext()));
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the next raw {@link Record}, or {@code null} if the stream is exhausted.
     *
     * <p>Delegates to {@link RecordStream#popRecord()}. Does <b>not</b> close the stream.
     * For a terminal first-element read, use {@link #getFirstRecord()}.</p>
     *
     * @return the next record, or {@code null} if the stream is exhausted
     */
    public Record popRecord() {
        return delegate.popRecord();
    }

    /**
     * Removes and returns the next element as a {@link Boolean}.
     *
     * <p>Delegates to {@link RecordStream#popBoolean()}. Does <b>not</b> close the stream.
     * For a terminal first-element read, use {@link #getFirstBoolean()}.</p>
     *
     * @return an {@link Optional} containing the boolean value, or empty if the stream is exhausted
     */
    public Optional<Boolean> popBoolean() {
        return delegate.popBoolean();
    }

    /**
     * Removes and returns the UDF result object from the next element.
     *
     * <p>Delegates to {@link RecordStream#popUdfResultObject()}. Does <b>not</b> close the stream.
     * For a terminal first-element read, use {@link #getFirstUdfResultObject()}.</p>
     *
     * @return an {@link Optional} containing the UDF result, or empty if the stream is exhausted
     */
    public Optional<Object> popUdfResultObject() {
        return delegate.popUdfResultObject();
    }

    /**
     * Removes and returns the UDF result from the next element, mapped with an explicit {@link RecordMapper}.
     *
     * <p>The UDF must return a map. Does <b>not</b> close the stream. For a terminal first-element read,
     * use {@link #getFirstUdfResultObject(RecordMapper)}.</p>
     *
     * @param mapper the mapper to convert the UDF result map (must not be null)
     * @return an {@link Optional} containing the mapped UDF result, or empty if exhausted or UDF returned null
     */
    public Optional<T> popUdfResultObject(RecordMapper<T> mapper) {
        if (hasNext()) {
            return next().udfResultAsObject(mapper, mappingContext());
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the UDF result from the next element, mapped to {@code T} using the
     * session's {@link RecordMappingFactory}.
     *
     * <p>Delegates mapping to {@link RecordResult#udfResultAsObject()}. Does <b>not</b> close the stream.
     * For a terminal first-element read, use {@link #getFirstUdfResultMapped()}.</p>
     *
     * <pre>
     * Optional&lt;Customer&gt; udfCustomer = typedStream.popUdfResultMapped();
     * </pre>
     *
     * @return an {@link Optional} containing the mapped UDF result, or empty if the stream is exhausted
     */
    public Optional<T> popUdfResultMapped() {
        if (hasNext()) {
            return next().udfResultAsObject();
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the next record mapped to {@code T} along with record metadata.
     *
     * <p>Does <b>not</b> close the stream. For a terminal first-element read, use
     * {@link #getFirstWithMetadata(RecordMapper)}.</p>
     *
     * @param mapper the mapper to convert the record (must not be null)
     * @return an {@link Optional} containing object and metadata, or empty if the stream is exhausted
     */
    public Optional<RecordStream.ObjectWithMetadata<T>> popWithMetadata(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            T object = mapper.fromMap(rec.bins, item.getKey(), rec.generation, mappingContext());
            return Optional.of(new RecordStream.ObjectWithMetadata<>(object, rec));
        }
        return Optional.empty();
    }

    /**
     * Removes and returns the next record mapped to {@code T} with metadata using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>Passes {@link RecordReadContext} into {@link RecordMapper#fromMap}. Does <b>not</b> close
     * the stream. For a terminal first-element read, use {@link #getFirstWithMetadata()}.</p>
     *
     * <pre>
     * Optional&lt;RecordStream.ObjectWithMetadata&lt;Customer&gt;&gt; row = typedStream.popWithMetadata();
     * </pre>
     *
     * @return an {@link Optional} containing object and metadata, or empty if the stream is exhausted
     */
    public Optional<RecordStream.ObjectWithMetadata<T>> popWithMetadata() {
        Optional<RecordResult> rr = delegate.pop(true);
        if (rr.isEmpty()) {
            return Optional.empty();
        }
        RecordResult item = rr.get();
        Record rec = item.recordOrThrow();
        T object = requireMapper().fromMap(rec.bins, item.getKey(), rec.generation, mappingContext());
        return Optional.of(new RecordStream.ObjectWithMetadata<>(object, rec));
    }

    // ========================================
    // GetFirst methods (terminal, auto-close)
    // ========================================

    /**
     * Gets the first {@link RecordResult} from the stream.
     *
     * <p>Delegates to {@link RecordStream#getFirst()}. This is a <b>terminal operation</b> that
     * closes the stream. For continued iteration, use {@link #pop()}.</p>
     *
     * @return an {@link Optional} containing the first result, or empty if the stream is empty
     */
    public Optional<RecordResult> getFirst() {
        return delegate.getFirst();
    }

    /**
     * Gets the first {@link RecordResult} from the stream.
     *
     * <p>Delegates to {@link RecordStream#getFirst(boolean)}. This is a <b>terminal operation</b>
     * that closes the stream.</p>
     *
     * @param throwException if {@code true}, non-OK result codes throw; if {@code false}, inspect
     *        {@link RecordResult#getResultCode()} on the returned value
     * @return an {@link Optional} containing the first result, or empty if the stream is empty
     */
    public Optional<RecordResult> getFirst(boolean throwException) {
        return delegate.getFirst(throwException);
    }

    /**
     * Gets the first successful record mapped to {@code T} using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>Same mapping path as {@link #toObjectList()} (includes {@link RecordReadContext}).
     * This is a <b>terminal operation</b> that closes the stream.</p>
     *
     * <pre>
     * Optional&lt;Customer&gt; customer = session.query(customerDataSet.id(1))
     *     .execute()
     *     .getFirstObject();
     * </pre>
     *
     * @return an {@link Optional} containing the first mapped object, or empty if the stream is empty
     */
    public Optional<T> getFirstObject() {
        try {
            return popObject();
        } finally {
            close();
        }
    }

    /**
     * Gets the first record mapped to {@code T} with metadata using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>Terminal variant of {@link #popWithMetadata()}. Closes the stream after reading the
     * first element.</p>
     *
     * <pre>
     * Optional&lt;RecordStream.ObjectWithMetadata&lt;Customer&gt;&gt; row =
     *     typedStream.getFirstWithMetadata();
     * </pre>
     *
     * @return an {@link Optional} containing object and metadata, or empty if the stream is empty
     */
    public Optional<RecordStream.ObjectWithMetadata<T>> getFirstWithMetadata() {
        try {
            return popWithMetadata();
        } finally {
            close();
        }
    }

    /**
     * Gets the first successful record mapped with an explicit {@link RecordMapper}.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream. For continued iteration,
     * use {@link #pop(RecordMapper)}.</p>
     *
     * @param mapper the mapper to convert the record (must not be null)
     * @return an {@link Optional} containing the first mapped object, or empty if the stream is empty
     */
    public Optional<T> getFirst(RecordMapper<T> mapper) {
        try {
            return pop(mapper);
        } finally {
            close();
        }
    }

    /**
     * Gets the first raw {@link Record}, or {@code null} if the stream is empty.
     *
     * <p>Delegates to {@link RecordStream#getFirstRecord()}. This is a <b>terminal operation</b>
     * that closes the stream.</p>
     *
     * @return the first record, or {@code null} if the stream is empty
     */
    public Record getFirstRecord() {
        return delegate.getFirstRecord();
    }

    /**
     * Gets the first element as a {@link Boolean}.
     *
     * <p>Delegates to {@link RecordStream#getFirstBoolean()}. This is a <b>terminal operation</b>
     * that closes the stream.</p>
     *
     * @return an {@link Optional} containing the boolean value, or empty if the stream is empty
     */
    public Optional<Boolean> getFirstBoolean() {
        return delegate.getFirstBoolean();
    }

    /**
     * Gets the UDF result from the first element.
     *
     * <p>Delegates to {@link RecordStream#getFirstUdfResultObject()}. This is a <b>terminal operation</b>
     * that closes the stream.</p>
     *
     * @return an {@link Optional} containing the UDF result, or empty if the stream is empty
     */
    public Optional<Object> getFirstUdfResultObject() {
        return delegate.getFirstUdfResultObject();
    }

    /**
     * Gets the UDF result from the first element, mapped with an explicit {@link RecordMapper}.
     *
     * <p>The UDF must return a map. This is a <b>terminal operation</b> that closes the stream.</p>
     *
     * @param mapper the mapper to convert the UDF result map (must not be null)
     * @return an {@link Optional} containing the mapped UDF result, or empty if the stream is empty
     */
    public Optional<T> getFirstUdfResultObject(RecordMapper<T> mapper) {
        try {
            return popUdfResultObject(mapper);
        } finally {
            close();
        }
    }

    /**
     * Gets the UDF result from the first element, mapped to {@code T} using the session's
     * {@link RecordMappingFactory}.
     *
     * <p>Terminal variant of {@link #popUdfResultMapped()}. Closes the stream after reading
     * the first element.</p>
     *
     * @return an {@link Optional} containing the mapped UDF result, or empty if the stream is empty
     */
    public Optional<T> getFirstUdfResultMapped() {
        try {
            return popUdfResultMapped();
        } finally {
            close();
        }
    }

    /**
     * Gets the first record mapped to {@code T} with metadata using an explicit {@link RecordMapper}.
     *
     * <p>This is a <b>terminal operation</b> that closes the stream.</p>
     *
     * @param mapper the mapper to convert the record (must not be null)
     * @return an {@link Optional} containing object and metadata, or empty if the stream is empty
     */
    public Optional<RecordStream.ObjectWithMetadata<T>> getFirstWithMetadata(RecordMapper<T> mapper) {
        try {
            return popWithMetadata(mapper);
        } finally {
            close();
        }
    }

    /**
     * Closes this stream, releasing any underlying resources.
     *
     * <p>Delegates to {@link RecordStream#close()}. Idempotent — safe to call multiple times.
     * Terminal operations call this automatically.</p>
     */
    @Override
    public void close() {
        delegate.close();
    }
}
