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
 * {@link RecordReadContext} as factory-backed reads so {@link RecordMapper#fromMap(Map, Key, int, RecordReadContext)}
 * receives the {@link Session}.</p>
 *
 * @param <T> entity type
 */
public final class TypedRecordStream<T> implements Iterator<RecordResult>, Iterable<RecordResult>, Closeable {

    private final Session session;
    private final Class<T> entityClass;
    private final RecordStream delegate;

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
        return requireMapper().fromMap(rec.bins, rr.key(), rec.generation, mappingContext());
    }

    @Override
    public Iterator<RecordResult> iterator() {
        return this;
    }

    public RecordStream asUntypedRecordStream() {
        return delegate;
    }

    public boolean hasMoreChunks() {
        return delegate.hasMoreChunks();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public RecordResult next() {
        return delegate.next();
    }

    public Stream<RecordResult> stream() {
        return delegate.stream();
    }

    public CompletableFuture<List<RecordResult>> asCompletableFuture() {
        return delegate.asCompletableFuture();
    }

    public CompletableFuture<List<T>> asCompletableFutureMapped() {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                List<T> results = new ArrayList<>();
                while (hasNext()) {
                    RecordResult rr = next();
                    Record rec = rr.recordOrThrow();
                    results.add(requireMapper().fromMap(rec.bins, rr.key(), rec.generation, mappingContext()));
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

    public CompletableFuture<List<T>> asCompletableFuture(RecordMapper<T> mapper) {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                List<T> results = new ArrayList<>();
                RecordReadContext<T> ctx = mappingContext();
                while (hasNext()) {
                    RecordResult rr = next();
                    Record rec = rr.recordOrThrow();
                    results.add(mapper.fromMap(rec.bins, rr.key(), rec.generation, ctx));
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

    public Flow.Publisher<RecordResult> asPublisher() {
        return delegate.asPublisher();
    }

    public TypedRecordStream<T> failures() {
        return new TypedRecordStream<>(session, entityClass, delegate.failures());
    }

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
     * Maps remaining records with an explicit {@link RecordMapper} for {@code T}, passing the same
     * {@link RecordReadContext} as {@link #toObjectList()} so the mapper can use {@link Session} and
     * {@link RecordReadContext#getRecordMappingFactory()} for dependent loads.
     */
    public List<T> toObjectList(RecordMapper<T> mapper) {
        try {
            List<T> result = new ArrayList<>();
            RecordReadContext<T> ctx = mappingContext();
            while (hasNext()) {
                RecordResult keyRecord = next();
                Record rec = keyRecord.recordOrThrow();
                result.add(mapper.fromMap(rec.bins, keyRecord.key(), rec.generation, ctx));
            }
            return result;
        } finally {
            close();
        }
    }

    public TypedNavigatableRecordStream<T> asNavigatableStream() {
        return new TypedNavigatableRecordStream<>(session, entityClass, delegate.asNavigatableStream());
    }

    public TypedNavigatableRecordStream<T> asNavigatableStream(long limit) {
        return new TypedNavigatableRecordStream<>(session, entityClass, delegate.asNavigatableStream(limit));
    }

    /**
     * Maps each successful record to {@code T} using the configured {@link RecordMappingFactory}.
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

    public void forEach(RecordMapper<T> mapper, Consumer<T> consumer) {
        try {
            RecordReadContext<T> ctx = mappingContext();
            while (hasNext()) {
                RecordResult rr = next();
                Record rec = rr.recordOrThrow();
                consumer.accept(mapper.fromMap(rec.bins, rr.key(), rec.generation, ctx));
            }
        } finally {
            close();
        }
    }

    public Optional<Record> get(Key key) {
        return delegate.get(key);
    }

    /**
     * Searches the stream for a record with {@code key} and maps it using the session's
     * {@link RecordMappingFactory} for {@code T} (same as {@link #toObjectList()} — no explicit
     * {@link RecordMapper} argument).
     *
     * <p>This is a <b>terminal operation</b> that closes the stream.</p>
     */
    public Optional<T> getObject(Key key) {
        try {
            while (hasNext()) {
                RecordResult thisRecord = next();
                if (thisRecord.key().equals(key)) {
                    return Optional.of(mapOk(thisRecord));
                }
            }
            return Optional.empty();
        } finally {
            close();
        }
    }

    public Optional<T> get(Key key, RecordMapper<T> mapper) {
        try {
            RecordReadContext<T> ctx = mappingContext();
            while (hasNext()) {
                RecordResult thisRecord = next();
                if (thisRecord.key().equals(key)) {
                    Record rec = thisRecord.recordOrThrow();
                    return Optional.of(mapper.fromMap(rec.bins, thisRecord.key(), rec.generation, ctx));
                }
            }
            return Optional.empty();
        } finally {
            close();
        }
    }

    public Optional<RecordResult> pop() {
        return delegate.pop();
    }

    public Optional<RecordResult> pop(boolean throwException) {
        return delegate.pop(throwException);
    }

    /**
     * Like {@link RecordStream#pop(RecordMapper)} but resolves the mapper from
     * {@link Session#getRecordMappingFactory()} for this stream's {@code T} and uses the same
     * four-argument {@link RecordMapper#fromMap} path as {@link #toObjectList()}.
     */
    public Optional<T> popObject() {
        return delegate.pop(true).map(this::mapOk);
    }

    public Optional<T> pop(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            return Optional.of(mapper.fromMap(rec.bins, item.key(), rec.generation, mappingContext()));
        }
        return Optional.empty();
    }

    public Record popRecord() {
        return delegate.popRecord();
    }

    public Optional<Boolean> popBoolean() {
        return delegate.popBoolean();
    }

    public Optional<Object> popUdfResult() {
        return delegate.popUdfResult();
    }

    public Optional<T> popUdfResult(RecordMapper<T> mapper) {
        if (hasNext()) {
            return Optional.ofNullable(next().udfResultAs(mapper, mappingContext()));
        }
        return Optional.empty();
    }

    public Optional<RecordStream.ObjectWithMetadata<T>> popWithMetadata(RecordMapper<T> mapper) {
        if (hasNext()) {
            RecordResult item = next();
            Record rec = item.recordOrThrow();
            T object = mapper.fromMap(rec.bins, item.key(), rec.generation, mappingContext());
            return Optional.of(new RecordStream.ObjectWithMetadata<>(object, rec));
        }
        return Optional.empty();
    }

    /**
     * Like {@link RecordStream#popWithMetadata(RecordMapper)} but uses the factory mapper for
     * {@code T} and passes {@link RecordReadContext} into {@link RecordMapper#fromMap}.
     */
    public Optional<RecordStream.ObjectWithMetadata<T>> popWithMetadata() {
        Optional<RecordResult> rr = delegate.pop(true);
        if (rr.isEmpty()) {
            return Optional.empty();
        }
        RecordResult item = rr.get();
        Record rec = item.recordOrThrow();
        T object = requireMapper().fromMap(rec.bins, item.key(), rec.generation, mappingContext());
        return Optional.of(new RecordStream.ObjectWithMetadata<>(object, rec));
    }

    public Optional<RecordResult> getFirst() {
        return delegate.getFirst();
    }

    public Optional<RecordResult> getFirst(boolean throwException) {
        return delegate.getFirst(throwException);
    }

    /**
     * First mapped domain object using {@link RecordMappingFactory} for {@code T} — no explicit
     * {@link RecordMapper}. Same mapping path as {@link #toObjectList()} (includes
     * {@link RecordReadContext}).
     *
     * <p>This is a <b>terminal operation</b> that closes the stream.</p>
     */
    public Optional<T> getFirstObject() {
        try {
            return popObject();
        } finally {
            close();
        }
    }

    /**
     * Terminal variant of {@link #popWithMetadata()} that closes the stream after the first element.
     */
    public Optional<RecordStream.ObjectWithMetadata<T>> getFirstWithMetadata() {
        try {
            return popWithMetadata();
        } finally {
            close();
        }
    }

    public Optional<T> getFirst(RecordMapper<T> mapper) {
        try {
            return pop(mapper);
        } finally {
            close();
        }
    }

    public Record getFirstRecord() {
        return delegate.getFirstRecord();
    }

    public Optional<Boolean> getFirstBoolean() {
        return delegate.getFirstBoolean();
    }

    public Optional<Object> getFirstUdfResult() {
        return delegate.getFirstUdfResult();
    }

    public Optional<T> getFirstUdfResult(RecordMapper<T> mapper) {
        try {
            return popUdfResult(mapper);
        } finally {
            close();
        }
    }

    public Optional<RecordStream.ObjectWithMetadata<T>> getFirstWithMetadata(RecordMapper<T> mapper) {
        try {
            return popWithMetadata(mapper);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        delegate.close();
    }
}
