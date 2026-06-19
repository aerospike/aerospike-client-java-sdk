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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.aerospike.client.sdk.query.ResettablePagination;
import com.aerospike.client.sdk.query.SortDir;
import com.aerospike.client.sdk.query.SortProperties;

/**
 * Client-side sorted/paginated view over a materialized {@link NavigatableRecordStream},
 * with factory-backed mapping for type {@code T}.
 *
 * @param <T> entity type
 */
public final class TypedNavigatableRecordStream<T> implements Iterator<RecordResult>, Iterable<RecordResult>, Closeable,
        ResettablePagination {

    private final Session session;
    private final Class<T> entityClass;
    private final NavigatableRecordStream delegate;

    public TypedNavigatableRecordStream(Session session, Class<T> entityClass, NavigatableRecordStream delegate) {
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

    public NavigatableRecordStream asUntypedNavigatableStream() {
        return delegate;
    }

    @Override
    public Iterator<RecordResult> iterator() {
        return this;
    }

    public Stream<RecordResult> stream() {
        return delegate.stream();
    }

    @Override
    public int currentPage() {
        return delegate.currentPage();
    }

    @Override
    public int maxPages() {
        return delegate.maxPages();
    }

    @Override
    public void setPageTo(int newPage) {
        delegate.setPageTo(newPage);
    }

    public TypedNavigatableRecordStream<T> pageSize(int pageSize) {
        delegate.pageSize(pageSize);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(String field) {
        delegate.sortBy(field);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(String field, boolean caseInsensitive) {
        delegate.sortBy(field, caseInsensitive);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(String field, SortDir sortDir) {
        delegate.sortBy(field, sortDir);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(String field, SortDir sortDir, boolean caseSensitive) {
        delegate.sortBy(field, sortDir, caseSensitive);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(List<SortProperties> sortPropertyList) {
        delegate.sortBy(sortPropertyList);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(SortProperties... sortPropertyList) {
        delegate.sortBy(sortPropertyList);
        return this;
    }

    public TypedNavigatableRecordStream<T> sortBy(SortProperties sortProperty) {
        delegate.sortBy(sortProperty);
        return this;
    }

    public boolean hasMorePages() {
        return delegate.hasMorePages();
    }

    public boolean hasNext() {
        return delegate.hasNext();
    }

    public RecordResult next() {
        return delegate.next();
    }

    public List<T> toObjectList() {
        List<T> result = new java.util.ArrayList<>();
        while (hasNext()) {
            result.add(mapOk(next()));
        }
        return result;
    }

    /**
     * Maps the current page using an explicit mapper for {@code T}, passing {@link RecordReadContext}
     * like {@link #toObjectList()}.
     */
    public List<T> toObjectList(RecordMapper<T> mapper) {
        List<T> result = new java.util.ArrayList<>();
        RecordReadContext<T> ctx = mappingContext();
        while (hasNext()) {
            RecordResult rr = next();
            Record rec = rr.recordOrThrow();
            result.add(mapper.fromMap(rec.bins, rr.key(), rec.generation, ctx));
        }
        return result;
    }

    public void forEachObject(Consumer<T> consumer) {
        while (hasNext()) {
            consumer.accept(mapOk(next()));
        }
    }

    public Optional<RecordResult> getFirst() throws AerospikeException {
        return delegate.getFirst();
    }

    public Optional<RecordResult> getFirst(boolean throwException) throws AerospikeException {
        return delegate.getFirst(throwException);
    }

    /**
     * First mapped domain object on the current page using {@link RecordMappingFactory} for
     * {@code T} (same four-argument {@link RecordMapper#fromMap} path as {@link #toObjectList()}).
     */
    public Optional<T> getFirstObject() {
        return delegate.getFirst().map(this::mapOk);
    }

    public Optional<T> getFirst(RecordMapper<T> mapper) {
        if (!hasNext()) {
            return Optional.empty();
        }
        RecordResult item = next();
        Record rec = item.recordOrThrow();
        return Optional.of(mapper.fromMap(rec.bins, item.key(), rec.generation, mappingContext()));
    }

    public int size() {
        return delegate.size();
    }

    public TypedNavigatableRecordStream<T> reset() {
        delegate.reset();
        return this;
    }

    @Override
    public void close() {
        delegate.close();
    }
}
