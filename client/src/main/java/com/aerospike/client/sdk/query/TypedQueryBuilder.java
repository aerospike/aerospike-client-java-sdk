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
package com.aerospike.client.sdk.query;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import com.aerospike.client.sdk.ErrorHandler;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.TypedRecordStream;
import com.aerospike.client.sdk.TypedDataSet;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.QueryDuration;

/**
 * Query builder for a {@link TypedDataSet}; delegates to {@link QueryBuilder} and wraps
 * {@link com.aerospike.client.sdk.RecordStream} results as {@link TypedRecordStream}.
 *
 * @param <T> entity type bound to the dataset
 */
public final class TypedQueryBuilder<T> {

    private final Session session;
    private final Class<T> entityClass;
    private final QueryBuilder delegate;

    public TypedQueryBuilder(Session session, Class<T> entityClass, QueryBuilder delegate) {
        this.session = Objects.requireNonNull(session, "session");
        this.entityClass = Objects.requireNonNull(entityClass, "entityClass");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    private TypedRecordStream<T> wrap(com.aerospike.client.sdk.RecordStream raw) {
        return new TypedRecordStream<>(session, entityClass, raw);
    }

    public QueryBuilderBinBuilder bin(String binName) {
        return delegate.bin(binName);
    }

    public TypedQueryBuilder<T> readingOnlyBins(String... binNames) {
        delegate.readingOnlyBins(binNames);
        return this;
    }

    public TypedQueryBuilder<T> withNoBins() {
        delegate.withNoBins();
        return this;
    }

    public TypedQueryBuilder<T> limit(long limit) {
        delegate.limit(limit);
        return this;
    }

    public TypedQueryBuilder<T> chunkSize(int chunkSize) {
        delegate.chunkSize(chunkSize);
        return this;
    }

    public TypedQueryBuilder<T> onPartition(int partId) {
        delegate.onPartition(partId);
        return this;
    }

    public TypedQueryBuilder<T> onPartitionRange(int startIncl, int endExcl) {
        delegate.onPartitionRange(startIncl, endExcl);
        return this;
    }

    public TypedQueryBuilder<T> failOnFilteredOut() {
        delegate.failOnFilteredOut();
        return this;
    }

    public TypedQueryBuilder<T> includeMissingKeys() {
        delegate.includeMissingKeys();
        return this;
    }

    public TypedQueryBuilder<T> recordsPerSecond(int recordsPerSecond) {
        delegate.recordsPerSecond(recordsPerSecond);
        return this;
    }

    public TypedQueryBuilder<T> withHint(Function<QueryHint.Start, ? extends QueryHint.Result> configurator) {
        delegate.withHint(configurator);
        return this;
    }

    public TypedQueryBuilder<T> where(String ael, Object... params) {
        delegate.where(ael, params);
        return this;
    }

    public TypedQueryBuilder<T> where(BooleanExpression ael) {
        delegate.where(ael);
        return this;
    }

    public TypedQueryBuilder<T> where(Expression expression) {
        delegate.where(expression);
        return this;
    }

    public TypedQueryBuilder<T> where(Exp exp) {
        delegate.where(exp);
        return this;
    }

    public TypedQueryBuilder<T> where(PreparedAel ael, Object... params) {
        delegate.where(ael, params);
        return this;
    }

    public TypedQueryBuilder<T> notInAnyTransaction() {
        delegate.notInAnyTransaction();
        return this;
    }

    public TypedQueryBuilder<T> inTransaction(Txn txn) {
        delegate.inTransaction(txn);
        return this;
    }

    public int getRecordsPerSecond() {
        return delegate.getRecordsPerSecond();
    }

    public QueryHint.Result getQueryHint() {
        return delegate.getQueryHint();
    }

    public QueryDuration getEffectiveQueryDuration() {
        return delegate.getEffectiveQueryDuration();
    }

    public String[] getBinNames() {
        return delegate.getBinNames();
    }

    public boolean getWithNoBins() {
        return delegate.getWithNoBins();
    }

    public long getLimit() {
        return delegate.getLimit();
    }

    public int getChunkSize() {
        return delegate.getChunkSize();
    }

    public int getStartPartition() {
        return delegate.getStartPartition();
    }

    public int getEndPartition() {
        return delegate.getEndPartition();
    }

    public java.util.List<com.aerospike.client.sdk.Operation> getOperations() {
        return delegate.getOperations();
    }

    public TypedRecordStream<T> execute() {
        return wrap(delegate.execute());
    }

    public TypedRecordStream<T> execute(ErrorStrategy strategy) {
        return wrap(delegate.execute(strategy));
    }

    public TypedRecordStream<T> execute(ErrorHandler handler) {
        return wrap(delegate.execute(handler));
    }

    public TypedRecordStream<T> executeAsync(ErrorStrategy strategy) {
        return wrap(delegate.executeAsync(strategy));
    }

    public TypedRecordStream<T> executeAsync(ErrorHandler handler) {
        return wrap(delegate.executeAsync(handler));
    }
}
