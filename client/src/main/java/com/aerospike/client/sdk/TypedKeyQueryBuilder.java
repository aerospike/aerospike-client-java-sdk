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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Fluent builder for a <strong>single-leg</strong> key read started from {@link Session#query(TypedKey)}.
 * Configures the underlying {@link ChainableQueryBuilder}; {@link #execute()} returns a
 * {@link TypedRecordStream} when the chain is still exactly one typed point read (one key, one query spec).
 * Chaining another verb (for example a second {@code query}, {@code upsert}, or {@code executeUdf}) widens
 * to {@link ChainableQueryBuilder} / {@link ChainableOperationBuilder} and yields a plain {@link RecordStream}
 * from those builders' {@code execute()}.
 *
 * @param <T> entity type carried by the initial {@link TypedKey}
 */
public final class TypedKeyQueryBuilder<T> implements FilterableOperation<TypedKeyQueryBuilder<T>> {

    private final Session session;
    private final Class<T> entityClass;
    private final ChainableQueryBuilder inner;

    TypedKeyQueryBuilder(Session session, Class<T> entityClass, ChainableQueryBuilder inner) {
        this.session = Objects.requireNonNull(session, "session");
        this.entityClass = Objects.requireNonNull(entityClass, "entityClass");
        this.inner = Objects.requireNonNull(inner, "inner");
    }

    /**
     * Underlying batch query builder; use when you need APIs not mirrored here or after the chain widened.
     */
    public ChainableQueryBuilder asChainableQueryBuilder() {
        return inner;
    }

    private TypedRecordStream<T> wrap(RecordStream raw) {
        return new TypedRecordStream<>(session, entityClass, raw);
    }

    private TypedRecordStream<T> executeTypedIfEligible(List<OperationSpec> specs, ErrorDisposition disposition) {
        if (!isSingleHomogeneousTypedKeyQueryLeg(specs, entityClass)) {
            if (specs.isEmpty()) {
                return wrap(new RecordStream());
            }
            throw new IllegalStateException(
                "This chain is not a single typed key read (one query spec, one key). "
                    + "Use the ChainableQueryBuilder returned from .query(...), .upsert(...), etc., "
                    + "or asChainableQueryBuilder().execute() for a RecordStream.");
        }
        return wrap(inner.executeWithPreparedSpecs(specs, disposition));
    }

    /**
     * True when {@code specs} is empty (no keys matched partition/limit filters) or exactly one query spec
     * for one key with a read-mapping class matching {@code entityClass}.
     */
    static <T> boolean isSingleHomogeneousTypedKeyQueryLeg(List<OperationSpec> specs, Class<T> entityClass) {
        if (specs.isEmpty()) {
            return true;
        }
        if (specs.size() != 1) {
            return false;
        }
        OperationSpec s = specs.get(0);
        if (!s.isQuery()) {
            return false;
        }
        if (s.getKeys().size() != 1) {
            return false;
        }
        Class<?> hint = s.getReadMappingClass();
        return hint != null && hint.equals(entityClass);
    }

    public TypedKeyQueryBuilder<T> readingOnlyBins(String... binNames) {
        inner.readingOnlyBins(binNames);
        return this;
    }

    public TypedKeyQueryBuilder<T> withNoBins() {
        inner.withNoBins();
        return this;
    }

    public QueryBinBuilder<TypedKeyQueryBuilder<T>> bin(String binName) {
        inner.verifyBinChainState("adding bin operation");
        return new QueryBinBuilder<>(inner, binName, cb -> this);
    }

    @Override
    public TypedKeyQueryBuilder<T> where(String ael, Object... params) {
        inner.where(ael, params);
        return this;
    }

    @Override
    public TypedKeyQueryBuilder<T> where(BooleanExpression ael) {
        inner.where(ael);
        return this;
    }

    @Override
    public TypedKeyQueryBuilder<T> where(PreparedAel ael, Object... params) {
        inner.where(ael, params);
        return this;
    }

    @Override
    public TypedKeyQueryBuilder<T> where(Exp exp) {
        inner.where(exp);
        return this;
    }

    @Override
    public TypedKeyQueryBuilder<T> where(Expression e) {
        inner.where(e);
        return this;
    }

    @Override
    public TypedKeyQueryBuilder<T> failOnFilteredOut() {
        inner.failOnFilteredOut();
        return this;
    }

    @Override
    public TypedKeyQueryBuilder<T> includeMissingKeys() {
        inner.includeMissingKeys();
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultWhere(String ael, Object... params) {
        inner.defaultWhere(ael, params);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultWhere(BooleanExpression ael) {
        inner.defaultWhere(ael);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultWhere(PreparedAel ael, Object... params) {
        inner.defaultWhere(ael, params);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultWhere(Exp exp) {
        inner.defaultWhere(exp);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultExpireRecordAfter(Duration duration) {
        inner.defaultExpireRecordAfter(duration);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultExpireRecordAfterSeconds(long seconds) {
        inner.defaultExpireRecordAfterSeconds(seconds);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultExpireRecordAt(LocalDateTime dateTime) {
        inner.defaultExpireRecordAt(dateTime);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultExpireRecordAt(Date date) {
        inner.defaultExpireRecordAt(date);
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultNeverExpire() {
        inner.defaultNeverExpire();
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultNoChangeInExpiration() {
        inner.defaultNoChangeInExpiration();
        return this;
    }

    public TypedKeyQueryBuilder<T> defaultExpiryFromServerDefault() {
        inner.defaultExpiryFromServerDefault();
        return this;
    }

    public TypedKeyQueryBuilder<T> limit(long limit) {
        inner.limit(limit);
        return this;
    }

    public TypedKeyQueryBuilder<T> onPartition(int partId) {
        inner.onPartition(partId);
        return this;
    }

    public TypedKeyQueryBuilder<T> onPartitionRange(int startIncl, int endExcl) {
        inner.onPartitionRange(startIncl, endExcl);
        return this;
    }

    public TypedKeyQueryBuilder<T> chunkSize(int chunkSize) {
        inner.chunkSize(chunkSize);
        return this;
    }

    public TypedKeyQueryBuilder<T> notInAnyTransaction() {
        inner.notInAnyTransaction();
        return this;
    }

    public TypedKeyQueryBuilder<T> inTransaction(Txn txn) {
        inner.inTransaction(txn);
        return this;
    }

    public ChainableQueryBuilder query(Key key) {
        return inner.query(key);
    }

    public ChainableQueryBuilder query(List<Key> keyList) {
        return inner.query(keyList);
    }

    public ChainableQueryBuilder query(Key key1, Key key2, Key... moreKeys) {
        return inner.query(key1, key2, moreKeys);
    }

    public <U> ChainableQueryBuilder query(TypedKey<U> typedKey) {
        return inner.query(typedKey);
    }

    public ChainableQueryBuilder query(TypedKey<?> k1, TypedKey<?> k2, TypedKey<?>... more) {
        return inner.query(k1, k2, more);
    }

    public ChainableQueryBuilder queryTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.queryTypedKeys(typedKeys);
    }

    public ChainableOperationBuilder upsert(Key key) {
        return inner.upsert(key);
    }

    public ChainableOperationBuilder upsert(List<Key> keys) {
        return inner.upsert(keys);
    }

    public ChainableOperationBuilder upsert(Key key1, Key key2, Key... moreKeys) {
        return inner.upsert(key1, key2, moreKeys);
    }

    public ChainableOperationBuilder upsert(TypedKey<?> typedKey) {
        return inner.upsert(typedKey.getKey());
    }

    public ChainableOperationBuilder upsertTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.upsertTypedKeys(typedKeys);
    }

    public ChainableOperationBuilder update(Key key) {
        return inner.update(key);
    }

    public ChainableOperationBuilder update(List<Key> keys) {
        return inner.update(keys);
    }

    public ChainableOperationBuilder update(Key key1, Key key2, Key... moreKeys) {
        return inner.update(key1, key2, moreKeys);
    }

    public ChainableOperationBuilder update(TypedKey<?> typedKey) {
        return inner.update(typedKey.getKey());
    }

    public ChainableOperationBuilder updateTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.updateTypedKeys(typedKeys);
    }

    public ChainableOperationBuilder insert(Key key) {
        return inner.insert(key);
    }

    public ChainableOperationBuilder insert(List<Key> keys) {
        return inner.insert(keys);
    }

    public ChainableOperationBuilder insert(Key key1, Key key2, Key... moreKeys) {
        return inner.insert(key1, key2, moreKeys);
    }

    public ChainableOperationBuilder insert(TypedKey<?> typedKey) {
        return inner.insert(typedKey.getKey());
    }

    public ChainableOperationBuilder insertTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.insertTypedKeys(typedKeys);
    }

    public ChainableOperationBuilder replace(Key key) {
        return inner.replace(key);
    }

    public ChainableOperationBuilder replace(List<Key> keys) {
        return inner.replace(keys);
    }

    public ChainableOperationBuilder replace(Key key1, Key key2, Key... moreKeys) {
        return inner.replace(key1, key2, moreKeys);
    }

    public ChainableOperationBuilder replace(TypedKey<?> typedKey) {
        return inner.replace(typedKey.getKey());
    }

    public ChainableOperationBuilder replaceTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.replaceTypedKeys(typedKeys);
    }

    public ChainableOperationBuilder replaceIfExists(Key key) {
        return inner.replaceIfExists(key);
    }

    public ChainableOperationBuilder replaceIfExists(List<Key> keys) {
        return inner.replaceIfExists(keys);
    }

    public ChainableOperationBuilder replaceIfExists(Key key1, Key key2, Key... moreKeys) {
        return inner.replaceIfExists(key1, key2, moreKeys);
    }

    public ChainableOperationBuilder replaceIfExists(TypedKey<?> typedKey) {
        return inner.replaceIfExists(typedKey.getKey());
    }

    public ChainableOperationBuilder replaceIfExistsTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.replaceIfExistsTypedKeys(typedKeys);
    }

    public ChainableNoBinsBuilder delete(Key key) {
        return inner.delete(key);
    }

    public ChainableNoBinsBuilder delete(TypedKey<?> typedKey) {
        return inner.delete(typedKey);
    }

    public ChainableNoBinsBuilder delete(List<Key> keys) {
        return inner.delete(keys);
    }

    public ChainableNoBinsBuilder delete(Key key1, Key key2, Key... moreKeys) {
        return inner.delete(key1, key2, moreKeys);
    }

    public ChainableNoBinsBuilder deleteTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.deleteTypedKeys(typedKeys);
    }

    public ChainableNoBinsBuilder touch(Key key) {
        return inner.touch(key);
    }

    public ChainableNoBinsBuilder touch(TypedKey<?> typedKey) {
        return inner.touch(typedKey);
    }

    public ChainableNoBinsBuilder touch(List<Key> keys) {
        return inner.touch(keys);
    }

    public ChainableNoBinsBuilder touch(Key key1, Key key2, Key... moreKeys) {
        return inner.touch(key1, key2, moreKeys);
    }

    public ChainableNoBinsBuilder touchTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.touchTypedKeys(typedKeys);
    }

    public ChainableNoBinsBuilder exists(Key key) {
        return inner.exists(key);
    }

    public ChainableNoBinsBuilder exists(TypedKey<?> typedKey) {
        return inner.exists(typedKey);
    }

    public ChainableNoBinsBuilder exists(List<Key> keys) {
        return inner.exists(keys);
    }

    public ChainableNoBinsBuilder exists(Key key1, Key key2, Key... moreKeys) {
        return inner.exists(key1, key2, moreKeys);
    }

    public ChainableNoBinsBuilder existsTypedKeys(List<? extends TypedKey<?>> typedKeys) {
        return inner.existsTypedKeys(typedKeys);
    }

    public UdfFunctionBuilder executeUdf(Key key) {
        return inner.executeUdf(key);
    }

    public UdfFunctionBuilder executeUdf(List<Key> keys) {
        return inner.executeUdf(keys);
    }

    public UdfFunctionBuilder executeUdf(Key key1, Key key2, Key... moreKeys) {
        return inner.executeUdf(key1, key2, moreKeys);
    }

    public TypedRecordStream<T> execute() {
        List<OperationSpec> specs = inner.prepareSpecsForExecute();
        return executeTypedIfEligible(specs, AbstractFilterableBuilder.defaultDisposition(specs));
    }

    public TypedRecordStream<T> execute(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        List<OperationSpec> specs = inner.prepareSpecsForExecute();
        return executeTypedIfEligible(specs, ErrorDisposition.fromStrategy(strategy));
    }

    public TypedRecordStream<T> execute(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        List<OperationSpec> specs = inner.prepareSpecsForExecute();
        return executeTypedIfEligible(specs, ErrorDisposition.handler(handler));
    }

    public TypedRecordStream<T> executeAsync(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        List<OperationSpec> specs = inner.prepareSpecsForExecute();
        if (!isSingleHomogeneousTypedKeyQueryLeg(specs, entityClass)) {
            if (specs.isEmpty()) {
                return wrap(new RecordStream());
            }
            throw new IllegalStateException(
                "This chain is not a single typed key read (one query spec, one key). "
                    + "Use the ChainableQueryBuilder returned from .query(...), .upsert(...), etc., "
                    + "or asChainableQueryBuilder().executeAsync(...) for a RecordStream.");
        }
        return wrap(inner.executeAsyncWithPreparedSpecs(specs, null));
    }

    public TypedRecordStream<T> executeAsync(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        List<OperationSpec> specs = inner.prepareSpecsForExecute();
        if (!isSingleHomogeneousTypedKeyQueryLeg(specs, entityClass)) {
            if (specs.isEmpty()) {
                return wrap(new RecordStream());
            }
            throw new IllegalStateException(
                "This chain is not a single typed key read (one query spec, one key). "
                    + "Use the ChainableQueryBuilder returned from .query(...), .upsert(...), etc., "
                    + "or asChainableQueryBuilder().executeAsync(...) for a RecordStream.");
        }
        return wrap(inner.executeAsyncWithPreparedSpecs(specs, handler));
    }
}
