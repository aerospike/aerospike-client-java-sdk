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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.query.WhereClauseProcessor;

/**
 * Builder for chainable batch operations that do NOT support bin-level modifications.
 * This builder is used for {@code delete}, {@code touch}, and {@code exists} operations.
 *
 * <p>Unlike {@link ChainableOperationBuilder}, this class does not have a {@code bin()} method
 * since these operations don't modify bin values. However, they can still set expiration,
 * generation checks, and filter clauses.
 *
 * <p>Example usage:
 * <pre>{@code
 * session.delete(users.ids("user-1", "user-2"))
 *     .where("$.status == 'inactive'")
 *     .touch(users.id("user-3"))
 *     .expireRecordAfter(Duration.ofDays(30))
 *     .upsert(users.id("user-4"))
 *     .bin("status").setTo("active")
 *     .execute();
 * }</pre>
 *
 * @see ChainableOperationBuilder for operations with bin modifications
 * @see ChainableQueryBuilder for read operations
 */
public class ChainableNoBinsBuilder extends AbstractSessionOperationBuilder<ChainableNoBinsBuilder>
        implements FilterableOperation<ChainableNoBinsBuilder> {

    private final Session session;
    private final List<OperationSpec> operationSpecs;
    private OperationSpec currentSpec = null;
    private Expression defaultWhereClause;
    private long defaultExpirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;

    /**
     * Package-private constructor.
     */
    ChainableNoBinsBuilder(Session session, List<OperationSpec> existingSpecs,
                           Expression defaultWhereClause, long defaultExpirationInSeconds, Txn txnToUse) {
        super(session, null);  // opType will be set per operation
        this.session = session;
        this.operationSpecs = existingSpecs;
        this.defaultWhereClause = defaultWhereClause;
        this.defaultExpirationInSeconds = defaultExpirationInSeconds;
        this.txnToUse = txnToUse;
    }

    // ========================================
    // Initialization methods
    // ========================================

    ChainableNoBinsBuilder initDelete(Key key) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(List.of(key), OpType.DELETE);
        return this;
    }

    ChainableNoBinsBuilder initDelete(List<Key> keys) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(keys, OpType.DELETE);
        return this;
    }

    ChainableNoBinsBuilder initTouch(Key key) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(List.of(key), OpType.TOUCH);
        return this;
    }

    ChainableNoBinsBuilder initTouch(List<Key> keys) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(keys, OpType.TOUCH);
        return this;
    }

    ChainableNoBinsBuilder initExists(Key key) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(List.of(key), OpType.EXISTS);
        return this;
    }

    ChainableNoBinsBuilder initExists(List<Key> keys) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(keys, OpType.EXISTS);
        return this;
    }

    // ========================================
    // Chainable operation methods
    // ========================================

    /**
     * Chain an upsert operation on a single key.
     * Returns a {@link ChainableOperationBuilder} since upsert operations support bin modifications.
     *
     * @param key the key to upsert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder upsert(Key key) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.UPSERT, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(key, OpType.UPSERT);
    }

    /**
     * Chain an upsert operation on multiple keys.
     *
     * @param keys the keys to upsert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder upsert(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.UPSERT, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(keys, OpType.UPSERT);
    }

    /**
     * Chain an upsert operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder upsert(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return upsert(keys);
    }

    /**
     * Chain an update operation on a single key.
     * Returns a {@link ChainableOperationBuilder} since update operations support bin modifications.
     *
     * @param key the key to update
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder update(Key key) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.UPDATE, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(key, OpType.UPDATE);
    }

    /**
     * Chain an update operation on multiple keys.
     *
     * @param keys the keys to update
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder update(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.UPDATE, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(keys, OpType.UPDATE);
    }

    /**
     * Chain an update operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder update(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return update(keys);
    }

    /**
     * Chain an insert operation on a single key.
     * Returns a {@link ChainableOperationBuilder} since insert operations support bin modifications.
     *
     * @param key the key to insert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder insert(Key key) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.INSERT, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(key, OpType.INSERT);
    }

    /**
     * Chain an insert operation on multiple keys.
     *
     * @param keys the keys to insert
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder insert(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.INSERT, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(keys, OpType.INSERT);
    }

    /**
     * Chain an insert operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder insert(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return insert(keys);
    }

    /**
     * Chain a replace operation on a single key.
     * Returns a {@link ChainableOperationBuilder} since replace operations support bin modifications.
     *
     * @param key the key to replace
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replace(Key key) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.REPLACE, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(key, OpType.REPLACE);
    }

    /**
     * Chain a replace operation on multiple keys.
     *
     * @param keys the keys to replace
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replace(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.REPLACE, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(keys, OpType.REPLACE);
    }

    /**
     * Chain a replace operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replace(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return replace(keys);
    }

    /**
     * Chain a replaceIfExists operation on a single key.
     * Returns a {@link ChainableOperationBuilder} since replaceIfExists operations support bin modifications.
     * The operation will fail if the record does not exist.
     *
     * @param key the key to replace
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replaceIfExists(Key key) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.REPLACE_IF_EXISTS, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(key, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Chain a replaceIfExists operation on multiple keys.
     * The operation will fail for any record that does not exist.
     *
     * @param keys the keys to replace
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replaceIfExists(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableOperationBuilder(session, OpType.REPLACE_IF_EXISTS, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .init(keys, OpType.REPLACE_IF_EXISTS);
    }

    /**
     * Chain a replaceIfExists operation on multiple keys (varargs).
     * The operation will fail for any record that does not exist.
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableOperationBuilder for method chaining
     */
    public ChainableOperationBuilder replaceIfExists(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return replaceIfExists(keys);
    }

    /**
     * Chain a delete operation on a single key.
     *
     * @param key the key to delete
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder delete(Key key) {
        return initDelete(key);
    }

    /**
     * Chain a delete operation on multiple keys.
     *
     * @param keys the keys to delete
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder delete(List<Key> keys) {
        return initDelete(keys);
    }

    /**
     * Chain a delete operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder delete(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return delete(keys);
    }

    /**
     * Chain a touch operation on a single key.
     *
     * @param key the key to touch
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder touch(Key key) {
        return initTouch(key);
    }

    /**
     * Chain a touch operation on multiple keys.
     *
     * @param keys the keys to touch
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder touch(List<Key> keys) {
        return initTouch(keys);
    }

    /**
     * Chain a touch operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder touch(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return touch(keys);
    }

    /**
     * Chain an exists check operation on a single key.
     *
     * @param key the key to check
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder exists(Key key) {
        return initExists(key);
    }

    /**
     * Chain an exists check operation on multiple keys.
     *
     * @param keys the keys to check
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder exists(List<Key> keys) {
        return initExists(keys);
    }

    /**
     * Chain an exists check operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder exists(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return exists(keys);
    }

    /**
     * Chain a query (read) operation on a single key.
     * Returns a {@link ChainableQueryBuilder} for specifying which bins to read.
     *
     * @param key the key to query
     * @return ChainableQueryBuilder for method chaining
     */
    public ChainableQueryBuilder query(Key key) {
        finalizeCurrentOperation();
        return new ChainableQueryBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initQuery(key);
    }

    /**
     * Chain a query (read) operation on multiple keys.
     *
     * @param keys the keys to query
     * @return ChainableQueryBuilder for method chaining
     */
    public ChainableQueryBuilder query(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableQueryBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initQuery(keys);
    }

    /**
     * Chain a query (read) operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableQueryBuilder for method chaining
     */
    public ChainableQueryBuilder query(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return query(keys);
    }

    /**
     * Chain a UDF execution on a single key.
     * Returns a {@link UdfFunctionBuilder} requiring the UDF function to be specified.
     *
     * @param key the key to execute the UDF on
     * @return UdfFunctionBuilder requiring function specification
     */
    public UdfFunctionBuilder executeUdf(Key key) {
        finalizeCurrentOperation();
        return new UdfFunctionBuilder(session, List.of(key), operationSpecs,
                defaultWhereClause, defaultExpirationInSeconds, txnToUse);
    }

    /**
     * Chain a UDF execution on multiple keys.
     *
     * @param keys the keys to execute the UDF on
     * @return UdfFunctionBuilder requiring function specification
     */
    public UdfFunctionBuilder executeUdf(List<Key> keys) {
        finalizeCurrentOperation();
        return new UdfFunctionBuilder(session, keys, operationSpecs,
                defaultWhereClause, defaultExpirationInSeconds, txnToUse);
    }

    /**
     * Chain a UDF execution on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return UdfFunctionBuilder requiring function specification
     */
    public UdfFunctionBuilder executeUdf(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return executeUdf(keys);
    }

    // ========================================
    // Delete-specific methods
    // ========================================

    /**
     * Use durable delete for the current operation. This overrides all other
     * durable delete defaults.
     *
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder withDurableDelete() {
        verifyState("setting durable delete");

        // This check is wrong because records also can be deleted in a UDF call and
        // in a write where all bins are set to null.
        /*
        if (currentSpec.getOpType() != OpType.DELETE) {
            throw new IllegalStateException("withDurableDelete() can only be called on delete operations");
        }
        */
        currentSpec.setDurableDelete(true);
        return this;
    }

    /**
     * Do not use durable delete for the current operation. This overrides all other
     * durable delete defaults.
     *
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder withoutDurableDelete() {
        verifyState("setting durable delete");
        currentSpec.setDurableDelete(false);
        return this;
    }

    // ========================================
    // Per-operation policies
    // ========================================

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder expireRecordAfter(Duration duration) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(duration.toSeconds());
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(expirationInSeconds);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder expireRecordAt(Date date) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(getExpirationInSecondsAndCheckValue(date));
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder expireRecordAt(LocalDateTime date) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(getExpirationInSecondsAndCheckValue(date));
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder withNoChangeInExpiration() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_NO_CHANGE);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder neverExpire() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_NEVER_EXPIRE);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder expiryFromServerDefault() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_SERVER_DEFAULT);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder ensureGenerationIs(int generation) {
        verifyState("setting generation");
        if (generation <= 0) {
            throw new IllegalArgumentException("Generation must be greater than 0");
        }
        currentSpec.setGeneration(generation);
        return this;
    }

    // ========================================
    // FilterableOperation implementation
    // ========================================

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder where(String ael, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = createWhereClauseProcessor(false, ael, params);
        if (processor != null) {
            ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
            currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder where(BooleanExpression ael) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(ael);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder where(PreparedAel ael, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(false, ael, params);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder where(Exp exp) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(exp);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder where(Expression e) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(e);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    /**
     * Set the default where clause for all operations in this batch that don't have their own where clause.
     *
     * @param ael the AEL filter expression
     * @param params parameters to substitute into the AEL
     * @return this builder for method chaining
     * @see ChainableOperationBuilder#defaultWhere(String, Object...)
     */
    public ChainableNoBinsBuilder defaultWhere(String ael, Object... params) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = createWhereClauseProcessor(false, ael, params);
        if (processor != null) {
            ParseResult parseResult = processor.process(namespace, session);
            this.defaultWhereClause = Exp.build(parseResult.getExp());
        }
        return this;
    }

    /**
     * Set the default where clause using a BooleanExpression.
     *
     * @param ael the boolean expression filter
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultWhere(BooleanExpression ael) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = WhereClauseProcessor.from(ael);
        ParseResult parseResult = processor.process(namespace, session);
        this.defaultWhereClause = Exp.build(parseResult.getExp());
        return this;
    }

    /**
     * Set the default where clause using a PreparedAel.
     *
     * @param ael the prepared AEL filter
     * @param params parameters to bind to the prepared AEL
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultWhere(PreparedAel ael, Object... params) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = WhereClauseProcessor.from(false, ael, params);
        ParseResult parseResult = processor.process(namespace, session);
        this.defaultWhereClause = Exp.build(parseResult.getExp());
        return this;
    }

    /**
     * Set the default where clause using an Aerospike Exp.
     *
     * @param exp the expression filter
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultWhere(Exp exp) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = WhereClauseProcessor.from(exp);
        ParseResult parseResult = processor.process(namespace, session);
        this.defaultWhereClause = Exp.build(parseResult.getExp());
        return this;
    }

    // ========================================
    // Default Expiration Methods
    // ========================================

    /**
     * Set the default expiration for all operations in this chain that don't have an explicit expiration.
     *
     * @param duration the duration after which records should expire
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultExpireRecordAfter(Duration duration) {
        this.defaultExpirationInSeconds = duration.getSeconds();
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain that don't have an explicit expiration.
     *
     * @param seconds the number of seconds after which records should expire
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultExpireRecordAfterSeconds(long seconds) {
        this.defaultExpirationInSeconds = seconds;
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to an absolute date/time.
     *
     * @param dateTime the date/time at which records should expire
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultExpireRecordAt(LocalDateTime dateTime) {
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to an absolute date/time.
     *
     * @param date the date at which records should expire
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultExpireRecordAt(Date date) {
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to never expire (TTL = -1).
     *
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultNeverExpire() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Set the default to not change TTL for all operations in this chain (TTL = -2).
     *
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultNoChangeInExpiration() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to use the server default (TTL = 0).
     *
     * @return this builder for method chaining
     */
    public ChainableNoBinsBuilder defaultExpiryFromServerDefault() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder failOnFilteredOut() {
        verifyState("setting failOnFilteredOut");
        currentSpec.setFailOnFilteredOut(true);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public ChainableNoBinsBuilder includeMissingKeys() {
        verifyState("setting includeMissingKeys");
        currentSpec.setIncludeMissingKeys(true);
        return this;
    }

    // ========================================
    // Execution
    // ========================================

    /**
     * Execute all chained operations synchronously with default error handling.
     * Single-key operations throw on error; batch/multi-key operations embed errors in the stream.
     * All operations complete before this method returns, making it safe for transactions.
     *
     * @return RecordStream containing the results of all operations
     * @see #execute(ErrorStrategy)
     * @see #execute(ErrorHandler)
     */
    public RecordStream execute() {
        prepareSpecs();
        return OperationSpecExecutor.execute(session, operationSpecs, defaultWhereClause,
            defaultExpirationInSeconds, txnToUse, notInAnyTransaction,
            AbstractFilterableBuilder.defaultDisposition(operationSpecs),
            durableDeleteDefault);
    }

    /**
     * Execute all chained operations synchronously with the given error strategy.
     *
     * @param strategy the error strategy (must not be null)
     * @return RecordStream containing the results
     */
    public RecordStream execute(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeWithDisposition(ErrorDisposition.fromStrategy(strategy));
    }

    /**
     * Execute all chained operations synchronously, dispatching errors to the handler.
     * Error results are excluded from the returned stream.
     *
     * @param handler the error handler callback (must not be null)
     * @return RecordStream containing only successful results
     */
    public RecordStream execute(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        return executeWithDisposition(ErrorDisposition.handler(handler));
    }

    private RecordStream executeWithDisposition(ErrorDisposition disposition) {
        prepareSpecs();
        return OperationSpecExecutor.execute(session, operationSpecs, defaultWhereClause,
            defaultExpirationInSeconds, txnToUse, notInAnyTransaction, disposition, durableDeleteDefault);
    }

    /**
     * Execute all chained operations asynchronously with errors embedded in the stream.
     *
     * @param strategy the error strategy (must not be null)
     * @return RecordStream that will be populated as results arrive
     */
    public RecordStream executeAsync(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeAsyncInternal(null);
    }

    /**
     * Execute all chained operations asynchronously with errors dispatched to the handler.
     * Error results are excluded from the returned stream.
     *
     * @param handler the error handler callback (must not be null)
     * @return RecordStream containing only successful results
     */
    public RecordStream executeAsync(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        return executeAsyncInternal(handler);
    }

    private RecordStream executeAsyncInternal(ErrorHandler errorHandler) {
        prepareSpecs();

        if (txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using execute() for transactional safety."
            );
        }

        int totalKeys = operationSpecs.stream().mapToInt(spec -> spec.getKeys().size()).sum();
        AsyncRecordStream asyncStream = new AsyncRecordStream(totalKeys);

        Cluster cluster = session.getCluster();
        cluster.startVirtualThread(() -> {
            try {
                RecordStream syncResult = OperationSpecExecutor.execute(
                    session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds,
                    txnToUse, notInAnyTransaction, durableDeleteDefault);
                syncResult.forEach(result -> dispatchResult(result, asyncStream, errorHandler));
            } finally {
                asyncStream.complete();
            }
        });

        return new RecordStream(asyncStream);
    }

    private void prepareSpecs() {
        finalizeCurrentOperation();
        if (operationSpecs.isEmpty()) {
            throw new IllegalStateException("No operations specified");
        }
    }

    // ========================================
    // Internal helpers
    // ========================================

    /**
     * Verify that an operation has been specified before setting properties on it.
     *
     * <p><b>Important:</b> This condition should never occur in normal usage due to the API design.
     * This check exists as a safety mechanism to provide clear error messages if the API is used incorrectly
     * (e.g., through reflection or other non-standard means).</p>
     *
     * @param operationContext description of what operation is being attempted (e.g., "setting expiration", "setting where clause")
     * @throws IllegalStateException if no operation has been specified yet
     */
    private void verifyState(String operationContext) {
        if (currentSpec == null) {
            throw new IllegalStateException("Must call delete/touch/exists before " + operationContext);
        }
    }

    private void finalizeCurrentOperation() {
        if (currentSpec != null) {
            operationSpecs.add(currentSpec);
            currentSpec = null;
        }
    }

    private String getNamespaceFromKeys(List<Key> keys) {
        return keys.isEmpty() ? null : keys.get(0).namespace;
    }

}

