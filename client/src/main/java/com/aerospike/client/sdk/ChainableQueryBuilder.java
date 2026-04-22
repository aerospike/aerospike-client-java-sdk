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
 * Builder for chainable batch query (read) operations.
 * This builder is used for {@code query} operations that read records without modifying them.
 *
 * <p>Unlike write operations, query operations support bin projection (selecting specific bins to read)
 * but cannot modify bin values. They can still apply where clauses for filtering.
 *
 * <p>Example usage:
 * <pre>{@code
 * session.query(users.ids("user-1", "user-2"))
 *     .bins("name", "email")  // Only read these bins
 *     .where("$.age > 21")
 *     .upsert(users.id("user-3"))
 *     .bin("status").setTo("active")
 *     .execute();
 * }</pre>
 *
 * @see ChainableOperationBuilder for write operations with bin modifications
 * @see ChainableNoBinsBuilder for operations without bin modifications
 */
public class ChainableQueryBuilder extends AbstractFilterableBuilder
        implements FilterableOperation<ChainableQueryBuilder> {

    private final Session session;
    private final List<OperationSpec> operationSpecs;
    private OperationSpec currentSpec = null;
    private Expression defaultWhereClause;
    private long defaultExpirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;
    private Txn txnToUse;
    private long limit = 0;
    private int startPartition = 0;
    private int endPartition = 4096;
    private boolean notInAnyTransaction;
    private boolean transactionSet;

    /**
     * Package-private constructor.
     */
    ChainableQueryBuilder(Session session, List<OperationSpec> existingSpecs,
                         Expression defaultWhereClause, long defaultExpirationInSeconds, Txn txnToUse) {
        this.session = session;
        this.operationSpecs = existingSpecs;
        this.defaultWhereClause = defaultWhereClause;
        this.defaultExpirationInSeconds = defaultExpirationInSeconds;
        this.txnToUse = txnToUse;
    }

    // ========================================
    // Initialization methods
    // ========================================

    ChainableQueryBuilder initQuery(Key key) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(List.of(key));  // null opType for query
        return this;
    }

    ChainableQueryBuilder initQuery(List<Key> keys) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(keys);  // null opType for query
        return this;
    }

    // ========================================
    // Query-specific methods
    // ========================================

    /**
     * Specify which bins to read (bin projection).
     * If not called, all bins will be read.
     *
     * @param binNames the names of the bins to read
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder readingOnlyBins(String... binNames) {
        verifyState("specifying bins");
        if (binNames == null || binNames.length == 0) {
            throw new IllegalArgumentException("Must specify at least one bin name");
        }
        currentSpec.setProjectedBins(binNames);
        return this;
    }

    /**
     * Specifies that no bins should be read (header-only query).
     * Useful when you only need to check for record existence or get metadata.
     *
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder withNoBins() {
        verifyState("specifying no bins");
        currentSpec.setProjectedBins(new String[0]);
        return this;
    }

    /**
     * Returns a bin builder for read operations on a specific bin.
     *
     * <p>Unlike write operations, query bin operations only support reading values
     * ({@code get()}) and computing expressions ({@code selectFrom()}).</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("name").get()
     *     .bin("ageIn20Years").selectFrom("$.age + 20")
     *     .execute();
     * }</pre>
     *
     * @param binName the name of the bin
     * @return QueryBinBuilder for constructing bin read operations
     */
    public QueryBinBuilder bin(String binName) {
        verifyState("adding bin operation");
        return new QueryBinBuilder(this, binName);
    }

    /**
     * Package-private method to add an operation to the current spec.
     * Used by QueryBinBuilder.
     */
    void addOperation(Operation op) {
        verifyState("adding operation");
        currentSpec.getOperations().add(op);
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
     * Returns a {@link ChainableNoBinsBuilder} since delete operations don't support bin modifications.
     *
     * @param key the key to delete
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder delete(Key key) {
        finalizeCurrentOperation();
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initDelete(key);
    }

    /**
     * Chain a delete operation on multiple keys.
     *
     * @param keys the keys to delete
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder delete(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initDelete(keys);
    }

    /**
     * Chain a delete operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableNoBinsBuilder for method chaining
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
     * Returns a {@link ChainableNoBinsBuilder} since touch operations don't support bin modifications.
     *
     * @param key the key to touch
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder touch(Key key) {
        finalizeCurrentOperation();
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initTouch(key);
    }

    /**
     * Chain a touch operation on multiple keys.
     *
     * @param keys the keys to touch
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder touch(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initTouch(keys);
    }

    /**
     * Chain a touch operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableNoBinsBuilder for method chaining
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
     * Returns a {@link ChainableNoBinsBuilder} since exists operations don't support bin modifications.
     *
     * @param key the key to check
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder exists(Key key) {
        finalizeCurrentOperation();
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initExists(key);
    }

    /**
     * Chain an exists check operation on multiple keys.
     *
     * @param keys the keys to check
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder exists(List<Key> keys) {
        finalizeCurrentOperation();
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse)
                .initExists(keys);
    }

    /**
     * Chain an exists check operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return ChainableNoBinsBuilder for method chaining
     */
    public ChainableNoBinsBuilder exists(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return exists(keys);
    }

    /**
     * Chain another query (read) operation on a single key.
     *
     * @param key the key to query
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder query(Key key) {
        return initQuery(key);
    }

    /**
     * Chain another query (read) operation on multiple keys.
     *
     * @param keys the keys to query
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder query(List<Key> keys) {
        return initQuery(keys);
    }

    /**
     * Chain another query (read) operation on multiple keys (varargs).
     *
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
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
    // FilterableOperation implementation
    // ========================================

    @Override
    public ChainableQueryBuilder where(String ael, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = createWhereClauseProcessor(false, ael, params);
        if (processor != null) {
            ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
            currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        }
        return this;
    }

    @Override
    public ChainableQueryBuilder where(BooleanExpression ael) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(ael);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    @Override
    public ChainableQueryBuilder where(PreparedAel ael, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(false, ael, params);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    @Override
    public ChainableQueryBuilder where(Exp exp) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(exp);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    @Override
    public ChainableQueryBuilder where(Expression e) {
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
    public ChainableQueryBuilder defaultWhere(String ael, Object... params) {
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
    public ChainableQueryBuilder defaultWhere(BooleanExpression ael) {
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
    public ChainableQueryBuilder defaultWhere(PreparedAel ael, Object... params) {
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
    public ChainableQueryBuilder defaultWhere(Exp exp) {
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
    public ChainableQueryBuilder defaultExpireRecordAfter(Duration duration) {
        this.defaultExpirationInSeconds = duration.getSeconds();
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain that don't have an explicit expiration.
     *
     * @param seconds the number of seconds after which records should expire
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder defaultExpireRecordAfterSeconds(long seconds) {
        this.defaultExpirationInSeconds = seconds;
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to an absolute date/time.
     *
     * @param dateTime the date/time at which records should expire
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder defaultExpireRecordAt(LocalDateTime dateTime) {
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to an absolute date/time.
     *
     * @param date the date at which records should expire
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder defaultExpireRecordAt(Date date) {
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to never expire (TTL = -1).
     *
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder defaultNeverExpire() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Set the default to not change TTL for all operations in this chain (TTL = -2).
     *
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder defaultNoChangeInExpiration() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to use the server default (TTL = 0).
     *
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder defaultExpiryFromServerDefault() {
        this.defaultExpirationInSeconds = AbstractOperationBuilder.TTL_SERVER_DEFAULT;
        return this;
    }

    private long getExpirationInSecondsAndCheckValue(LocalDateTime date) {
        LocalDateTime now = LocalDateTime.now();
        long expirationInSeconds = java.time.temporal.ChronoUnit.SECONDS.between(now, date);
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    private long getExpirationInSecondsAndCheckValue(Date date) {
        long expirationInSeconds = (date.getTime() - new Date().getTime()) / 1000L;
        if (expirationInSeconds < 0) {
            throw new IllegalArgumentException("Expiration must be set in the future, not to " + date);
        }
        return expirationInSeconds;
    }

    @Override
    public ChainableQueryBuilder failOnFilteredOut() {
        verifyState("setting failOnFilteredOut");
        currentSpec.setFailOnFilteredOut(true);
        return this;
    }

    @Override
    public ChainableQueryBuilder includeMissingKeys() {
        verifyState("setting includeMissingKeys");
        currentSpec.setIncludeMissingKeys(true);
        return this;
    }

    /**
     * Sets the maximum number of records to return.
     *
     * <p><b>Note:</b> This method limits the number of results returned from the batch.
     * The batch will still process all keys, but only the first N successful results
     * will be returned.</p>
     *
     * @param limit the maximum number of records to return (must be > 0)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if limit is <= 0
     */
    public ChainableQueryBuilder limit(long limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be > 0, not " + limit);
        }
        this.limit = limit;
        return this;
    }

    /**
     * Targets a specific partition for the query.
     *
     * <p>This method restricts the query to a single partition. Keys that don't
     * belong to this partition will be filtered out before execution.</p>
     *
     * @param partId the partition ID to target (0-4095)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if partId is out of range
     */
    public ChainableQueryBuilder onPartition(int partId) {
        if (partId < 0 || partId >= 4096) {
            throw new IllegalArgumentException("Partition ID must be between 0 and 4095, not " + partId);
        }
        this.startPartition = partId;
        this.endPartition = partId + 1;
        return this;
    }

    /**
     * Targets a range of partitions for the query.
     *
     * <p>This method restricts the query to a range of partitions. Keys that don't
     * belong to this partition range will be filtered out before execution.</p>
     *
     * @param startIncl the start partition (inclusive, 0-4095)
     * @param endExcl the end partition (exclusive, 1-4096)
     * @return this builder for method chaining
     * @throws IllegalArgumentException if the partition range is invalid
     */
    public ChainableQueryBuilder onPartitionRange(int startIncl, int endExcl) {
        if (startIncl < 0 || startIncl >= 4096) {
            throw new IllegalArgumentException("Start partition must be between 0 and 4095, not " + startIncl);
        }
        if (endExcl <= 0 || endExcl > 4096) {
            throw new IllegalArgumentException("End partition must be between 1 and 4096, not " + endExcl);
        }
        if (startIncl >= endExcl) {
            throw new IllegalArgumentException("Start partition must be less than end partition");
        }
        this.startPartition = startIncl;
        this.endPartition = endExcl;
        return this;
    }

    /**
     * Sets the chunk size for server-side streaming.
     *
     * <p><b>Note:</b> This method is primarily applicable to dataset queries.
     * For key-based batch queries, chunk size doesn't affect behavior as all
     * keys are sent in a single batch.</p>
     *
     * @param chunkSize the number of records per chunk
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder chunkSize(int chunkSize) {
        // For key-based queries, chunk size is not applicable
        // We accept the method for API compatibility but it has no effect
        return this;
    }

    /**
     * Specify that operations are not to be included in any transaction.
     *
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder notInAnyTransaction() {
        if (transactionSet) {
            throw AerospikeException.resultCodeToException(ResultCode.PARAMETER_ERROR,
                "The transaction mode has already been set");
        }
        this.transactionSet = true;
        this.txnToUse = null;
        this.notInAnyTransaction = true;
        return this;
    }

    /**
     * Specify the transaction to use for operations.
     *
     * @param txn the transaction
     * @return this builder for method chaining
     */
    public ChainableQueryBuilder inTransaction(Txn txn) {
        if (transactionSet) {
            throw AerospikeException.resultCodeToException(ResultCode.PARAMETER_ERROR,
                "The transaction mode has already been set");
        }
        this.transactionSet = true;
        this.txnToUse = txn;
        this.notInAnyTransaction = false;
        return this;
    }

    // ========================================
    // Execution
    // ========================================

    /**
     * Execute all chained query operations synchronously with default error handling.
     * Single-key operations throw on error; batch/multi-key operations embed errors in the stream.
     * For single-key operations, this uses optimized point execution.
     *
     * @return RecordStream containing the results of all operations
     * @see #execute(ErrorStrategy)
     * @see #execute(ErrorHandler)
     */
    public RecordStream execute() {
        List<OperationSpec> specs = prepareSpecs();
        if (specs.isEmpty()) {
            return new RecordStream();
        }
        return OperationSpecExecutor.execute(session, specs, defaultWhereClause,
            defaultExpirationInSeconds, txnToUse, notInAnyTransaction,
            AbstractFilterableBuilder.defaultDisposition(specs));
    }

    /**
     * Execute all chained query operations synchronously with the given error strategy.
     *
     * @param strategy the error strategy (must not be null)
     * @return RecordStream containing the results
     */
    public RecordStream execute(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeWithDisposition(ErrorDisposition.fromStrategy(strategy));
    }

    /**
     * Execute all chained query operations synchronously, dispatching errors to the handler.
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
        List<OperationSpec> specs = prepareSpecs();
        if (specs.isEmpty()) {
            return new RecordStream();
        }
        return OperationSpecExecutor.execute(session, specs, defaultWhereClause,
            defaultExpirationInSeconds, txnToUse, notInAnyTransaction, disposition);
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
        List<OperationSpec> specs = prepareSpecs();
        if (specs.isEmpty()) {
            return new RecordStream();
        }

        if (txnToUse != null && Log.warnEnabled()) {
            Log.warn(
                "executeAsync() called within a transaction. " +
                "Async operations may still be in flight when commit() is called, " +
                "which could lead to inconsistent state. " +
                "Consider using execute() for transactional safety."
            );
        }

        int totalKeys = specs.stream().mapToInt(spec -> spec.getKeys().size()).sum();
        AsyncRecordStream asyncStream = new AsyncRecordStream(totalKeys);

        Cluster cluster = session.getCluster();
        cluster.startVirtualThread(() -> {
            try {
                RecordStream syncResult = OperationSpecExecutor.execute(
                    session, specs, defaultWhereClause, defaultExpirationInSeconds, txnToUse,
                    notInAnyTransaction);
                syncResult.forEach(result -> dispatchResult(result, asyncStream, errorHandler));
            } finally {
                asyncStream.complete();
            }
        });

        return new RecordStream(asyncStream);
    }


    private List<OperationSpec> prepareSpecs() {
        finalizeCurrentOperation();
        if (operationSpecs.isEmpty()) {
            throw new IllegalStateException("No operations specified");
        }
        List<OperationSpec> filteredSpecs = applyPartitionFilter(operationSpecs);
        return applyKeyLimit(filteredSpecs);
    }

    /**
     * Apply partition filter to specs, filtering out keys that don't belong to the partition range.
     */
    private List<OperationSpec> applyPartitionFilter(List<OperationSpec> specs) {
        if (!hasPartitionFilter()) {
            return specs;
        }

        List<OperationSpec> filtered = new ArrayList<>();
        for (OperationSpec spec : specs) {
            List<Key> filteredKeys = new ArrayList<>();
            for (Key key : spec.getKeys()) {
                if (isKeyInPartitionRange(key)) {
                    filteredKeys.add(key);
                }
            }
            if (!filteredKeys.isEmpty()) {
                OperationSpec newSpec = copySpecWithKeys(spec, filteredKeys);
                filtered.add(newSpec);
            }
        }
        return filtered;
    }

    /**
     * Apply limit to keys across all specs.
     */
    private List<OperationSpec> applyKeyLimit(List<OperationSpec> specs) {
        if (limit <= 0) {
            return specs;
        }

        List<OperationSpec> limited = new ArrayList<>();
        long remaining = limit;

        for (OperationSpec spec : specs) {
            if (remaining <= 0) {
                break;
            }

            List<Key> keys = spec.getKeys();
            if (keys.size() <= remaining) {
                limited.add(spec);
                remaining -= keys.size();
            } else {
                // Need to truncate this spec's keys
                List<Key> truncatedKeys = new ArrayList<>(keys.subList(0, (int) remaining));
                OperationSpec newSpec = copySpecWithKeys(spec, truncatedKeys);
                limited.add(newSpec);
                remaining = 0;
            }
        }
        return limited;
    }

    /**
     * Create a copy of the spec with different keys.
     */
    private OperationSpec copySpecWithKeys(OperationSpec original, List<Key> newKeys) {
        OperationSpec newSpec;
        if (original.isQuery()) {
            newSpec = new OperationSpec(newKeys);
        } else {
            newSpec = new OperationSpec(newKeys, original.getOpType());
        }
        newSpec.setWhereClause(original.getWhereClause());
        newSpec.setGeneration(original.getGeneration());
        newSpec.setExpirationInSeconds(original.getExpirationInSeconds());
        newSpec.setFailOnFilteredOut(original.isFailOnFilteredOut());
        newSpec.setIncludeMissingKeys(original.isIncludeMissingKeys());
        newSpec.setDurablyDelete(original.getDurablyDelete());
        newSpec.setProjectedBins(original.getProjectedBins());
        newSpec.getOperations().addAll(original.getOperations());
        return newSpec;
    }

    /**
     * Check if partition filtering is active.
     */
    private boolean hasPartitionFilter() {
        return startPartition > 0 || endPartition < 4096;
    }

    /**
     * Check if a key is within the partition range.
     */
    private boolean isKeyInPartitionRange(Key key) {
        if (!hasPartitionFilter()) {
            return true;
        }
        int partId = com.aerospike.client.sdk.tend.Partition.getPartitionId(key.digest);
        return partId >= startPartition && partId < endPartition;
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
     * @param operationContext description of what operation is being attempted (e.g., "specifying bins", "setting where clause")
     * @throws IllegalStateException if no operation has been specified yet
     */
    private void verifyState(String operationContext) {
        if (currentSpec == null) {
            throw new IllegalStateException("Must call query() before " + operationContext);
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

