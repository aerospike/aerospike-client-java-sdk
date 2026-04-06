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

import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.query.WhereClauseProcessor;
import com.aerospike.dsl.ParseResult;

/**
 * Builder for chainable batch UDF (User Defined Function) operations.
 * This builder is used for executing UDFs on records.
 *
 * <p>UDFs are server-side Lua functions that can perform custom operations on records.
 * They are registered on the server and referenced by package name and function name.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Execute a UDF with arguments
 * session.executeUdf(users.id("user-1"))
 *     .function("myPackage", "myFunction")
 *     .passing("arg1", 42, true)
 *     .execute();
 *
 * // Execute a UDF with no arguments
 * session.executeUdf(users.id("user-1"))
 *     .function("myPackage", "myFunction")
 *     .execute();
 *
 * // Chain with other operations
 * session.executeUdf(users.id("user-1"))
 *     .function("myPackage", "myFunction")
 *     .passing("arg1")
 *     .upsert(users.id("user-2"))
 *     .bin("name").setTo("Alice")
 *     .execute();
 * }</pre>
 *
 * @see ChainableOperationBuilder for write operations with bin modifications
 * @see ChainableNoBinsBuilder for operations without bin modifications
 * @see ChainableQueryBuilder for read operations
 */
public class ChainableUdfBuilder extends AbstractSessionOperationBuilder<ChainableUdfBuilder>
        implements FilterableOperation<ChainableUdfBuilder> {

    private final List<OperationSpec> operationSpecs;
    private OperationSpec currentSpec = null;
    private Expression defaultWhereClause;
    private long defaultExpirationInSeconds = AbstractOperationBuilder.NOT_EXPLICITLY_SET;

    /**
     * Package-private constructor for creating a new chain.
     */
    ChainableUdfBuilder(Session session) {
        super(session, OpType.UDF);
        this.operationSpecs = new ArrayList<>();
    }

    /**
     * Package-private constructor for continuing an existing chain.
     */
    ChainableUdfBuilder(Session session, List<OperationSpec> existingSpecs,
                        Expression defaultWhereClause, long defaultExpirationInSeconds, Txn txnToUse) {
        super(session, OpType.UDF);
        this.operationSpecs = existingSpecs;
        this.defaultWhereClause = defaultWhereClause;
        this.defaultExpirationInSeconds = defaultExpirationInSeconds;
        this.txnToUse = txnToUse;
    }

    // ========================================
    // Initialization methods
    // ========================================

    /**
     * Initialize a UDF operation with the function already specified.
     * Called by {@link UdfFunctionBuilder#function(String, String)}.
     *
     * @param keys the keys to execute the UDF on
     * @param packageName the UDF package name
     * @param functionName the UDF function name
     * @return this builder for method chaining
     */
    ChainableUdfBuilder initUdfWithFunction(List<Key> keys, String packageName, String functionName) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(keys, OpType.UDF);
        currentSpec.setUdfPackageName(packageName);
        currentSpec.setUdfFunctionName(functionName);
        return this;
    }

    /**
     * Specify arguments to pass to the UDF.
     * Arguments are converted to Aerospike Value objects using {@link Value#get(Object)}.
     *
     * <p>Supported argument types include:
     * <ul>
     *   <li>String</li>
     *   <li>byte, short, int, long, float, double</li>
     *   <li>boolean</li>
     *   <li>byte[]</li>
     *   <li>List</li>
     *   <li>Map</li>
     *   <li>Value (passed through directly)</li>
     * </ul>
     *
     * @param args the arguments to pass to the UDF
     * @return this builder for method chaining
     * @throws AerospikeException if any argument type is not supported
     */
    public ChainableUdfBuilder passing(Object... args) {
        verifyState("specifying UDF arguments");
        if (args == null || args.length == 0) {
            currentSpec.setUdfArguments(new Value[0]);
        } else {
            Value[] values = new Value[args.length];
            for (int i = 0; i < args.length; i++) {
                values[i] = Value.get(args[i]);
            }
            currentSpec.setUdfArguments(values);
        }
        return this;
    }

    /**
     * Specify arguments to pass to the UDF using explicit Value objects.
     * Use this method when you need more control over type conversion.
     *
     * @param args the Value arguments to pass to the UDF
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder passingValues(Value... args) {
        verifyState("specifying UDF arguments");
        currentSpec.setUdfArguments(args == null ? new Value[0] : args);
        return this;
    }

    /**
     * Specify arguments to pass to the UDF from a List.
     * Arguments are converted to Aerospike Value objects using {@link Value#get(Object)}.
     *
     * <p>This is useful when arguments are already collected in a List rather than
     * as individual values.</p>
     *
     * @param args the list of arguments to pass to the UDF
     * @return this builder for method chaining
     * @throws AerospikeException if any argument type is not supported
     */
    public ChainableUdfBuilder passing(List<?> args) {
        verifyState("specifying UDF arguments");
        if (args == null || args.isEmpty()) {
            currentSpec.setUdfArguments(new Value[0]);
        } else {
            Value[] values = new Value[args.size()];
            for (int i = 0; i < args.size(); i++) {
                values[i] = Value.get(args.get(i));
            }
            currentSpec.setUdfArguments(values);
        }
        return this;
    }

    /**
     * Specify arguments to pass to the UDF using a List of explicit Value objects.
     * Use this method when you need more control over type conversion.
     *
     * @param args the list of Value arguments to pass to the UDF
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder passingValues(List<Value> args) {
        verifyState("specifying UDF arguments");
        if (args == null || args.isEmpty()) {
            currentSpec.setUdfArguments(new Value[0]);
        } else {
            currentSpec.setUdfArguments(args.toArray(new Value[0]));
        }
        return this;
    }

    // ========================================
    // Chainable operation methods
    // ========================================

    /**
     * Chain an executeUdf operation on a single key.
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
     * Chain an executeUdf operation on multiple keys.
     * Returns a {@link UdfFunctionBuilder} requiring the UDF function to be specified.
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
     * Chain an executeUdf operation on multiple keys (varargs).
     * Returns a {@link UdfFunctionBuilder} requiring the UDF function to be specified.
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

    // ========================================
    // Per-operation policies (override parent to work with OperationSpec)
    // ========================================

    @Override
    public ChainableUdfBuilder expireRecordAfter(Duration duration) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(duration.toSeconds());
        return this;
    }

    @Override
    public ChainableUdfBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(expirationInSeconds);
        return this;
    }

    @Override
    public ChainableUdfBuilder expireRecordAt(Date date) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(getExpirationInSecondsAndCheckValue(date));
        return this;
    }

    @Override
    public ChainableUdfBuilder expireRecordAt(LocalDateTime date) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(getExpirationInSecondsAndCheckValue(date));
        return this;
    }

    @Override
    public ChainableUdfBuilder withNoChangeInExpiration() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_NO_CHANGE);
        return this;
    }

    @Override
    public ChainableUdfBuilder neverExpire() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_NEVER_EXPIRE);
        return this;
    }

    @Override
    public ChainableUdfBuilder expiryFromServerDefault() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_SERVER_DEFAULT);
        return this;
    }

    @Override
    public ChainableUdfBuilder ensureGenerationIs(int generation) {
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

    @Override
    public ChainableUdfBuilder where(String dsl, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = createWhereClauseProcessor(false, dsl, params);
        if (processor != null) {
            ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
            currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        }
        return this;
    }

    @Override
    public ChainableUdfBuilder where(BooleanExpression dsl) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(dsl);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    @Override
    public ChainableUdfBuilder where(PreparedAel dsl, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(false, dsl, params);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    @Override
    public ChainableUdfBuilder where(Exp exp) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(exp);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    @Override
    public ChainableUdfBuilder where(Expression e) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(e);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }

    /**
     * Set the default where clause for all operations in this batch that don't have their own where clause.
     *
     * @param dsl the AEL filter expression
     * @param params parameters to substitute into the AEL
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultWhere(String dsl, Object... params) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = createWhereClauseProcessor(false, dsl, params);
        if (processor != null) {
            ParseResult parseResult = processor.process(namespace, session);
            this.defaultWhereClause = Exp.build(parseResult.getExp());
        }
        return this;
    }

    /**
     * Set the default where clause using a BooleanExpression.
     *
     * @param dsl the boolean expression filter
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultWhere(BooleanExpression dsl) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = WhereClauseProcessor.from(dsl);
        ParseResult parseResult = processor.process(namespace, session);
        this.defaultWhereClause = Exp.build(parseResult.getExp());
        return this;
    }

    /**
     * Set the default where clause using a PreparedDsl.
     *
     * @param dsl the prepared AEL filter
     * @param params parameters to bind to the prepared AEL
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultWhere(PreparedAel dsl, Object... params) {
        String namespace = currentSpec != null ?
                getNamespaceFromKeys(currentSpec.getKeys()) :
                (!operationSpecs.isEmpty() ? getNamespaceFromKeys(operationSpecs.get(0).getKeys()) : null);

        if (namespace == null) {
            throw new IllegalStateException("Cannot set defaultWhere before any operations are specified");
        }

        WhereClauseProcessor processor = WhereClauseProcessor.from(false, dsl, params);
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
    public ChainableUdfBuilder defaultWhere(Exp exp) {
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
    public ChainableUdfBuilder defaultExpireRecordAfter(Duration duration) {
        this.defaultExpirationInSeconds = duration.getSeconds();
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain that don't have an explicit expiration.
     *
     * @param seconds the number of seconds after which records should expire
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultExpireRecordAfterSeconds(long seconds) {
        this.defaultExpirationInSeconds = seconds;
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to an absolute date/time.
     *
     * @param dateTime the date/time at which records should expire
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultExpireRecordAt(LocalDateTime dateTime) {
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(dateTime);
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to an absolute date/time.
     *
     * @param date the date at which records should expire
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultExpireRecordAt(Date date) {
        this.defaultExpirationInSeconds = getExpirationInSecondsAndCheckValue(date);
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to never expire (TTL = -1).
     *
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultNeverExpire() {
        this.defaultExpirationInSeconds = TTL_NEVER_EXPIRE;
        return this;
    }

    /**
     * Set the default to not change TTL for all operations in this chain (TTL = -2).
     *
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultNoChangeInExpiration() {
        this.defaultExpirationInSeconds = TTL_NO_CHANGE;
        return this;
    }

    /**
     * Set the default expiration for all operations in this chain to use the server default (TTL = 0).
     *
     * @return this builder for method chaining
     */
    public ChainableUdfBuilder defaultExpiryFromServerDefault() {
        this.defaultExpirationInSeconds = TTL_SERVER_DEFAULT;
        return this;
    }

    @Override
    public ChainableUdfBuilder failOnFilteredOut() {
        verifyState("setting failOnFilteredOut");
        currentSpec.setFailOnFilteredOut(true);
        return this;
    }

    @Override
    public ChainableUdfBuilder includeMissingKeys() {
        verifyState("setting includeMissingKeys");
        currentSpec.setIncludeMissingKeys(true);
        return this;
    }

    // ========================================
    // Execution
    // ========================================

    /**
     * Execute all chained UDF operations synchronously with default error handling.
     * Single-key operations throw on error; batch/multi-key operations embed errors in the stream.
     * All operations complete before this method returns, making it safe for transactions.
     *
     * @return RecordStream containing the results of all operations
     * @see #execute(ErrorStrategy)
     * @see #execute(ErrorHandler)
     */
    public RecordStream execute() {
        prepareSpecs();

        if (Log.debugEnabled()) {
            int totalKeys = operationSpecs.stream().mapToInt(spec -> spec.getKeys().size()).sum();
            Log.debug("ChainableUdfBuilder.execute() called for " + operationSpecs.size() +
                     " operation(s), " + totalKeys + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        return OperationSpecExecutor.execute(session, operationSpecs, defaultWhereClause,
            defaultExpirationInSeconds, txnToUse, notInAnyTransaction,
            AbstractFilterableBuilder.defaultDisposition(operationSpecs));
    }

    /**
     * Execute all chained UDF operations synchronously with the given error strategy.
     *
     * @param strategy the error strategy (must not be null)
     * @return RecordStream containing the results
     */
    public RecordStream execute(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeWithDisposition(ErrorDisposition.fromStrategy(strategy));
    }

    /**
     * Execute all chained UDF operations synchronously, dispatching errors to the handler.
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

        if (Log.debugEnabled()) {
            int totalKeys = operationSpecs.stream().mapToInt(spec -> spec.getKeys().size()).sum();
            Log.debug("ChainableUdfBuilder.execute() called for " + operationSpecs.size() +
                     " operation(s), " + totalKeys + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

        return OperationSpecExecutor.execute(session, operationSpecs, defaultWhereClause,
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
        prepareSpecs();

        if (Log.debugEnabled()) {
            int totalKeys = operationSpecs.stream().mapToInt(spec -> spec.getKeys().size()).sum();
            Log.debug("ChainableUdfBuilder.executeAsync() called for " + operationSpecs.size() +
                     " operation(s), " + totalKeys + " key(s), transaction: " +
                     (txnToUse != null ? "yes" : "no"));
        }

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
                RecordStream syncResult = OperationSpecExecutor.execute(session, operationSpecs,
                	defaultWhereClause, defaultExpirationInSeconds, txnToUse, notInAnyTransaction);
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
     * @param operationContext description of what operation is being attempted
     * @throws IllegalStateException if no operation has been specified yet
     */
    private void verifyState(String operationContext) {
        if (currentSpec == null) {
            throw new IllegalStateException("Must call executeUdf before " + operationContext);
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

    @Override
    protected Session getSession() {
        return session;
    }
}
