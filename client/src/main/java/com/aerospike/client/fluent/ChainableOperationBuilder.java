package com.aerospike.client.fluent;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;
import com.aerospike.dsl.ParseResult;

/**
 * Builder for chainable batch operations that support bin-level modifications.
 * This builder is used for {@code upsert}, {@code update}, {@code insert}, and {@code replace} operations.
 * 
 * <p>This class allows chaining multiple heterogeneous operations (upsert, update, insert, replace,
 * delete, touch, exists, query) together, which are then executed as a single batch operation
 * for optimal performance.
 * 
 * <p>Example usage:
 * <pre>{@code
 * session.upsert(users.id("user-1"))
 *     .bin("name").setTo("Alice")
 *     .bin("age").setTo(30)
 *     .update(users.id("user-2"))
 *     .bin("loginCount").add(1)
 *     .delete(users.id("user-3"))
 *     .execute();
 * }</pre>
 * 
 * @see ChainableNoBinsBuilder for operations without bin modifications
 * @see ChainableQueryBuilder for read operations
 */
public class ChainableOperationBuilder extends AbstractOperationBuilder<ChainableOperationBuilder>
        implements FilterableOperation<ChainableOperationBuilder> {
    
    private final List<OperationSpec> operationSpecs;
    private OperationSpec currentSpec = null;
    private Expression defaultWhereClause;
    
    /**
     * Package-private constructor for creating a new chain.
     */
    ChainableOperationBuilder(Session session, OpType opType) {
        super(session, opType);
        this.operationSpecs = new ArrayList<>();
    }
    
    /**
     * Package-private constructor for continuing an existing chain.
     */
    ChainableOperationBuilder(Session session, OpType opType, List<OperationSpec> existingSpecs,
                              Expression defaultWhereClause, Txn txnToUse) {
        super(session, opType);
        this.operationSpecs = existingSpecs;
        this.defaultWhereClause = defaultWhereClause;
        this.txnToUse = txnToUse;
    }
    
    // ========================================
    // Initialization methods
    // ========================================
    
    ChainableOperationBuilder init(Key key, OpType opType) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(List.of(key), opType);
        return this;
    }
    
    ChainableOperationBuilder init(List<Key> keys, OpType opType) {
        finalizeCurrentOperation();
        currentSpec = new OperationSpec(keys, opType);
        return this;
    }
    
    // ========================================
    // Bin operations - override to use OperationSpec
    // ========================================
    
    /**
     * Returns a bin builder for operating on a specific bin.
     * This starts a bin operation that will be part of the current operation spec.
     * 
     * @param binName the name of the bin
     * @return BinBuilder for constructing bin operations
     */
    @Override
    public BinBuilder<ChainableOperationBuilder> bin(String binName) {
        verifyState("adding bin operation");
        return new BinBuilder<>(this, binName);
    }
    
    /**
     * Specify a set of bin names for the bins+values pattern.
     * Use this followed by {@code .values(...)} to efficiently set multiple bins at once.
     * 
     * <p>Note: This is only for setting simple values, not for CDT operations.
     * 
     * @param binName the first bin name (required)
     * @param binNames additional bin names
     * @return BinsValuesBuilder for specifying values
     */
    public BinsValuesBuilder bins(String binName, String... binNames) {
        verifyState("specifying bins");
        return new BinsValuesBuilder(new ChainableBinsValuesOperations(), currentSpec.getKeys(), binName, binNames);
    }
    
    /**
     * Override setTo to store in currentSpec.operations instead of inherited ops.
     */
    @Override
    protected ChainableOperationBuilder setTo(Bin bin) {
        verifyState("setting bin value");
        currentSpec.getOperations().add(Operation.put(bin));
        return this;
    }
    
    /**
     * Override get to store in currentSpec.operations.
     */
    @Override
    protected ChainableOperationBuilder get(String binName) {
        verifyState("getting bin value");
        currentSpec.getOperations().add(Operation.get(binName));
        return this;
    }
    
    /**
     * Override append to store in currentSpec.operations.
     */
    @Override
    protected ChainableOperationBuilder append(Bin bin) {
        verifyState("appending to bin");
        currentSpec.getOperations().add(Operation.append(bin));
        return this;
    }
    
    /**
     * Override prepend to store in currentSpec.operations.
     */
    @Override
    protected ChainableOperationBuilder prepend(Bin bin) {
        verifyState("prepending to bin");
        currentSpec.getOperations().add(Operation.prepend(bin));
        return this;
    }
    
    /**
     * Override add to store in currentSpec.operations.
     */
    @Override
    protected ChainableOperationBuilder add(Bin bin) {
        verifyState("adding to bin");
        currentSpec.getOperations().add(Operation.add(bin));
        return this;
    }
    
    /**
     * Override addOp to store in currentSpec.operations.
     */
    @Override
    protected ChainableOperationBuilder addOp(Operation op) {
        verifyState("adding operation");
        currentSpec.getOperations().add(op);
        return this;
    }
    
    // ========================================
    // Chainable operation methods
    // ========================================
    
    /**
     * Chain an upsert operation on a single key.
     * 
     * @param key the key to upsert
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder upsert(Key key) {
        return init(key, OpType.UPSERT);
    }
    
    /**
     * Chain an upsert operation on multiple keys.
     * 
     * @param keys the keys to upsert
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder upsert(List<Key> keys) {
        return init(keys, OpType.UPSERT);
    }
    
    /**
     * Chain an upsert operation on multiple keys (varargs).
     * 
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
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
     * 
     * @param key the key to update
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder update(Key key) {
        return init(key, OpType.UPDATE);
    }
    
    /**
     * Chain an update operation on multiple keys.
     * 
     * @param keys the keys to update
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder update(List<Key> keys) {
        return init(keys, OpType.UPDATE);
    }
    
    /**
     * Chain an update operation on multiple keys (varargs).
     * 
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
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
     * 
     * @param key the key to insert
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder insert(Key key) {
        return init(key, OpType.INSERT);
    }
    
    /**
     * Chain an insert operation on multiple keys.
     * 
     * @param keys the keys to insert
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder insert(List<Key> keys) {
        return init(keys, OpType.INSERT);
    }
    
    /**
     * Chain an insert operation on multiple keys (varargs).
     * 
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
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
     * 
     * @param key the key to replace
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder replace(Key key) {
        return init(key, OpType.REPLACE);
    }
    
    /**
     * Chain a replace operation on multiple keys.
     * 
     * @param keys the keys to replace
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder replace(List<Key> keys) {
        return init(keys, OpType.REPLACE);
    }
    
    /**
     * Chain a replace operation on multiple keys (varargs).
     * 
     * @param key1 first key
     * @param key2 second key
     * @param moreKeys additional keys
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder replace(Key key1, Key key2, Key... moreKeys) {
        List<Key> keys = new ArrayList<>();
        keys.add(key1);
        keys.add(key2);
        keys.addAll(Arrays.asList(moreKeys));
        return replace(keys);
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
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableNoBinsBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableQueryBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
        return new ChainableQueryBuilder(session, operationSpecs, defaultWhereClause, txnToUse)
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
    public ChainableOperationBuilder expireRecordAfter(Duration duration) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(duration.toSeconds());
        return this;
    }
    
    @Override
    public ChainableOperationBuilder expireRecordAfterSeconds(int expirationInSeconds) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(expirationInSeconds);
        return this;
    }
    
    @Override
    public ChainableOperationBuilder expireRecordAt(Date date) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(getExpirationInSecondsAndCheckValue(date));
        return this;
    }
    
    @Override
    public ChainableOperationBuilder expireRecordAt(LocalDateTime date) {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(getExpirationInSecondsAndCheckValue(date));
        return this;
    }
    
    @Override
    public ChainableOperationBuilder withNoChangeInExpiration() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_NO_CHANGE);
        return this;
    }
    
    @Override
    public ChainableOperationBuilder neverExpire() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_NEVER_EXPIRE);
        return this;
    }
    
    @Override
    public ChainableOperationBuilder expiryFromServerDefault() {
        verifyState("setting expiration");
        currentSpec.setExpirationInSeconds(TTL_SERVER_DEFAULT);
        return this;
    }
    
    @Override
    public ChainableOperationBuilder ensureGenerationIs(int generation) {
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
    public ChainableOperationBuilder where(String dsl, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = createWhereClauseProcessor(false, dsl, params);
        if (processor != null) {
            ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
            currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        }
        return this;
    }
    
    @Override
    public ChainableOperationBuilder where(BooleanExpression dsl) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(dsl);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }
    
    @Override
    public ChainableOperationBuilder where(PreparedDsl dsl, Object... params) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(false, dsl, params);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }
    
    @Override
    public ChainableOperationBuilder where(Exp exp) {
        verifyState("setting where clause");
        WhereClauseProcessor processor = WhereClauseProcessor.from(exp);
        ParseResult parseResult = processor.process(getNamespaceFromKeys(currentSpec.getKeys()), session);
        currentSpec.setWhereClause(Exp.build(parseResult.getExp()));
        return this;
    }
    
    /**
     * Set the default where clause for all operations in this batch that don't have their own where clause.
     * 
     * <p>This filter is applied to operations that don't have an explicit {@code where()} clause.
     * Operations with their own where clause will use their own filter instead.
     * 
     * @param dsl the DSL filter expression
     * @param params parameters to substitute into the DSL
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder defaultWhere(String dsl, Object... params) {
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
    public ChainableOperationBuilder defaultWhere(BooleanExpression dsl) {
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
     * @param dsl the prepared DSL filter
     * @param params parameters to bind to the prepared DSL
     * @return this builder for method chaining
     */
    public ChainableOperationBuilder defaultWhere(PreparedDsl dsl, Object... params) {
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
    public ChainableOperationBuilder defaultWhere(Exp exp) {
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
    
    @Override
    public ChainableOperationBuilder failOnFilteredOut() {
        verifyState("setting failOnFilteredOut");
        currentSpec.setFailOnFilteredOut(true);
        return this;
    }
    
    @Override
    public ChainableOperationBuilder respondAllKeys() {
        verifyState("setting respondAllKeys");
        currentSpec.setRespondAllKeys(true);
        return this;
    }
    
    // ========================================
    // Execution
    // ========================================
    
    /**
     * Execute all chained operations as a single batch.
     * 
     * @return RecordStream containing the results of all operations
     */
    public RecordStream execute() {
        finalizeCurrentOperation();
        
        if (operationSpecs.isEmpty()) {
            throw new IllegalStateException("No operations specified");
        }
        
        return BatchExecutor.execute(session, operationSpecs, defaultWhereClause, txnToUse);
    }
    
    // ========================================
    // Internal helpers
    // ========================================
    
    /**
     * Verify that an operation has been specified before setting properties on it.
     * 
     * <p><b>Important:</b> This condition should never occur in normal usage due to the fluent API design.
     * This check exists as a safety mechanism to provide clear error messages if the API is used incorrectly
     * (e.g., through reflection or other non-standard means).</p>
     * 
     * @param operationContext description of what operation is being attempted (e.g., "setting expiration", "setting where clause")
     * @throws IllegalStateException if no operation has been specified yet
     */
    private void verifyState(String operationContext) {
        if (currentSpec == null) {
            throw new IllegalStateException("Must call upsert/update/insert/replace before " + operationContext);
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
    
    /**
     * Inner class implementing BinsValuesOperations for the chainable context.
     */
    private class ChainableBinsValuesOperations implements BinsValuesOperations {
        @Override
        public Session getSession() {
            return session;
        }
        
        @Override
        public OpType getOpType() {
            return currentSpec != null ? currentSpec.getOpType() : null;
        }
        
        @Override
        public int getNumKeys() {
            return currentSpec != null ? currentSpec.getKeys().size() : 0;
        }
        
        @Override
        public boolean isMultiKey() {
            return currentSpec != null && currentSpec.getKeys().size() > 1;
        }
        
        @Override
        public boolean isRespondAllKeys() {
            return currentSpec != null && currentSpec.isRespondAllKeys();
        } 
        
        @Override
        public Txn getTxnToUse() {
            return txnToUse;
        }
        
        @Override
        public int getExpirationAsInt(long expirationInSeconds) {
            return ChainableOperationBuilder.this.getExpirationAsInt(expirationInSeconds);
        }
        
        @Override
        public long getExpirationInSecondsAndCheckValue(Date date) {
            return ChainableOperationBuilder.this.getExpirationInSecondsAndCheckValue(date);
        }
        
        @Override
        public long getExpirationInSecondsAndCheckValue(LocalDateTime dateTime) {
            return ChainableOperationBuilder.this.getExpirationInSecondsAndCheckValue(dateTime);
        }
        
//        @Override
//        public WritePolicy getWritePolicy(Settings settings, int generation, OpType opType) {
//            WritePolicy result = settings.asWritePolicy();
//            result.generation = generation;
//            result.generationPolicy = generation > 0 ? 
//                    GenerationPolicy.EXPECT_GEN_EQUAL : 
//                    GenerationPolicy.NONE;
//            result.recordExistsAction = AbstractSessionOperationBuilder.recordExistsActionFromOpType(opType);
//            return result;
//        }
//        
        @Override
        public void showWarningsOnException(AerospikeException ae, Txn txn, Key key, int expiration) {
            if (Log.warnEnabled()) {
                if (ae.getResultCode() == ResultCode.FAIL_FORBIDDEN && expiration > 0) {
                    Log.warn("Operation failed on server with FAIL_FORBIDDEN (22) and the record had "
                            + "an expiry set in the operation. This is possibly caused by nsup being disabled. "
                            + "See https://aerospike.com/docs/database/reference/error-codes for more information");
                }
                if (ae.getResultCode() == ResultCode.UNSUPPORTED_FEATURE) {
                    if (txn != null && !session.isNamespaceSC(key.namespace)) {
                        Log.warn(String.format("Namespace '%s' is involved in transaction, but it is not an SC namespace. "
                                + "This will throw an Unsupported Server Feature exception", key.namespace));
                    }
                }
            }
        }
        
//        @Override
//        public void executeAndPublishSingleOperation(
//                WritePolicy wp,
//                Key key,
//                Operation[] operations,
//                AsyncRecordStream asyncStream,
//                int index,
//                boolean stackTraceOnException) {
//            try {
//                Record record = session.getClient().operate(wp, key, operations);
//                if (currentSpec != null && currentSpec.respondAllKeys || record != null) {
//                    asyncStream.publish(new RecordResult(key, record, index));
//                }
//            } catch (AerospikeException ae) {
//                if (ae.getResultCode() == ResultCode.FILTERED_OUT) {
//                    if (currentSpec != null && (currentSpec.failOnFilteredOut || currentSpec.respondAllKeys)) {
//                        asyncStream.publish(new RecordResult(key, AeroException.from(ae), index));
//                    }
//                    // Otherwise skip this record
//                } else {
//                    showWarningsOnException(ae, txnToUse, key, wp.expiration);
//                    asyncStream.publish(new RecordResult(key, AeroException.from(ae), index));
//                }
//            }
//        }
    }
}
