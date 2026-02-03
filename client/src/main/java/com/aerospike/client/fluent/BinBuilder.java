package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Consumer;

import com.aerospike.client.fluent.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.MapOrder;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.exp.ExpReadFlags;
import com.aerospike.client.fluent.exp.ExpWriteFlags;
import com.aerospike.client.fluent.query.PreparedDsl;

public class BinBuilder<T extends AbstractOperationBuilder<T>> extends AbstractCdtBuilder<T> {
    public BinBuilder(T opBuilder, String binName) {
        super(opBuilder, binName, null);
    }

    /**
     * Create set database operation.
     */
    public T setTo(String value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(int value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(long value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(float value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(double value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(boolean value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(byte[] value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(List<?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(Map<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public T setTo(SortedMap<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create bin-remove operation.
     */
    public T remove() {
        return opBuilder.setTo(Bin.asNull(binName));
    }
    /**
     * Create set database operation.
     */
    public T get() {
        return opBuilder.get(binName);
    }
    /**
     * Create string append database operation.
     */
    public T append(String value) {
        return opBuilder.append(new Bin(binName, value));
    }
    /**
     * Create string prepend database operation.
     */
    public T prepend(String value) {
        return opBuilder.prepend(new Bin(binName, value));
    }
    /**
     * Create integer add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public T add(int amount) {
        return opBuilder.add(new Bin(binName, amount));
    }
    /**
     * Create long add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public T add(long amount) {
        return opBuilder.add(new Bin(binName, amount));
    }
    /**
     * Create float add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public T add(float amount) {
        return opBuilder.add(new Bin(binName, amount));
    }
    /**
     * Create double add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public T add(double amount) {
        return opBuilder.add(new Bin(binName, amount));
    }

    // ==================================================================
    // Expression Operations - Read and write values computed from DSL expressions
    // 
    // Each operation supports 5 DSL input types:
    // 1. String dsl - DSL string expression
    // 2. BooleanExpression - Programmatic boolean expression
    // 3. PreparedDsl - Prepared DSL with parameters
    // 4. Exp - Low-level expression builder
    // 5. Expression - Compiled expression
    //
    // Each also has an overload with Consumer<Options> for configuring flags.
    // ==================================================================

    // ----------------------------------------
    // selectFrom - Read expression operations
    // ----------------------------------------

    /**
     * Create a read expression operation from a DSL string.
     * @param dsl the DSL expression to evaluate
     * @return the parent builder for method chaining
     */
    public T selectFrom(String dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, dsl, ExpReadFlags.DEFAULT));
    }

    /**
     * Create a read expression operation from a DSL string with options.
     * @param dsl the DSL expression to evaluate
     * @param options a consumer to configure expression options
     * @return the parent builder for method chaining
     */
    public T selectFrom(String dsl, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, dsl, opts.getFlags()));
    }

    /**
     * Create a read expression operation from a BooleanExpression.
     * @param dsl the boolean expression to evaluate
     * @return the parent builder for method chaining
     */
    public T selectFrom(BooleanExpression dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, dsl, ExpReadFlags.DEFAULT));
    }

    /**
     * Create a read expression operation from a BooleanExpression with options.
     * @param dsl the boolean expression to evaluate
     * @param options a consumer to configure expression options
     * @return the parent builder for method chaining
     */
    public T selectFrom(BooleanExpression dsl, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, dsl, opts.getFlags()));
    }

    /**
     * Create a read expression operation from a PreparedDsl.
     * @param dsl the prepared DSL statement
     * @param params the parameters to bind
     * @return the parent builder for method chaining
     */
    public T selectFrom(PreparedDsl dsl, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, dsl, params, ExpReadFlags.DEFAULT));
    }

    /**
     * Create a read expression operation from a PreparedDsl with options.
     * @param dsl the prepared DSL statement
     * @param options a consumer to configure expression options
     * @param params the parameters to bind
     * @return the parent builder for method chaining
     */
    public T selectFrom(PreparedDsl dsl, Consumer<ExpressionReadOptions> options, Object... params) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, dsl, params, opts.getFlags()));
    }

    /**
     * Create a read expression operation from an Exp.
     * @param exp the expression to evaluate
     * @return the parent builder for method chaining
     */
    public T selectFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
    }

    /**
     * Create a read expression operation from an Exp with options.
     * @param exp the expression to evaluate
     * @param options a consumer to configure expression options
     * @return the parent builder for method chaining
     */
    public T selectFrom(Exp exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
    }

    /**
     * Create a read expression operation from an Expression.
     * @param exp the compiled expression to evaluate
     * @return the parent builder for method chaining
     */
    public T selectFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
    }

    /**
     * Create a read expression operation from an Expression with options.
     * @param exp the compiled expression to evaluate
     * @param options a consumer to configure expression options
     * @return the parent builder for method chaining
     */
    public T selectFrom(Expression exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
    }

    // ----------------------------------------
    // insertFrom - Write with CREATE_ONLY
    // ----------------------------------------

    /** Create a write expression (CREATE_ONLY) from a DSL string. */
    public T insertFrom(String dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, ExpWriteFlags.CREATE_ONLY));
    }

    /** Create a write expression (CREATE_ONLY) from a DSL string with options. */
    public T insertFrom(String dsl, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, opts.getFlags()));
    }

    /** Create a write expression (CREATE_ONLY) from a BooleanExpression. */
    public T insertFrom(BooleanExpression dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, ExpWriteFlags.CREATE_ONLY));
    }

    /** Create a write expression (CREATE_ONLY) from a BooleanExpression with options. */
    public T insertFrom(BooleanExpression dsl, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, opts.getFlags()));
    }

    /** Create a write expression (CREATE_ONLY) from a PreparedDsl. */
    public T insertFrom(PreparedDsl dsl, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, params, ExpWriteFlags.CREATE_ONLY));
    }

    /** Create a write expression (CREATE_ONLY) from a PreparedDsl with options. */
    public T insertFrom(PreparedDsl dsl, Consumer<ExpressionWriteOptions> options, Object... params) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, params, opts.getFlags()));
    }

    /** Create a write expression (CREATE_ONLY) from an Exp. */
    public T insertFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.CREATE_ONLY));
    }

    /** Create a write expression (CREATE_ONLY) from an Exp with options. */
    public T insertFrom(Exp exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    /** Create a write expression (CREATE_ONLY) from an Expression. */
    public T insertFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.CREATE_ONLY));
    }

    /** Create a write expression (CREATE_ONLY) from an Expression with options. */
    public T insertFrom(Expression exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    // ----------------------------------------
    // updateFrom - Write with UPDATE_ONLY
    // ----------------------------------------

    /** Create a write expression (UPDATE_ONLY) from a DSL string. */
    public T updateFrom(String dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, ExpWriteFlags.UPDATE_ONLY));
    }

    /** Create a write expression (UPDATE_ONLY) from a DSL string with options. */
    public T updateFrom(String dsl, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, opts.getFlags()));
    }

    /** Create a write expression (UPDATE_ONLY) from a BooleanExpression. */
    public T updateFrom(BooleanExpression dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, ExpWriteFlags.UPDATE_ONLY));
    }

    /** Create a write expression (UPDATE_ONLY) from a BooleanExpression with options. */
    public T updateFrom(BooleanExpression dsl, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, opts.getFlags()));
    }

    /** Create a write expression (UPDATE_ONLY) from a PreparedDsl. */
    public T updateFrom(PreparedDsl dsl, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, params, ExpWriteFlags.UPDATE_ONLY));
    }

    /** Create a write expression (UPDATE_ONLY) from a PreparedDsl with options. */
    public T updateFrom(PreparedDsl dsl, Consumer<ExpressionWriteOptions> options, Object... params) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, params, opts.getFlags()));
    }

    /** Create a write expression (UPDATE_ONLY) from an Exp. */
    public T updateFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.UPDATE_ONLY));
    }

    /** Create a write expression (UPDATE_ONLY) from an Exp with options. */
    public T updateFrom(Exp exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    /** Create a write expression (UPDATE_ONLY) from an Expression. */
    public T updateFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.UPDATE_ONLY));
    }

    /** Create a write expression (UPDATE_ONLY) from an Expression with options. */
    public T updateFrom(Expression exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    // ----------------------------------------
    // upsertFrom - Write with DEFAULT (upsert)
    // ----------------------------------------

    /** Create a write expression (upsert) from a DSL string. */
    public T upsertFrom(String dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, ExpWriteFlags.DEFAULT));
    }

    /** Create a write expression (upsert) from a DSL string with options. */
    public T upsertFrom(String dsl, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, opts.getFlags()));
    }

    /** Create a write expression (upsert) from a BooleanExpression. */
    public T upsertFrom(BooleanExpression dsl) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, ExpWriteFlags.DEFAULT));
    }

    /** Create a write expression (upsert) from a BooleanExpression with options. */
    public T upsertFrom(BooleanExpression dsl, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, opts.getFlags()));
    }

    /** Create a write expression (upsert) from a PreparedDsl. */
    public T upsertFrom(PreparedDsl dsl, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, params, ExpWriteFlags.DEFAULT));
    }

    /** Create a write expression (upsert) from a PreparedDsl with options. */
    public T upsertFrom(PreparedDsl dsl, Consumer<ExpressionWriteOptions> options, Object... params) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, dsl, params, opts.getFlags()));
    }

    /** Create a write expression (upsert) from an Exp. */
    public T upsertFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.DEFAULT));
    }

    /** Create a write expression (upsert) from an Exp with options. */
    public T upsertFrom(Exp exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    /** Create a write expression (upsert) from an Expression. */
    public T upsertFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.DEFAULT));
    }

    /** Create a write expression (upsert) from an Expression with options. */
    public T upsertFrom(Expression exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    // ==================================================================
    // CDT Operations. Note: make sure to mirror these operations to
    // CdtContextNonInvertableBuilder and CdtContextInvertableBuilder
    // ==================================================================
    public CdtContextNonInvertableBuilder<T> onMapIndex(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX, index));
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }
    public CdtContextNonInvertableBuilder<T> onMapRank(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK, index));
    }
    public CdtContextInvertableBuilder<T> onMapValue(long value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onMapValue(String value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onMapValue(byte[] value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl))); 
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    
    /**
     * Navigate to map items by key relative to index range.
     * Server selects map items nearest to key and greater by index.
     * 
     * @param key the reference key
     * @param index the relative index offset
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index));
    }
    
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index));
    }
    
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index));
    }
    
    /**
     * Navigate to map items by key relative to index range with count limit.
     * Server selects map items nearest to key and greater by index with a count limit.
     * 
     * @param key the reference key
     * @param index the relative index offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count));
    }
    
    /**
     * Navigate to map items by value relative to rank range.
     * Server selects map items nearest to value and greater by relative rank.
     * 
     * @param value the reference value
     * @param rank the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank));
    }
    
    /**
     * Navigate to map items by value relative to rank range with count limit.
     * Server selects map items nearest to value and greater by relative rank with a count limit.
     * 
     * @param value the reference value
     * @param rank the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count));
    }

    /**
     * Navigate to map items by index range.
     * Server selects "count" map items starting at specified index.
     * 
     * @param index the starting index
     * @param count the number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index, count));
    }
    
    /**
     * Navigate to map items by index range to end.
     * Server selects map items starting at specified index to the end of map.
     * 
     * @param index the starting index
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index));
    }
    
    /**
     * Navigate to map items by rank range.
     * Server selects "count" map items starting at specified rank.
     * 
     * @param rank the starting rank
     * @param count the number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank, count));
    }
    
    /**
     * Navigate to map items by rank range to end.
     * Server selects map items starting at specified rank to the end of map.
     * 
     * @param rank the starting rank
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank));
    }
    
    // Additional value type overloads
    public CdtContextInvertableBuilder<T> onMapValue(double value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onMapValue(boolean value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onMapValue(List<?> value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onMapValue(Map<?,?> value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onMapValue(SpecialValue value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, value.toAerospikeValue()));
    }
    
    // Additional range type overloads
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl))); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl))); 
    }
    // SpecialValue combinations for onMapKeyRange
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue())); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl))); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl))); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl))); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl))); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue())); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue())); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue())); 
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue())); 
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    // SpecialValue combinations for onMapValueRange
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    public CdtContextNonInvertableBuilder<T> onListIndex(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }
    public CdtContextNonInvertableBuilder<T> onListIndex(int index, ListOrder order, boolean pad) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index, order, pad));
    }
    public CdtContextNonInvertableBuilder<T> onListRank(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_RANK, index));
    }
    public CdtContextInvertableBuilder<T> onListValue(long value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onListValue(String value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onListValue(byte[] value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder<T> onListValue(SpecialValue value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, value.toAerospikeValue()));
    }
    
    /**
     * Navigate to map items by a list of keys.
     * 
     * @param keys the list of keys to match
     * @return builder for continued chaining
     */
    public CdtContextInvertableBuilder<T> onMapKeyList(List<?> keys) {
        List<Value> valueList = new ArrayList<>();
        for (Object key : keys) {
            valueList.add(Value.get(key));
        }
        CdtOperationParams params = new CdtOperationParams(CdtOperation.MAP_BY_KEY_LIST, valueList);
        return new CdtGetOrRemoveBuilder<>(this.binName, this.opBuilder, params);
    }
    
    /**
     * Navigate to map items by a list of values.
     * 
     * @param values the list of values to match
     * @return builder for continued chaining
     */
    public CdtContextInvertableBuilder<T> onMapValueList(List<?> values) {
        List<Value> valueList = new ArrayList<>();
        for (Object value : values) {
            valueList.add(Value.get(value));
        }
        CdtOperationParams params = new CdtOperationParams(CdtOperation.MAP_BY_VALUE_LIST, valueList);
        return new CdtGetOrRemoveBuilder<>(this.binName, this.opBuilder, params);
    }

}