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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpReadFlags;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Builder for bin-level read operations in query contexts.
 *
 * <p>This builder is used by {@link ChainableQueryBuilder} for query operations
 * and only supports read operations (no write operations like setTo, insertFrom, etc.)</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Simple read
 * session.query(key)
 *     .bin("name").get()
 *     .bin("ageIn20Years").selectFrom("$.age + 20")
 *     .execute();
 *
 * // CDT read operations
 * session.query(key)
 *     .bin("settings").onMapKey("theme").getValues()
 *     .bin("scores").onListIndex(0).getValues()
 *     .execute();
 * }</pre>
 */
public final class QueryBinBuilder<P> implements CdtOperationAcceptor<P> {
    private final ChainableQueryBuilder queryBuilder;
    private final String binName;
    private final Function<ChainableQueryBuilder, P> wrapParent;

    QueryBinBuilder(ChainableQueryBuilder queryBuilder, String binName) {
        this(queryBuilder, binName, identityParent());
    }

    QueryBinBuilder(ChainableQueryBuilder queryBuilder, String binName, Function<ChainableQueryBuilder, P> wrapParent) {
        this.queryBuilder = queryBuilder;
        this.binName = binName;
        this.wrapParent = wrapParent;
    }

    @SuppressWarnings("unchecked")
    private static <P> Function<ChainableQueryBuilder, P> identityParent() {
        return (Function<ChainableQueryBuilder, P>) Function.<ChainableQueryBuilder>identity();
    }

    private P wrapResult() {
        return wrapParent.apply(queryBuilder);
    }

    // ========================================
    // CdtOperationAcceptor implementation
    // ========================================

    @Override
    public void acceptOp(Operation op) {
        queryBuilder.addOperation(op);
    }

    @Override
    public P getParentBuilder() {
        return wrapResult();
    }

    /**
     * Create a read bin operation.
     *
     * @return the query builder for method chaining
     */
    public P get() {
        queryBuilder.addOperation(Operation.get(binName));
        return wrapResult();
    }

    /**
     * Read the number of entries in the map bin.
     *
     * @return the query builder for method chaining
     */
    public P mapSize() {
        queryBuilder.addOperation(MapOperation.size(binName));
        return wrapResult();
    }

    /**
     * Read the number of elements in the list bin.
     *
     * @return the query builder for method chaining
     */
    public P listSize() {
        queryBuilder.addOperation(ListOperation.size(binName));
        return wrapResult();
    }

    /**
     * Read the element at {@code index} from the list bin.
     *
     * @param index list index (0-based)
     * @return the query builder for method chaining
     */
    public P listGet(int index) {
        queryBuilder.addOperation(ListOperation.get(binName, index));
        return wrapResult();
    }

    /**
     * Read from {@code index} through the end of the list bin.
     *
     * @param index start index (0-based)
     * @return the query builder for method chaining
     */
    public P listGetRange(int index) {
        queryBuilder.addOperation(ListOperation.getRange(binName, index));
        return wrapResult();
    }

    /**
     * Read {@code count} elements starting at {@code index} from the list bin.
     *
     * @param index start index (0-based)
     * @param count number of elements
     * @return the query builder for method chaining
     */
    public P listGetRange(int index, int count) {
        queryBuilder.addOperation(ListOperation.getRange(binName, index, count));
        return wrapResult();
    }

    // ----------------------------------------
    // selectFrom - Read expression operations
    // Supports all 5 AEL input types
    // ----------------------------------------

    /**
     * Read a computed value from this bin using a AEL expression.
     * The result appears as a virtual bin in the returned record.
     *
     * <pre>{@code
     * // Compute total from price and quantity
     * session.query(key)
     *     .bin("total").selectFrom("$.price * $.quantity")
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @see #selectFrom(String, Consumer) for options like ignoreEvalFailure()
     */
    public P selectFrom(String ael) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT));
        return wrapResult();
    }

    /**
     * Read a computed value with options.
     *
     * <pre>{@code
     * // Ignore errors if expression can't evaluate
     * session.query(key)
     *     .bin("ratio").selectFrom("$.a / $.b", opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @param options configure via {@code ignoreEvalFailure()}
     */
    public P selectFrom(String ael, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, opts.getFlags()));
        return wrapResult();
    }

    /**
     * Read a computed value using a programmatic BooleanExpression.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("isAdult").selectFrom(Exp.ge(Exp.intBin("age"), Exp.val(18)))
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     */
    public P selectFrom(BooleanExpression ael) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT));
        return wrapResult();
    }

    /**
     * Read a computed value using a BooleanExpression with options.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("isAdult").selectFrom(myBoolExpr, opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     */
    public P selectFrom(BooleanExpression ael, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, opts.getFlags()));
        return wrapResult();
    }

    /**
     * Read a computed value using a PreparedAel with bound parameters.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.query(key)
     *     .bin("total").selectFrom(calc, quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param params parameter values to bind
     */
    public P selectFrom(PreparedAel ael, Object... params) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, params, ExpReadFlags.DEFAULT));
        return wrapResult();
    }

    /**
     * Read a computed value using a PreparedAel with options and bound parameters.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.a / ?");
     * session.query(key)
     *     .bin("ratio").selectFrom(calc, opt -> opt.ignoreEvalFailure(), divisor)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param options configure via {@code ignoreEvalFailure()}
     * @param params parameter values to bind
     */
    public P selectFrom(PreparedAel ael, Consumer<ExpressionReadOptions> options, Object... params) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, params, opts.getFlags()));
        return wrapResult();
    }

    /**
     * Read a computed value using a low-level Exp expression.
     *
     * <pre>{@code
     * Exp computation = Exp.mul(Exp.intBin("price"), Exp.intBin("quantity"));
     * session.query(key)
     *     .bin("total").selectFrom(computation)
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     */
    public P selectFrom(Exp exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return wrapResult();
    }

    /**
     * Read a computed value using a low-level Exp expression with options.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("ratio").selectFrom(myExp, opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     */
    public P selectFrom(Exp exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return wrapResult();
    }

    /**
     * Read a computed value using a pre-compiled Expression.
     *
     * <pre>{@code
     * Expression compiled = Exp.build(Exp.mul(Exp.intBin("price"), Exp.intBin("qty")));
     * session.query(key)
     *     .bin("total").selectFrom(compiled)
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     */
    public P selectFrom(Expression exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return wrapResult();
    }

    /**
     * Read a computed value using a pre-compiled Expression with options.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("ratio").selectFrom(compiledExp, opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     */
    public P selectFrom(Expression exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return wrapResult();
    }

    // ========================================
    // CDT Navigation Methods (Read-Only)
    // ========================================

    /**
     * Navigate to a map element by index.
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("settings").onMapIndex(0).getValues()
     *     .execute();
     * }</pre>
     *
     * @param index the index to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<P> onMapIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX, index));
    }

    /**
     * Navigate to a map element by key.
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("settings").onMapKey("theme").getValues()
     *     .execute();
     * }</pre>
     *
     * @param key the key to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<P> onMapKey(long key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /** Navigate to a map element by key. */
    public CdtReadContextBuilder<P> onMapKey(String key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /** Navigate to a map element by key. */
    public CdtReadContextBuilder<P> onMapKey(byte[] key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /**
     * Navigate to a map element by rank.
     *
     * @param rank the rank to access (0 = lowest value)
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<P> onMapRank(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK, rank));
    }

    /**
     * Navigate to map elements by value.
     *
     * @param value the value to match
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextInvertableBuilder<P> onMapValue(long value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<P> onMapValue(String value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<P> onMapValue(byte[] value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<P> onMapValue(double value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<P> onMapValue(boolean value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * Navigate to map elements by index range.
     *
     * @param index the starting index
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onMapIndexRange(int index, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index, count));
    }

    /** Navigate to map elements by index range (from index to end). */
    public CdtReadActionInvertableBuilder<P> onMapIndexRange(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index));
    }

    /**
     * Navigate to map elements by key range.
     *
     * @param startIncl inclusive start key
     * @param endExcl exclusive end key
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onMapKeyRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to map elements by key range. */
    public CdtReadActionInvertableBuilder<P> onMapKeyRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map elements by rank range.
     *
     * @param rank the starting rank (0 = lowest value)
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onMapRankRange(int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank, count));
    }

    /** Navigate to map elements by rank range (from rank to end). */
    public CdtReadActionInvertableBuilder<P> onMapRankRange(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank));
    }

    /**
     * Navigate to map elements by value range.
     *
     * @param startIncl inclusive start value
     * @param endExcl exclusive end value
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onMapValueRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to map elements by value range. */
    public CdtReadActionInvertableBuilder<P> onMapValueRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map elements by a list of keys.
     *
     * @param keys the keys to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<P> onMapKeyList(List<?> keys) {
        List<Value> values = new ArrayList<>(keys.size());
        for (Object key : keys) {
            values.add(Value.get(key));
        }
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_LIST, values));
    }

    /**
     * Navigate to map elements by a list of values.
     *
     * @param valueList the values to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<P> onMapValueList(List<?> valueList) {
        List<Value> values = new ArrayList<>(valueList.size());
        for (Object v : valueList) {
            values.add(Value.get(v));
        }
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_LIST, values));
    }

    /**
     * Navigate to a list element by index.
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("scores").onListIndex(0).getValues()
     *     .execute();
     * }</pre>
     *
     * @param index the index to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<P> onListIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }

    /** Navigate to a list element by index with create options. */
    public CdtReadContextBuilder<P> onListIndex(int index, ListOrder order, boolean pad) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index, order, pad));
    }

    /**
     * Navigate to a list element by rank.
     *
     * @param rank the rank to access (0 = lowest value)
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<P> onListRank(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK, rank));
    }

    /**
     * Navigate to list elements by value.
     *
     * @param value the value to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<P> onListValue(long value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /** Navigate to list elements by value. */
    public CdtReadContextInvertableBuilder<P> onListValue(String value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /** Navigate to list elements by value. */
    public CdtReadContextInvertableBuilder<P> onListValue(byte[] value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * Navigate to list elements by index range (from index to end).
     *
     * @param index the starting index
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListIndexRange(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX_RANGE, index));
    }

    /**
     * Navigate to list elements by index range.
     *
     * @param index the starting index
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListIndexRange(int index, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX_RANGE, index, count));
    }

    /**
     * Navigate to list elements by rank range (from rank to end).
     *
     * @param rank the starting rank (0 = lowest value)
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListRankRange(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK_RANGE, rank));
    }

    /**
     * Navigate to list elements by rank range.
     *
     * @param rank the starting rank (0 = lowest value)
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListRankRange(int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK_RANGE, rank, count));
    }

    /**
     * Navigate to list elements by value range.
     *
     * @param startIncl inclusive start value
     * @param endExcl exclusive end value
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListValueRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(byte[] startIncl, byte[] endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(double startIncl, double endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range (SpecialValue combinations). */
    public CdtReadActionInvertableBuilder<P> onListValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(SpecialValue startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(SpecialValue startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(SpecialValue startIncl, double endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(long startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(String startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<P> onListValueRange(double startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * Navigate to list elements by a list of values.
     *
     * @param valueList the values to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<P> onListValueList(List<?> valueList) {
        List<Value> values = new ArrayList<>(valueList.size());
        for (Object v : valueList) {
            values.add(Value.get(v));
        }
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_LIST, values));
    }

    /**
     * Navigate to list elements by value relative to rank range.
     *
     * @param value the reference value
     * @param rank the relative rank offset
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(long value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(String value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(byte[] value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(double value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(SpecialValue value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank));
    }

    /**
     * Navigate to list elements by value relative to rank range with count limit.
     *
     * @param value the reference value
     * @param rank the relative rank offset
     * @param count the maximum number of elements to select
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(long value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(String value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(byte[] value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(double value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<P> onListValueRelativeRankRange(SpecialValue value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count));
    }
}
