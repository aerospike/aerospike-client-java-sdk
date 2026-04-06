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

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.cdt.ListOrder;
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
public class QueryBinBuilder implements CdtOperationAcceptor<ChainableQueryBuilder> {
    private final ChainableQueryBuilder queryBuilder;
    private final String binName;

    QueryBinBuilder(ChainableQueryBuilder queryBuilder, String binName) {
        this.queryBuilder = queryBuilder;
        this.binName = binName;
    }

    // ========================================
    // CdtOperationAcceptor implementation
    // ========================================

    @Override
    public void acceptOp(Operation op) {
        queryBuilder.addOperation(op);
    }

    @Override
    public ChainableQueryBuilder getParentBuilder() {
        return queryBuilder;
    }

    /**
     * Create a read bin operation.
     *
     * @return the query builder for method chaining
     */
    public ChainableQueryBuilder get() {
        queryBuilder.addOperation(Operation.get(binName));
        return queryBuilder;
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
     * @param dsl the AEL expression string
     * @see #selectFrom(String, Consumer) for options like ignoreEvalFailure()
     */
    public ChainableQueryBuilder selectFrom(String dsl) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, ExpReadFlags.DEFAULT));
        return queryBuilder;
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
     * @param dsl the AEL expression string
     * @param options configure via {@code ignoreEvalFailure()}
     */
    public ChainableQueryBuilder selectFrom(String dsl, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, opts.getFlags()));
        return queryBuilder;
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
     * @param dsl the boolean expression to evaluate
     */
    public ChainableQueryBuilder selectFrom(BooleanExpression dsl) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, ExpReadFlags.DEFAULT));
        return queryBuilder;
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
     * @param dsl the boolean expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     */
    public ChainableQueryBuilder selectFrom(BooleanExpression dsl, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, opts.getFlags()));
        return queryBuilder;
    }

    /**
     * Read a computed value using a PreparedDsl with bound parameters.
     *
     * <pre>{@code
     * PreparedDsl calc = PreparedDsl.prepare("$.price * ?");
     * session.query(key)
     *     .bin("total").selectFrom(calc, quantity)
     *     .execute();
     * }</pre>
     *
     * @param dsl the prepared AEL statement
     * @param params parameter values to bind
     */
    public ChainableQueryBuilder selectFrom(PreparedAel dsl, Object... params) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, params, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /**
     * Read a computed value using a PreparedDsl with options and bound parameters.
     *
     * <pre>{@code
     * PreparedDsl calc = PreparedDsl.prepare("$.a / ?");
     * session.query(key)
     *     .bin("ratio").selectFrom(calc, opt -> opt.ignoreEvalFailure(), divisor)
     *     .execute();
     * }</pre>
     *
     * @param dsl the prepared AEL statement
     * @param options configure via {@code ignoreEvalFailure()}
     * @param params parameter values to bind
     */
    public ChainableQueryBuilder selectFrom(PreparedAel dsl, Consumer<ExpressionReadOptions> options, Object... params) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, params, opts.getFlags()));
        return queryBuilder;
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
    public ChainableQueryBuilder selectFrom(Exp exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return queryBuilder;
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
    public ChainableQueryBuilder selectFrom(Exp exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return queryBuilder;
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
    public ChainableQueryBuilder selectFrom(Expression exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return queryBuilder;
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
    public ChainableQueryBuilder selectFrom(Expression exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return queryBuilder;
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
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapIndex(int index) {
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
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapKey(long key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /** Navigate to a map element by key. */
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapKey(String key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /** Navigate to a map element by key. */
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapKey(byte[] key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /**
     * Navigate to a map element by rank.
     *
     * @param rank the rank to access (0 = lowest value)
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<ChainableQueryBuilder> onMapRank(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK, rank));
    }

    /**
     * Navigate to map elements by value.
     *
     * @param value the value to match
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapValue(long value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapValue(String value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapValue(byte[] value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapValue(double value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapValue(boolean value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * Navigate to map elements by index range.
     *
     * @param index the starting index
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapIndexRange(int index, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index, count));
    }

    /** Navigate to map elements by index range (from index to end). */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapIndexRange(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index));
    }

    /**
     * Navigate to map elements by key range.
     *
     * @param startIncl inclusive start key
     * @param endExcl exclusive end key
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapKeyRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to map elements by key range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapKeyRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map elements by rank range.
     *
     * @param rank the starting rank (0 = lowest value)
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapRankRange(int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank, count));
    }

    /** Navigate to map elements by rank range (from rank to end). */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapRankRange(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank));
    }

    /**
     * Navigate to map elements by value range.
     *
     * @param startIncl inclusive start value
     * @param endExcl exclusive end value
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapValueRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to map elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onMapValueRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map elements by a list of keys.
     *
     * @param keys the keys to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapKeyList(List<?> keys) {
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
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onMapValueList(List<?> valueList) {
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
    public CdtReadContextBuilder<ChainableQueryBuilder> onListIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }

    /** Navigate to a list element by index with create options. */
    public CdtReadContextBuilder<ChainableQueryBuilder> onListIndex(int index, ListOrder order, boolean pad) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index, order, pad));
    }

    /**
     * Navigate to a list element by rank.
     *
     * @param rank the rank to access (0 = lowest value)
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<ChainableQueryBuilder> onListRank(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK, rank));
    }

    /**
     * Navigate to list elements by value.
     *
     * @param value the value to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onListValue(long value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /** Navigate to list elements by value. */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onListValue(String value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /** Navigate to list elements by value. */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onListValue(byte[] value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * Navigate to list elements by index range (from index to end).
     *
     * @param index the starting index
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListIndexRange(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX_RANGE, index));
    }

    /**
     * Navigate to list elements by index range.
     *
     * @param index the starting index
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListIndexRange(int index, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX_RANGE, index, count));
    }

    /**
     * Navigate to list elements by rank range (from rank to end).
     *
     * @param rank the starting rank (0 = lowest value)
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListRankRange(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK_RANGE, rank));
    }

    /**
     * Navigate to list elements by rank range.
     *
     * @param rank the starting rank (0 = lowest value)
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListRankRange(int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK_RANGE, rank, count));
    }

    /**
     * Navigate to list elements by value range.
     *
     * @param startIncl inclusive start value
     * @param endExcl exclusive end value
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(byte[] startIncl, byte[] endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(double startIncl, double endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range (SpecialValue combinations). */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(SpecialValue startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(SpecialValue startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(SpecialValue startIncl, double endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(long startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(String startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /** Navigate to list elements by value range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRange(double startIncl, SpecialValue endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * Navigate to list elements by a list of values.
     *
     * @param valueList the values to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<ChainableQueryBuilder> onListValueList(List<?> valueList) {
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
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(long value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(String value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(byte[] value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(double value, int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /** Navigate to list elements by value relative to rank range. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(SpecialValue value, int rank) {
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
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(long value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(String value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(byte[] value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(double value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /** Navigate to list elements by value relative to rank range with count limit. */
    public CdtReadActionInvertableBuilder<ChainableQueryBuilder> onListValueRelativeRankRange(SpecialValue value, int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count));
    }
}
