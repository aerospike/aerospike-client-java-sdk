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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.CdtOperationAcceptor;
import com.aerospike.client.sdk.CdtOperationParams;
import com.aerospike.client.sdk.CdtReadActionInvertableBuilder;
import com.aerospike.client.sdk.CdtReadContextBuilder;
import com.aerospike.client.sdk.CdtReadContextInvertableBuilder;
import com.aerospike.client.sdk.CdtReadOnlyBuilder;
import com.aerospike.client.sdk.ExpressionOpHelper;
import com.aerospike.client.sdk.ExpressionReadOptions;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.Value.HLLValue;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpReadFlags;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.operation.HLLOperation;

/**
 * Builder for bin-level read operations in QueryBuilder contexts.
 *
 * <p>This builder is used by {@link QueryBuilder} for query operations
 * and only supports read operations (get, selectFrom, and CDT read operations).</p>
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
 *
 * <p><b>Note:</b> For dataset-based queries (scans/index queries), CDT operations are
 * not currently supported by the server and will throw an exception at execution time.
 * This API is forward-looking to support future server capabilities.</p>
 */
public class QueryBuilderBinBuilder implements CdtOperationAcceptor<QueryBuilder> {
    private final QueryBuilder queryBuilder;
    private final String binName;

    QueryBuilderBinBuilder(QueryBuilder queryBuilder, String binName) {
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
    public QueryBuilder getParentBuilder() {
        return queryBuilder;
    }

    /**
     * Create a read bin operation.
     *
     * @return the query builder for method chaining
     */
    public QueryBuilder get() {
        queryBuilder.addOperation(Operation.get(binName));
        return queryBuilder;
    }

    /**
     * Read the number of entries in the map bin.
     *
     * @return the query builder for method chaining
     */
    public QueryBuilder mapSize() {
        queryBuilder.addOperation(MapOperation.size(binName));
        return queryBuilder;
    }

    /**
     * Read the number of elements in the list bin.
     *
     * @return the query builder for method chaining
     */
    public QueryBuilder listSize() {
        queryBuilder.addOperation(ListOperation.size(binName));
        return queryBuilder;
    }

    /**
     * Read the element at {@code index} from the list bin.
     *
     * @param index list index (0-based)
     * @return the query builder for method chaining
     */
    public QueryBuilder listGet(int index) {
        queryBuilder.addOperation(ListOperation.get(binName, index));
        return queryBuilder;
    }

    /**
     * Read from {@code index} through the end of the list bin.
     *
     * @param index start index (0-based)
     * @return the query builder for method chaining
     */
    public QueryBuilder listGetRange(int index) {
        queryBuilder.addOperation(ListOperation.getRange(binName, index));
        return queryBuilder;
    }

    /**
     * Read {@code count} elements starting at {@code index} from the list bin.
     *
     * @param index start index (0-based)
     * @param count number of elements
     * @return the query builder for method chaining
     */
    public QueryBuilder listGetRange(int index, int count) {
        queryBuilder.addOperation(ListOperation.getRange(binName, index, count));
        return queryBuilder;
    }

    // ----------------------------------------
    // selectFrom - Read expression operations
    // Supports all 5 AEL input types
    // ----------------------------------------

    /** Create a read expression from a AEL string. */
    public QueryBuilder selectFrom(String ael) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from a AEL string with options. */
    public QueryBuilder selectFrom(String ael, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from a BooleanExpression. */
    public QueryBuilder selectFrom(BooleanExpression ael) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from a BooleanExpression with options. */
    public QueryBuilder selectFrom(BooleanExpression ael, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from a PreparedAel. */
    public QueryBuilder selectFrom(PreparedAel ael, Object... params) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, params, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from a PreparedAel with options. */
    public QueryBuilder selectFrom(PreparedAel ael, Consumer<ExpressionReadOptions> options, Object... params) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, ael, params, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from an Exp. */
    public QueryBuilder selectFrom(Exp exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from an Exp with options. */
    public QueryBuilder selectFrom(Exp exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from an Expression. */
    public QueryBuilder selectFrom(Expression exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from an Expression with options. */
    public QueryBuilder selectFrom(Expression exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return queryBuilder;
    }

    // ========================================
    // CDT Navigation Methods (Read-Only)
    // Note: Not currently supported by server for dataset queries (scan/index).
    // API included for future support. Will throw at execution time if used.
    // ========================================

    /**
     * Navigate to a map element by index.
     *
     * @param index the index to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<QueryBuilder> onMapIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX, index));
    }

    /**
     * Navigate to a map element by key.
     *
     * @param key the key to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<QueryBuilder> onMapKey(long key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /** Navigate to a map element by key. */
    public CdtReadContextBuilder<QueryBuilder> onMapKey(String key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /** Navigate to a map element by key. */
    public CdtReadContextBuilder<QueryBuilder> onMapKey(byte[] key) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /**
     * Navigate to a map element by rank.
     *
     * @param rank the rank to access (0 = lowest value)
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<QueryBuilder> onMapRank(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK, rank));
    }

    /**
     * Navigate to map elements by value.
     *
     * @param value the value to match
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapValue(long value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapValue(String value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapValue(byte[] value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapValue(double value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /** Navigate to map elements by value. */
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapValue(boolean value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * Navigate to map elements by index range.
     *
     * @param index the starting index
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapIndexRange(int index, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index, count));
    }

    /** Navigate to map elements by index range (from index to end). */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapIndexRange(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index));
    }

    /**
     * Navigate to map elements by key range.
     *
     * @param startIncl inclusive start key
     * @param endExcl exclusive end key
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapKeyRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to map elements by key range. */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapKeyRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map elements by rank range.
     *
     * @param rank the starting rank (0 = lowest value)
     * @param count the number of elements
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapRankRange(int rank, int count) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank, count));
    }

    /** Navigate to map elements by rank range (from rank to end). */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapRankRange(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank));
    }

    /**
     * Navigate to map elements by value range.
     *
     * @param startIncl inclusive start value
     * @param endExcl exclusive end value
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapValueRange(long startIncl, long endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /** Navigate to map elements by value range. */
    public CdtReadActionInvertableBuilder<QueryBuilder> onMapValueRange(String startIncl, String endExcl) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map elements by a list of keys.
     *
     * @param keys the keys to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapKeyList(List<?> keys) {
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
    public CdtReadContextInvertableBuilder<QueryBuilder> onMapValueList(List<?> valueList) {
        List<Value> values = new ArrayList<>(valueList.size());
        for (Object v : valueList) {
            values.add(Value.get(v));
        }
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_LIST, values));
    }

    /**
     * Navigate to a list element by index.
     *
     * @param index the index to access
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<QueryBuilder> onListIndex(int index) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }

    /** Navigate to a list element by index with create options. */
    public CdtReadContextBuilder<QueryBuilder> onListIndex(int index, ListOrder order, boolean pad) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index, order, pad));
    }

    /**
     * Navigate to a list element by rank.
     *
     * @param rank the rank to access (0 = lowest value)
     * @return read-only CDT builder for further navigation or terminal operations
     */
    public CdtReadContextBuilder<QueryBuilder> onListRank(int rank) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_RANK, rank));
    }

    /**
     * Navigate to list elements by value.
     *
     * @param value the value to match
     * @return read-only CDT builder for terminal operations
     */
    public CdtReadContextInvertableBuilder<QueryBuilder> onListValue(long value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /** Navigate to list elements by value. */
    public CdtReadContextInvertableBuilder<QueryBuilder> onListValue(String value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /** Navigate to list elements by value. */
    public CdtReadContextInvertableBuilder<QueryBuilder> onListValue(byte[] value) {
        return new CdtReadOnlyBuilder<>(binName, this, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    // ----------------------------------------
    // HyperLogLog (HLL)
    // ----------------------------------------

    /**
     * Read the estimated cardinality of the HLL bin.
     *
     * <p>Server returns the estimated number of unique elements in the bin
     * as a long.</p>
     *
     * @return the query builder for method chaining
     */
    public QueryBuilder hllGetCount() {
        Operation op = HLLOperation.getCount(binName);
        queryBuilder.addOperation(op);
        return queryBuilder;
    }

    /**
     * Describe the HLL bin's configuration.
     *
     * <p>Server returns a list of two longs containing the {@code indexBitCount}
     * and {@code minHashBitCount} that were used to create the bin.</p>
     *
     * @return the query builder for method chaining
     */
    public QueryBuilder hllDescribe() {
        Operation op = HLLOperation.describe(binName);
        queryBuilder.addOperation(op);
        return queryBuilder;
    }

    /**
     * Read the union of the HLL bin with the supplied HLL values.
     *
     * <p>Server returns an HLL value that is the union of {@code hlls} together
     * with the bin's current contents. The bin itself is not modified.</p>
     *
     * @param hlls HLL values to union with the bin
     * @return the query builder for method chaining
     */
    public QueryBuilder hllGetUnion(List<HLLValue> hlls) {
        Operation op = HLLOperation.getUnion(binName, hlls);
        queryBuilder.addOperation(op);
        return queryBuilder;
    }

    /**
     * Read the estimated count of the union of the HLL bin with the supplied
     * HLL values.
     *
     * <p>Server returns the estimated number of unique elements in the union
     * of {@code hlls} with the bin's current contents. The bin itself is not
     * modified.</p>
     *
     * @param hlls HLL values to union with the bin
     * @return the query builder for method chaining
     */
    public QueryBuilder hllGetUnionCount(List<HLLValue> hlls) {
        Operation op = HLLOperation.getUnionCount(binName, hlls);
        queryBuilder.addOperation(op);
        return queryBuilder;
    }

    /**
     * Read the estimated count of the intersection of the HLL bin with the
     * supplied HLL values.
     *
     * <p>Server returns the estimated number of elements contained in the
     * intersection of {@code hlls} with the bin. The {@code hlls} list may
     * contain at most two values when minhash bits are 0; more are allowed
     * when minhash bits are nonzero.</p>
     *
     * @param hlls HLL values to intersect with the bin
     * @return the query builder for method chaining
     */
    public QueryBuilder hllGetIntersectCount(List<HLLValue> hlls) {
        Operation op = HLLOperation.getIntersectCount(binName, hlls);
        queryBuilder.addOperation(op);
        return queryBuilder;
    }

    /**
     * Read the estimated Jaccard similarity of the HLL bin with the supplied
     * HLL values.
     *
     * <p>Server returns a double in {@code [0.0, 1.0]} estimating the
     * similarity of the bin to {@code hlls}. The {@code hlls} list may
     * contain at most two values when minhash bits are 0; more are allowed
     * when minhash bits are nonzero.</p>
     *
     * @param hlls HLL values to compare against the bin
     * @return the query builder for method chaining
     */
    public QueryBuilder hllGetSimilarity(List<HLLValue> hlls) {
        Operation op = HLLOperation.getSimilarity(binName, hlls);
        queryBuilder.addOperation(op);
        return queryBuilder;
    }
}
