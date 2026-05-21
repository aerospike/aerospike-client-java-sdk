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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.path.CdtCollectOptions;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Read-only CDT context navigation interface with invertable operations.
 * 
 * <p>This interface extends {@link CdtReadActionInvertableBuilder} with context navigation
 * methods. Unlike {@link CdtContextInvertableBuilder}, this interface returns read-only
 * builders and does not include any write operations, making it safe for use in query contexts.</p>
 *
 * <p>This interface is returned when navigating to CDT elements that can support the
 * INVERTED flag (e.g., {@code onMapValue()}, {@code onMapKeyList()}).</p>
 *
 * @param <T> the type of the parent builder to return for method chaining
 * @see CdtReadContextBuilder for non-invertable context operations
 * @see CdtContextInvertableBuilder for the read/write version
 */
public interface CdtReadContextInvertableBuilder<T> extends CdtReadActionInvertableBuilder<T> {
    // Map index
    CdtReadContextBuilder<T> onMapIndex(int index);

    // Map index range operations
    CdtReadActionInvertableBuilder<T> onMapIndexRange(int index, int count);
    CdtReadActionInvertableBuilder<T> onMapIndexRange(int index);

    // Map key operations - returns context builder (not setter) for read-only
    CdtReadContextBuilder<T> onMapKey(long key);
    CdtReadContextBuilder<T> onMapKey(String key);
    CdtReadContextBuilder<T> onMapKey(byte[] key);

    // Map key range operations
    CdtReadActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl);
    // SpecialValue combinations for onMapKeyRange
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl);

    // Map key relative index range
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count);

    // Map rank
    CdtReadContextBuilder<T> onMapRank(int index);

    // Map rank range operations
    CdtReadActionInvertableBuilder<T> onMapRankRange(int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapRankRange(int rank);

    // Map value operations
    CdtReadContextInvertableBuilder<T> onMapValue(long value);
    CdtReadContextInvertableBuilder<T> onMapValue(String value);
    CdtReadContextInvertableBuilder<T> onMapValue(byte[] value);
    CdtReadContextInvertableBuilder<T> onMapValue(double value);
    CdtReadContextInvertableBuilder<T> onMapValue(boolean value);
    CdtReadContextInvertableBuilder<T> onMapValue(List<?> value);
    CdtReadContextInvertableBuilder<T> onMapValue(Map<?,?> value);
    CdtReadContextInvertableBuilder<T> onMapValue(SpecialValue value);

    // Map value range
    CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl);
    // SpecialValue combinations for onMapValueRange
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl);

    // Map value relative rank range
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count);

    // Map key and value list operations
    CdtReadContextInvertableBuilder<T> onMapKeyList(List<?> keys);
    CdtReadContextInvertableBuilder<T> onMapValueList(List<?> values);

    // List operations
    CdtReadContextBuilder<T> onListIndex(int index);
    CdtReadContextBuilder<T> onListIndex(int index, ListOrder order, boolean pad);
    CdtReadContextBuilder<T> onListRank(int index);
    CdtReadContextInvertableBuilder<T> onListValue(long value);
    CdtReadContextInvertableBuilder<T> onListValue(String value);
    CdtReadContextInvertableBuilder<T> onListValue(byte[] value);
    CdtReadContextInvertableBuilder<T> onListValue(SpecialValue value);
    CdtReadContextInvertableBuilder<T> onListValue(double value);
    CdtReadContextInvertableBuilder<T> onListValue(boolean value);
    CdtReadContextInvertableBuilder<T> onListValue(List<?> value);
    CdtReadContextInvertableBuilder<T> onListValue(Map<?,?> value);

    // List range operations
    CdtReadActionInvertableBuilder<T> onListIndexRange(int index, int count);
    CdtReadActionInvertableBuilder<T> onListIndexRange(int index);
    CdtReadActionInvertableBuilder<T> onListRankRange(int rank, int count);
    CdtReadActionInvertableBuilder<T> onListRankRange(int rank);
    CdtReadActionInvertableBuilder<T> onListValueRange(long startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(String startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(byte[] startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(double startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(long startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(String startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(byte[] startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(double startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(boolean startIncl, boolean endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(List<?> startIncl, List<?> endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, Map<?,?> endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, boolean endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(boolean startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, List<?> endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(List<?> startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, Map<?,?> endExcl);
    CdtReadActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, SpecialValue endExcl);
    CdtReadContextInvertableBuilder<T> onListValueList(List<?> values);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank, int count);

    // --- Path iteration (read), server 8.1.1+ ---

    /**
     * Descend into every child at the current path using {@link com.aerospike.client.sdk.cdt.CTX#allChildren()}.
     *
     * <p>Requires at least one {@code onEachChild()} (or filtered variant) before a {@code collect*}
     * terminal on this path. Intended for {@code session.query(...)} read chains.</p>
     *
     * <p><b>Example</b> — read every title under {@code catalog.book[*].title}:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("title").collectValues()
     *     .execute();
     * }</pre>
     *
     * @return this read path builder for further navigation or a read terminal
     * @see CdtContextNonInvertableBuilder#onEachChild()
     */
    CdtReadContextBuilder<T> onEachChild();

    /**
     * Descend into children at the current path that match {@code filter}
     * ({@link com.aerospike.client.sdk.cdt.CTX#allChildrenWithFilter(com.aerospike.client.sdk.exp.Exp)}).
     *
     * @param filter server-side {@link Exp} predicate evaluated for each candidate child
     * @return this read path builder for further navigation or a read terminal
     * @see CdtContextNonInvertableBuilder#onEachChild(Exp)
     */
    CdtReadContextBuilder<T> onEachChild(Exp filter);

    /**
     * Same as {@link #onEachChild(Exp)} with the filter expressed as AEL text.
     *
     * @param ael AEL predicate for {@code allChildrenWithFilter}
     * @return this read path builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    CdtReadContextBuilder<T> onEachChild(String ael);

    /**
     * Same as {@link #onEachChild(String)} with bound parameters for a {@link PreparedAel} template.
     *
     * @param ael prepared AEL template
     * @param bindParams values bound to placeholders in {@code ael}
     * @return this read path builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    CdtReadContextBuilder<T> onEachChild(PreparedAel ael, Object... bindParams);

    /**
     * Read terminal: flat list of matched leaf values via CDT {@code selectByPath}
     * ({@link com.aerospike.client.sdk.cdt.SelectFlags#VALUE}).
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().onMapKey("price").collectValues()
     *     .execute();
     * }</pre>
     *
     * @return the query builder after appending the CDT read op
     */
    T collectValues();

    /**
     * Same as {@link #collectValues()} with {@link CdtCollectOptions} (e.g. {@code NO_FAIL}).
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("items").onEachChild().collectValues(o -> o.noFail(true))
     *     .execute();
     * }</pre>
     *
     * @param options consumer that configures {@link CdtCollectOptions}
     * @return the query builder after appending the CDT read op
     */
    T collectValues(Consumer<CdtCollectOptions> options);

    /**
     * Read terminal: matched map keys ({@link com.aerospike.client.sdk.cdt.SelectFlags#MAP_KEY}).
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectKeys().execute();
     * }</pre>
     *
     * @return the query builder after appending the CDT read op
     */
    T collectKeys();

    /**
     * Same as {@link #collectKeys()} with {@link CdtCollectOptions}.
     *
     * @param options select flag customization
     * @return the query builder after appending the CDT read op
     */
    T collectKeys(Consumer<CdtCollectOptions> options);

    /**
     * Read terminal: matched map key/value pairs
     * ({@link com.aerospike.client.sdk.cdt.SelectFlags#MAP_KEY_VALUE}).
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectKeyValues().execute();
     * }</pre>
     *
     * @return the query builder after appending the CDT read op
     */
    T collectKeyValues();

    /**
     * Same as {@link #collectKeyValues()} with {@link CdtCollectOptions}.
     *
     * @param options select flag customization
     * @return the query builder after appending the CDT read op
     */
    T collectKeyValues(Consumer<CdtCollectOptions> options);

    /**
     * Read terminal: structure-preserving matching tree
     * ({@link com.aerospike.client.sdk.cdt.SelectFlags#MATCHING_TREE}).
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectTree().execute();
     * }</pre>
     *
     * @return the query builder after appending the CDT read op
     */
    T collectTree();

    /**
     * Same as {@link #collectTree()} with {@link CdtCollectOptions}.
     *
     * @param options select flag customization
     * @return the query builder after appending the CDT read op
     */
    T collectTree(Consumer<CdtCollectOptions> options);

    /**
     * Read terminal: same selection as {@link #collectValues()} but delivered as an expression read
     * ({@code EXP_READ}) on the query projection bin.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("title")
     *     .collectValuesAsExpressionRead(Exp.Type.MAP, Exp.Type.LIST)
     *     .execute();
     * }</pre>
     *
     * @param binValueType top-level type of the source bin (e.g. {@link com.aerospike.client.sdk.exp.Exp.Type#MAP})
     * @param resultType expected result type of the embedded {@code selectByPath}
     * @return the query builder after appending the expression read op
     * @see CdtContextNonInvertableBuilder#collectValuesAsExpressionRead(Exp.Type, Exp.Type)
     */
    T collectValuesAsExpressionRead(Exp.Type binValueType, Exp.Type resultType);

    /**
     * Same as {@link #collectValuesAsExpressionRead(Exp.Type, Exp.Type)} with explicit select and read flags.
     *
     * <p><b>Example</b> — honor {@code NO_FAIL} on both select and read:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("sku")
     *     .collectValuesAsExpressionRead(
     *         Exp.Type.MAP, Exp.Type.LIST,
     *         SelectFlags.VALUE | SelectFlags.NO_FAIL,
     *         ExpReadFlags.DEFAULT)
     *     .execute();
     * }</pre>
     *
     * @param binValueType top-level bin type for the inner bin expression
     * @param resultType expected result type of {@code selectByPath}
     * @param selectFlags {@link com.aerospike.client.sdk.cdt.SelectFlags} bitmask for {@link com.aerospike.client.sdk.exp.CdtExp#selectByPath}
     * @param readFlags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask for the expression read op
     * @return the query builder after appending the expression read op
     */
    T collectValuesAsExpressionRead(Exp.Type binValueType, Exp.Type resultType, int selectFlags, int readFlags);

    /**
     * Same as {@link #collectValuesAsExpressionRead(Exp.Type, Exp.Type)} with {@link ExpressionReadOptions}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("title")
     *     .collectValuesAsExpressionRead(Exp.Type.MAP, Exp.Type.LIST, o -> o.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param binValueType top-level bin type for the inner bin expression
     * @param resultType expected result type of {@code selectByPath}
     * @param options consumer that configures {@link ExpressionReadOptions}
     * @return the query builder after appending the expression read op
     */
    T collectValuesAsExpressionRead(Exp.Type binValueType, Exp.Type resultType, Consumer<ExpressionReadOptions> options);


    // Read-only terminal operations

    /**
     * Get the size of the map at the current CDT path.
     * @return the parent builder for method chaining
     */
    T mapSize();

    /**
     * Get the number of elements in the list at the current CDT path.
     * @return the parent builder for method chaining
     */
    T listSize();

    /**
     * Read the element at {@code index} from the list at the current CDT path.
     * @param index list index (0-based)
     * @return the parent builder for method chaining
     */
    T listGet(int index);

    /**
     * Read from {@code index} through the end of the list at the current CDT path.
     * @param index start index (0-based)
     * @return the parent builder for method chaining
     */
    T listGetRange(int index);

    /**
     * Read {@code count} elements starting at {@code index} from the list at the current CDT path.
     * @param index start index (0-based)
     * @param count number of elements
     * @return the parent builder for method chaining
     */
    T listGetRange(int index, int count);
}
