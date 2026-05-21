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
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.path.CdtCollectOptions;
import com.aerospike.client.sdk.cdt.path.CdtModifyOptions;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * This interface handles operations at the end of contexts. Note that some of these methods
 * like onMapValueRangemust be at the end of a context and hence must be followed by an action
 * (CdtAction* return types), others (like onMapIndex) are context items which can be followed
 * either by other context paths or by an action (CdtContext* return types), and onMapKey which
 * can be followed a context path, an action (get or remove) or can be used to set the value, and
 * hence returns a CdtSetter* method.
 * <p/>
 * Note that some methods are invertable (ie can support the INVERTED flag) and others aren't.
 * For example, onMapIndex returns a single value, hence cannot support the INVERTED flag.
 * onMapValue returns a list of values and hence can be inverted.
 * <p/>
 * Note that this is a paired interface with {@link CdtContextInvertableBuilder} and they have exactly
 * the same methods, differing only in the interface they extend.
 */
public interface CdtContextNonInvertableBuilder<T extends AbstractOperationBuilder<T>> extends CdtActionNonInvertableBuilder<T> {
    // Map index
    public CdtContextNonInvertableBuilder<T> onMapIndex(int index);

    // Map index range operations
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index, int count);
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index);

    // Map key operations
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key);
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key);
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key);
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key, MapOrder createType);
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key, MapOrder createType);
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key, MapOrder createType);

    // Map key range operations
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl);
    // SpecialValue combinations for onMapKeyRange
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl);

    // Map key relative rank range
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count);

    // Map rank
    public CdtContextNonInvertableBuilder<T> onMapRank(int index);

    // Map rank range operations
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank, int count);
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank);

    // Map value operations
    public CdtContextInvertableBuilder<T> onMapValue(long value);
    public CdtContextInvertableBuilder<T> onMapValue(String value);
    public CdtContextInvertableBuilder<T> onMapValue(byte[] value);
    public CdtContextInvertableBuilder<T> onMapValue(double value);
    public CdtContextInvertableBuilder<T> onMapValue(boolean value);
    public CdtContextInvertableBuilder<T> onMapValue(List<?> value);
    public CdtContextInvertableBuilder<T> onMapValue(Map<?,?> value);
    public CdtContextInvertableBuilder<T> onMapValue(SpecialValue value);

    // Map value range
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl);
    // SpecialValue combinations for onMapValueRange
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl);

    // Map value relative rank range
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count);

    // Map key and value list operations
    public CdtContextInvertableBuilder<T> onMapKeyList(List<?> keys);
    public CdtContextInvertableBuilder<T> onMapValueList(List<?> values);

    public CdtContextNonInvertableBuilder<T> onListIndex(int index);
    public CdtContextNonInvertableBuilder<T> onListIndex(int index, ListOrder order, boolean pad);
    public CdtContextNonInvertableBuilder<T> onListRank(int index);
    public CdtContextInvertableBuilder<T> onListValue(long value);
    public CdtContextInvertableBuilder<T> onListValue(String value);
    public CdtContextInvertableBuilder<T> onListValue(byte[] value);
    public CdtContextInvertableBuilder<T> onListValue(SpecialValue value);
    public CdtContextInvertableBuilder<T> onListValue(double value);
    public CdtContextInvertableBuilder<T> onListValue(boolean value);
    public CdtContextInvertableBuilder<T> onListValue(List<?> value);
    public CdtContextInvertableBuilder<T> onListValue(Map<?,?> value);

    // List index range
    public CdtActionInvertableBuilder<T> onListIndexRange(int index);
    public CdtActionInvertableBuilder<T> onListIndexRange(int index, int count);

    // List rank range
    public CdtActionInvertableBuilder<T> onListRankRange(int rank);
    public CdtActionInvertableBuilder<T> onListRankRange(int rank, int count);

    // List value range
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(boolean startIncl, boolean endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(List<?> startIncl, List<?> endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, Map<?,?> endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, boolean endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(boolean startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, List<?> endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(List<?> startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, Map<?,?> endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, SpecialValue endExcl);

    // List value list
    public CdtContextInvertableBuilder<T> onListValueList(java.util.List<?> values);

    // List value relative rank range
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank, int count);

    // --- Path iteration (CTX.allChildren / selectByPath / modifyByPath), server 8.1.1+ ---

    /**
     * Descend into every child at the current path using {@link com.aerospike.client.sdk.cdt.CTX#allChildren()}.
     *
     * <p>At least one {@code onEachChild()} (or filtered variant) is required before {@code collect*},
     * {@code modifyBy}, or {@code removeMatches}; it records the path used by
     * {@link com.aerospike.client.sdk.cdt.CdtOperation#selectByPath} /
     * {@link com.aerospike.client.sdk.cdt.CdtOperation#modifyByPath}.</p>
     *
     * <p><b>Example</b> — bump every integer in a top-level list bin {@code nums}:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("nums").onEachChild()
     *     .modifyBy(Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(1)))
     *     .execute();
     * }</pre>
     *
     * @return this path builder for further {@code onMapKey}, {@code onEachChild}, or a terminal
     */
    CdtContextNonInvertableBuilder<T> onEachChild();

    /**
     * Descend into children at the current path that match {@code filter}
     * ({@link com.aerospike.client.sdk.cdt.CTX#allChildrenWithFilter(com.aerospike.client.sdk.exp.Exp)}).
     *
     * <p>The filter is evaluated in the server's path-expression context (loop variables such as
     * {@link com.aerospike.client.sdk.exp.LoopVarPart#VALUE} refer to the candidate child).</p>
     *
     * <p><b>Example</b> — remove list elements greater than 5:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("nums").onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5)))
     *     .removeMatches()
     *     .execute();
     * }</pre>
     *
     * @param filter server-side {@link Exp} predicate; children where it is false are skipped
     * @return this path builder for further navigation or a terminal
     */
    CdtContextNonInvertableBuilder<T> onEachChild(Exp filter);

    /**
     * Same as {@link #onEachChild(Exp)} with the filter expressed as AEL text.
     *
     * <p><b>Status:</b> not implemented yet — throws {@link UnsupportedOperationException} until the
     * AEL compiler supports path-scoped fragments (see {@code docs/ael/path-expressions.md}).</p>
     *
     * @param ael AEL predicate for {@code allChildrenWithFilter}
     * @return this path builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    CdtContextNonInvertableBuilder<T> onEachChild(String ael);

    /**
     * Same as {@link #onEachChild(String)} with bound parameters for a {@link PreparedAel} template.
     *
     * @param ael prepared AEL template
     * @param bindParams values bound to placeholders in {@code ael}
     * @return this path builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    CdtContextNonInvertableBuilder<T> onEachChild(PreparedAel ael, Object... bindParams);

    /**
     * Terminal read: return matched leaf <strong>values</strong> as a flat list via CDT
     * {@code selectByPath} with {@link com.aerospike.client.sdk.cdt.SelectFlags#VALUE}.
     *
     * <p>Requires at least one {@link #onEachChild()} segment on the path. Does not use the expression
     * read opcode; see {@link #collectValuesAsExpressionRead} for {@code EXP_READ}.</p>
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("title").collectValues()
     *     .execute();
     * }</pre>
     *
     * @return the outer operation builder (e.g. upsert/query chain) after appending the CDT read op
     */
    T collectValues();

    /**
     * Same as {@link #collectValues()} with extra select flags (for example {@link com.aerospike.client.sdk.cdt.SelectFlags#NO_FAIL})
     * supplied through {@link CdtCollectOptions}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("items").onEachChild().collectValues(o -> o.noFail(true))
     *     .execute();
     * }</pre>
     *
     * @param options consumer that configures {@link CdtCollectOptions} (e.g. {@code noFail(true)})
     * @return the outer operation builder after appending the CDT read op
     */
    T collectValues(Consumer<CdtCollectOptions> options);

    /**
     * Terminal read: return matched <strong>map keys</strong> (map contexts only) via
     * {@link com.aerospike.client.sdk.cdt.SelectFlags#MAP_KEY}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectKeys().execute();
     * }</pre>
     *
     * @return the outer operation builder after appending the CDT read op
     * @throws IllegalArgumentException if the current path is not map-typed where keys apply
     */
    T collectKeys();

    /**
     * Same as {@link #collectKeys()} with {@link CdtCollectOptions}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectKeys(o -> o.noFail(true)).execute();
     * }</pre>
     *
     * @param options select flag customization
     * @return the outer operation builder after appending the CDT read op
     */
    T collectKeys(Consumer<CdtCollectOptions> options);

    /**
     * Terminal read: return matched map entries as {@code (key, value)} pairs via
     * {@link com.aerospike.client.sdk.cdt.SelectFlags#MAP_KEY_VALUE}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectKeyValues().execute();
     * }</pre>
     *
     * @return the outer operation builder after appending the CDT read op
     */
    T collectKeyValues();

    /**
     * Same as {@link #collectKeyValues()} with {@link CdtCollectOptions}.
     *
     * @param options select flag customization
     * @return the outer operation builder after appending the CDT read op
     */
    T collectKeyValues(Consumer<CdtCollectOptions> options);

    /**
     * Terminal read: return a structure-preserving <strong>tree</strong> of matches via
     * {@link com.aerospike.client.sdk.cdt.SelectFlags#MATCHING_TREE}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("book").onEachChild().collectTree().execute();
     * }</pre>
     *
     * @return the outer operation builder after appending the CDT read op
     */
    T collectTree();

    /**
     * Same as {@link #collectTree()} with {@link CdtCollectOptions}.
     *
     * @param options select flag customization
     * @return the outer operation builder after appending the CDT read op
     */
    T collectTree(Consumer<CdtCollectOptions> options);

    /**
     * Terminal write: apply {@code modifyExp} at each path match via CDT {@code modifyByPath}.
     *
     * <p>Use loop-variable {@link Exp} forms (e.g. {@code floatLoopVar(LoopVarPart.VALUE)}) inside
     * {@code modifyExp} to read the leaf being modified.</p>
     *
     * <p><b>Example</b> — scale every matched price by 1.1:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("price")
     *     .modifyBy(Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(1.1)))
     *     .execute();
     * }</pre>
     *
     * @param modifyExp modification sub-expression; compiled with {@link Exp#build(Exp)}
     * @return the outer operation builder after appending the CDT modify op
     */
    T modifyBy(Exp modifyExp);

    /**
     * Same as {@link #modifyBy(Exp)} with {@link CdtModifyOptions} (for example {@link com.aerospike.client.sdk.cdt.ModifyFlags#NO_FAIL}).
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("price")
     *     .modifyBy(Exp.mul(Exp.floatLoopVar(LoopVarPart.VALUE), Exp.val(1.05)), o -> o.noFail(true))
     *     .execute();
     * }</pre>
     *
     * @param modifyExp modification sub-expression
     * @param options consumer that configures {@link CdtModifyOptions}
     * @return the outer operation builder after appending the CDT modify op
     */
    T modifyBy(Exp modifyExp, Consumer<CdtModifyOptions> options);

    /**
     * Same as {@link #modifyBy(Exp)} with the body expressed as AEL.
     *
     * @param ael AEL text compiled to the modify expression once path-scoped AEL is supported
     * @return the outer operation builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     * @see #onEachChild(String)
     */
    T modifyBy(String ael);

    /**
     * Same as {@link #modifyBy(String)} with {@link CdtModifyOptions}.
     *
     * @param ael AEL modify body
     * @param options modify options consumer
     * @return the outer operation builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    T modifyBy(String ael, Consumer<CdtModifyOptions> options);

    /**
     * Same as {@link #modifyBy(Exp)} using a prepared AEL template and bind parameters.
     *
     * @param ael prepared AEL template
     * @param bindParams bind values for {@code ael}
     * @return the outer operation builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    T modifyBy(PreparedAel ael, Object... bindParams);

    /**
     * Same as {@link #modifyBy(PreparedAel, Object...)} with {@link CdtModifyOptions}.
     *
     * @param ael prepared AEL template
     * @param options modify options consumer
     * @param bindParams bind values for {@code ael}
     * @return the outer operation builder (unreachable until supported)
     * @throws UnsupportedOperationException always, until AEL path support ships
     */
    T modifyBy(PreparedAel ael, Consumer<CdtModifyOptions> options, Object... bindParams);

    /**
     * Terminal write: remove every path match, equivalent to {@code modifyByPath} with
     * {@link Exp#removeResult()}.
     *
     * <p><b>Example</b> — delete list elements matching a filter:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("nums").onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5)))
     *     .removeMatches()
     *     .execute();
     * }</pre>
     *
     * @return the outer operation builder after appending the CDT modify op
     */
    T removeMatches();

    /**
     * Same as {@link #removeMatches()} with {@link CdtModifyOptions}.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("nums").onEachChild(Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0)))
     *     .removeMatches(o -> o.noFail(true))
     *     .execute();
     * }</pre>
     *
     * @param options modify flag customization
     * @return the outer operation builder after appending the CDT modify op
     */
    T removeMatches(Consumer<CdtModifyOptions> options);

    /**
     * Terminal read: evaluate the same selection as {@link #collectValues()} inside an
     * <strong>expression read</strong> ({@code EXP_READ}) and store the result under this chain's bin name.
     *
     * <p>Equivalent to
     * {@code selectFrom(Exp.build(CdtExp.selectByPath(resultType, SelectFlags.VALUE, typedBin(bin), ctx…)))}
     * with {@code ctx…} taken from the fluent path. Use this when you need expression-read semantics
     * or to align with other {@code selectFrom} projections; use {@link #collectValues()} for a direct
     * CDT {@code selectByPath} operation.</p>
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key)
     *     .bin("catalog").onMapKey("book").onEachChild().onMapKey("title")
     *     .collectValuesAsExpressionRead(Exp.Type.MAP, Exp.Type.LIST)
     *     .execute();
     * }</pre>
     *
     * @param binValueType type of the <strong>source</strong> bin at the top level (e.g. {@link com.aerospike.client.sdk.exp.Exp.Type#MAP})
     * @param resultType expected result type of the select expression (often {@link com.aerospike.client.sdk.exp.Exp.Type#LIST})
     * @return the outer operation builder after appending the expression read op
     */
    T collectValuesAsExpressionRead(Exp.Type binValueType, Exp.Type resultType);

    /**
     * Same as {@link #collectValuesAsExpressionRead(Exp.Type, Exp.Type)} with explicit select and read flag bitmasks.
     *
     * <p><b>Example</b>:</p>
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
     * @param selectFlags {@link com.aerospike.client.sdk.cdt.SelectFlags} bitmask passed to {@link com.aerospike.client.sdk.exp.CdtExp#selectByPath}
     * @param readFlags {@link com.aerospike.client.sdk.exp.ExpReadFlags} bitmask for the expression read op
     * @return the outer operation builder after appending the expression read op
     */
    T collectValuesAsExpressionRead(Exp.Type binValueType, Exp.Type resultType, int selectFlags, int readFlags);

    /**
     * Same as {@link #collectValuesAsExpressionRead(Exp.Type, Exp.Type)} with {@link ExpressionReadOptions}
     * controlling read flags.
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
     * @return the outer operation builder after appending the expression read op
     */
    T collectValuesAsExpressionRead(Exp.Type binValueType, Exp.Type resultType, Consumer<ExpressionReadOptions> options);

    public T mapClear();
    public T mapSize();

    public T listSize();
    public T listGet(int index);
    public T listGetRange(int index);
    public T listGetRange(int index, int count);

    // listAppend -- unordered list
    public T listAppend(long value);
    public T listAppend(String value);
    public T listAppend(double value);
    public T listAppend(boolean value);
    public T listAppend(byte[] value);
    public T listAppend(List<?> value);
    public T listAppend(Map<?,?> value);
    public T listAppend(long value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(String value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(double value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(boolean value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(byte[] value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(List<?> value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(Map<?,?> value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(long value, ListEntryWriteOptions options);
    public T listAppend(String value, ListEntryWriteOptions options);
    public T listAppend(double value, ListEntryWriteOptions options);
    public T listAppend(boolean value, ListEntryWriteOptions options);
    public T listAppend(byte[] value, ListEntryWriteOptions options);
    public T listAppend(List<?> value, ListEntryWriteOptions options);
    public T listAppend(Map<?,?> value, ListEntryWriteOptions options);

    // listAdd -- ordered list
    public T listAdd(long value);
    public T listAdd(String value);
    public T listAdd(double value);
    public T listAdd(boolean value);
    public T listAdd(byte[] value);
    public T listAdd(List<?> value);
    public T listAdd(Map<?,?> value);
    public T listAdd(long value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(String value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(double value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(boolean value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(byte[] value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(List<?> value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(Map<?,?> value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(long value, ListEntryWriteOptions options);
    public T listAdd(String value, ListEntryWriteOptions options);
    public T listAdd(double value, ListEntryWriteOptions options);
    public T listAdd(boolean value, ListEntryWriteOptions options);
    public T listAdd(byte[] value, ListEntryWriteOptions options);
    public T listAdd(List<?> value, ListEntryWriteOptions options);
    public T listAdd(Map<?,?> value, ListEntryWriteOptions options);

    // listAppendItems / listAddItems -- bulk list operations
    public T listAppendItems(List<?> items);
    public T listAppendItems(List<?> items, Consumer<ListBulkWriteOptions> options);
    public T listAppendItems(List<?> items, ListBulkWriteOptions options);
    public T listAddItems(List<?> items);
    public T listAddItems(List<?> items, Consumer<ListBulkWriteOptions> options);
    public T listAddItems(List<?> items, ListBulkWriteOptions options);

    // list structural operations
    public T listClear();
    public T listSort();
    public T listSort(int sortFlags);
    public T listCreate(ListOrder order);
    public T listCreate(ListOrder order, Consumer<ListCreateOptions> options);
    public T listCreate(ListOrder order, ListCreateOptions options);
    public T listSetOrder(ListOrder order);
    public T listSetOrder(ListOrder order, boolean persistIndex);

    // list index-based write operations
    public T listInsert(int index, long value);
    public T listInsert(int index, String value);
    public T listInsert(int index, double value);
    public T listInsert(int index, boolean value);
    public T listInsert(int index, byte[] value);
    public T listInsert(int index, List<?> value);
    public T listInsert(int index, Map<?,?> value);
    public T listInsertItems(int index, List<?> items);
    public T listInsertItems(int index, List<?> items, Consumer<ListBulkWriteOptions> options);
    public T listInsertItems(int index, List<?> items, ListBulkWriteOptions options);
    public T listSet(int index, long value);
    public T listSet(int index, String value);
    public T listSet(int index, double value);
    public T listSet(int index, boolean value);
    public T listSet(int index, byte[] value);
    public T listSet(int index, List<?> value);
    public T listSet(int index, Map<?,?> value);
    public T listIncrement(int index);
    public T listIncrement(int index, long value);
    public T listIncrement(int index, double value);

    // list index-based remove operations
    public T listRemove(int index);
    public T listRemoveRange(int index);
    public T listRemoveRange(int index, int count);
    public T listPop(int index);
    public T listPopRange(int index);
    public T listPopRange(int index, int count);
    public T listTrim(int index, int count);

    // map structural / policy operations
    public T mapCreate(MapOrder order);
    public T mapCreate(MapOrder order, boolean persistIndex);
    public T mapSetPolicy(MapOrder order);
    public T mapSetPolicy(MapOrder order, boolean persistIndex);

    // bulk map write operations
    public T mapUpsertItems(Map<?, ?> items);
    public T mapUpsertItems(Map<?, ?> items, Consumer<MapBulkWriteOptions> options);
    public T mapUpsertItems(Map<?, ?> items, MapBulkWriteOptions options);
    public T mapInsertItems(Map<?, ?> items);
    public T mapInsertItems(Map<?, ?> items, Consumer<MapBulkWriteOptions> options);
    public T mapInsertItems(Map<?, ?> items, MapBulkWriteOptions options);
    public T mapUpdateItems(Map<?, ?> items);
    public T mapUpdateItems(Map<?, ?> items, Consumer<MapBulkWriteOptions> options);
    public T mapUpdateItems(Map<?, ?> items, MapBulkWriteOptions options);
}
