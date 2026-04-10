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
import java.util.Map;
import java.util.function.Consumer;

import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapPolicy;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.MapWriteFlags;

/**
 * Builder for map and list (CDT) read, remove, existence checks, and nested path operations on a bin.
 * After {@link BinBuilder} selects a bin, this type holds the current {@link CdtOperationParams} and dispatches
 * to server map or list operations. Context methods push another step onto the nested path and return {@code this};
 * terminal operations attach to the parent {@link AbstractOperationBuilder}.
 *
 * @param <T> the concrete parent operation builder type for chaining
 */
public class CdtGetOrRemoveBuilder<T extends AbstractOperationBuilder<T>> extends AbstractCdtBuilder<T>
                                implements CdtActionInvertableBuilder<T>, CdtActionNonInvertableBuilder<T>,
                                            CdtContextInvertableBuilder<T>, CdtContextNonInvertableBuilder<T>,
                                            CdtSetterInvertableBuilder<T>, CdtSetterNonInvertableBuilder<T> {

    /**
     * Discriminator for how map or list elements are selected for subsequent CDT operations.
     */
    public static enum CdtOperation {
        MAP_BY_INDEX,
        MAP_BY_INDEX_RANGE,
        MAP_BY_KEY,
        MAP_BY_KEY_LIST,
        MAP_BY_KEY_RANGE,
        MAP_BY_KEY_REL_INDEX_RANGE,
        MAP_BY_RANK,
        MAP_BY_RANK_RANGE,
        MAP_BY_VALUE,
        MAP_BY_VALUE_LIST,
        MAP_BY_VALUE_RANGE,
        MAP_BY_VALUE_REL_RANK_RANGE,
        LIST_BY_INDEX,
        LIST_BY_INDEX_RANGE,
        LIST_BY_RANK,
        LIST_BY_RANK_RANGE,
        LIST_BY_VALUE,
        LIST_BY_VALUE_LIST,
        LIST_BY_VALUE_RANGE,
        LIST_BY_VALUE_REL_RANK_RANGE
    }

    /**
     * Creates a builder for CDT operations along a nested map or list path.
     *
     * @param binName  target bin name
     * @param opBuilder parent operation builder
     * @param params    current selection and CDT context path
     */
    public CdtGetOrRemoveBuilder(String binName, T opBuilder, CdtOperationParams params) {
        super(opBuilder, binName, params);
    }

    /**
     * Read element values for the current map or list selection (CDT return type VALUE).
     *
     * @return the parent operation builder for chaining
     */
    public T getValues() {
        return dispatchGet(MapReturnType.VALUE, ListReturnType.VALUE);
    }

    /**
     * Read map keys for the current selection. List selections throw at runtime; use only after map paths.
     *
     * @return the parent operation builder for chaining
     */
    public T getKeys() {
        validateMapOnly("getKeys");
        return dispatchGet(MapReturnType.KEY, 0);
    }

    /**
     * Return the count of elements matching the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T count() {
        return dispatchGet(MapReturnType.COUNT, ListReturnType.COUNT);
    }

    /**
     * Return the count of elements not matching the current selection (inverted). Not supported after single-element paths such as {@code onMapIndex}, {@code onMapKey}, or {@code onListIndex}.
     *
     * @return the parent operation builder for chaining
     */
    public T countAllOthers() {
        validateNotSingleElement("countAllOthers");
        return dispatchGet(MapReturnType.COUNT | MapReturnType.INVERTED, ListReturnType.COUNT | ListReturnType.INVERTED);
    }

    /**
     * Remove elements matching the current map or list selection.
     * For map operations the server returns nothing ({@code MapReturnType.NONE}).
     * For list operations the server returns the count of removed elements ({@code ListReturnType.COUNT}).
     *
     * @return the parent operation builder for chaining
     * @see #removeAnd() to choose what data the server returns about the removed elements
     */
    public T remove() {
        return dispatchRemove(MapReturnType.NONE, ListReturnType.COUNT);
    }

    /**
     * Begin a remove operation that will also return data about the removed elements.
     * Chain one of the return-type methods on the result to specify what the server should return.
     * <pre>{@code
     * .onMapValueRange(1, 4).removeAnd().getValues()   // remove and return VALUES
     * .onMapValueRange(1, 4).removeAnd().count()        // remove and return COUNT
     * .onMapValueRange(1, 4).removeAnd().getKeys()      // remove and return KEYS (maps only)
     * }</pre>
     *
     * @return a {@link RemoveResultBuilder} for specifying the return type
     * @see #remove() for fire-and-forget removal with no return data
     */
    public RemoveResultBuilder<T> removeAnd() {
        return new RemoveResultBuilder<>(this, 0);
    }

    /**
     * Remove elements not matching the current selection (inverted).
     * Not supported after single-element paths such as {@code onMapIndex},
     * {@code onMapKey}, or {@code onListIndex}.
     *
     * @return the parent operation builder for chaining
     * @see #removeAllOthersAnd() to choose what data the server returns about the removed elements
     */
    public T removeAllOthers() {
        validateNotSingleElement("removeAllOthers");
        return dispatchRemove(MapReturnType.INVERTED, ListReturnType.INVERTED);
    }

    /**
     * Begin an inverted remove operation that will also return data about the removed elements.
     * Removes elements NOT matching the current selection and returns data about those removed elements.
     * Not supported after single-element paths such as {@code onMapIndex},
     * {@code onMapKey}, or {@code onListIndex}.
     * <pre>{@code
     * .onMapValueRange(1, 4).removeAllOthersAnd().getValues()  // remove others, return their VALUES
     * .onMapValueRange(1, 4).removeAllOthersAnd().count()       // remove others, return COUNT
     * }</pre>
     *
     * @return a {@link RemoveResultBuilder} for specifying the return type
     * @see #removeAllOthers() for fire-and-forget inverted removal with no return data
     */
    public RemoveResultBuilder<T> removeAllOthersAnd() {
        validateNotSingleElement("removeAllOthersAnd");
        return new RemoveResultBuilder<>(this, MapReturnType.INVERTED);
    }

    /**
     * Dispatch a remove operation with the given return types for map and list operations.
     * Used internally by {@link #remove()}, {@link #removeAllOthers()}, and {@link RemoveResultBuilder}.
     */
    T dispatchRemove(int mapReturnType, int listReturnType) {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.removeByIndex(binName, params.getInt1(), mapReturnType, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), mapReturnType, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), mapReturnType, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.removeByKey(binName, params.getVal1(), mapReturnType, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.removeByKeyList(binName, params.getValues(), mapReturnType, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.removeByKeyRange(binName, params.getVal1(), params.getVal2(), mapReturnType, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.removeByRank(binName, params.getInt1(), mapReturnType, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), mapReturnType, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.removeByValue(binName, params.getVal1(), mapReturnType, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.removeByValueList(binName, params.getValues(), mapReturnType, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), mapReturnType, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), mapReturnType, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), mapReturnType, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), mapReturnType, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), mapReturnType, params.context()));
            }
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.removeByIndex(binName, params.getInt1(), listReturnType, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), listReturnType, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByIndexRange(binName, params.getInt1(), listReturnType, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.removeByRank(binName, params.getInt1(), listReturnType, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), listReturnType, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByRankRange(binName, params.getInt1(), listReturnType, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.removeByValue(binName, params.getVal1(), listReturnType, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.removeByValueList(binName, params.getValues(), listReturnType, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), listReturnType, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), listReturnType, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), listReturnType, params.context()));
            }
        default:
            throw new IllegalArgumentException("remove operation does not know how to handle " + params.getOperation());
        }
    }

    /**
     * Convenience overload: same return type for both map and list operations.
     */
    T dispatchRemove(int returnType) {
        return dispatchRemove(returnType, returnType);
    }

    /**
     * Dispatch a get operation with the given return types for map and list operations.
     * Used internally by terminal get methods (getValues, getKeys, count, etc.) and their inverted variants.
     */
    private T dispatchGet(int mapReturnType, int listReturnType) {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), mapReturnType, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), mapReturnType, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), mapReturnType, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), mapReturnType, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), mapReturnType, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), mapReturnType, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), mapReturnType, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), mapReturnType, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), mapReturnType, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), mapReturnType, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), mapReturnType, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), mapReturnType, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), mapReturnType, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), mapReturnType, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), mapReturnType, params.context()));
            }
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), listReturnType, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), listReturnType, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), listReturnType, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), listReturnType, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), listReturnType, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), listReturnType, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), listReturnType, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), listReturnType, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), listReturnType, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), listReturnType, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), listReturnType, params.context()));
            }
        default:
            throw new IllegalArgumentException("get operation does not know how to handle " + params.getOperation());
        }
    }

    /**
     * Validate that the current operation is not a single-element selector (which cannot be inverted).
     */
    private void validateNotSingleElement(String methodName) {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException(methodName + "() cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");
        default:
            break;
        }
    }

    /**
     * Validate that the current operation is a map operation (not a list operation).
     */
    void validateMapOnly(String methodName) {
        switch (params.getOperation()) {
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
            throw new IllegalArgumentException(methodName + "() is only valid for map operations, not " + params.getOperation());
        default:
            break;
        }
    }

    /**
     * Read indexes for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getIndexes() {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("getIndexes() is not applicable for LIST_BY_INDEX (the index is already known)");
        }
        return dispatchGet(MapReturnType.INDEX, ListReturnType.INDEX);
    }

    /**
     * Read reverse indexes for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getReverseIndexes() {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("getReverseIndexes() is not applicable for LIST_BY_INDEX (the index is already known)");
        }
        return dispatchGet(MapReturnType.REVERSE_INDEX, ListReturnType.REVERSE_INDEX);
    }

    /**
     * Read ranks for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getRanks() {
        if (params.getOperation() == CdtOperation.LIST_BY_RANK) {
            throw new IllegalArgumentException("getRanks() is not applicable for LIST_BY_RANK (the rank is already known)");
        }
        return dispatchGet(MapReturnType.RANK, ListReturnType.RANK);
    }

    /**
     * Read reverse ranks for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getReverseRanks() {
        return dispatchGet(MapReturnType.REVERSE_RANK, ListReturnType.REVERSE_RANK);
    }

    /**
     * Read key-value pairs for the current map selection (maps only).
     *
     * @return the parent operation builder for chaining
     */
    public T getKeysAndValues() {
        validateMapOnly("getKeysAndValues");
        return dispatchGet(MapReturnType.KEY_VALUE, 0);
    }

    /**
     * Select a map entry by sort index for nested CDT operations; pushes the selection onto the nested context path.
     *
     * @param index index in server map order
     * @return this builder for continued nested CDT operations
     */
    public CdtContextNonInvertableBuilder<T> onMapIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX, index);
        return this;
    }
    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param createType ordering used if the map must be created
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKey(long)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param createType ordering used if the map must be created
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKey(long, MapOrder)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKey(long)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param createType ordering used if the map must be created
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKey(long, MapOrder)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    /**
     * Select a map entry by rank; pushes the selection onto the nested CDT context path.
     *
     * @param index server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextNonInvertableBuilder<T> onMapRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK, index);
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextInvertableBuilder<T> onMapValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(double value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(boolean value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(List<?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(Map<?,?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select map entries by value identity; pushes the selection onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, value.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    // SpecialValue combinations for onMapKeyRange
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    // SpecialValue combinations for onMapValueRange
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to map values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
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
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param index server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRelativeIndexRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param index server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRelativeIndexRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
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
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param index server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRelativeIndexRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    /**
     * Select a map entry by key; pushes the selection onto the nested CDT context path.
     *
     * @param key reference map key
     * @param index server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapKeyRelativeIndexRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
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
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank);
        return this;
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
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Select map entries by value relative to a rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count);
        return this;
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
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX_RANGE, index, count);
        return this;
    }

    /**
     * Navigate to map items by index range to end.
     * Server selects map items starting at specified index to the end of map.
     *
     * @param index the starting index
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX_RANGE, index);
        return this;
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
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK_RANGE, rank, count);
        return this;
    }

    /**
     * Navigate to map items by rank range to end.
     * Server selects map items starting at specified rank to the end of map.
     *
     * @param rank the starting rank
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK_RANGE, rank);
        return this;
    }

    /**
     * Select a list element by index; pushes onto the nested CDT context path.
     *
     * @param index server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextNonInvertableBuilder<T> onListIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index);
        return this;
    }
    /**
     * Select a list element by index; pushes onto the nested CDT context path.
     *
     * @param index server CDT index, rank, or count per operation semantics
     * @param order list order if the list is created
     * @param pad whether to pad when creating the list
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextNonInvertableBuilder<T> onListIndex(int index, ListOrder order, boolean pad) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index, order, pad);
        return this;
    }
    /**
     * Select a list element by rank; pushes onto the nested CDT context path.
     *
     * @param index server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextNonInvertableBuilder<T> onListRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK, index);
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextInvertableBuilder<T> onListValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, value.toAerospikeValue());
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(double value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(boolean value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(List<?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    /**
     * Select list elements matching a value; pushes onto the nested CDT context path.
     *
     * @param value reference value
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(Map<?,?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /**
     * Select a list element by index; pushes onto the nested CDT context path.
     *
     * @param index server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onListIndexRange(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX_RANGE, index);
        return this;
    }
    /**
     * Select a list element by index; pushes onto the nested CDT context path.
     *
     * @param index server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onListIndexRange(int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX_RANGE, index, count);
        return this;
    }
    /**
     * Select a list element by rank; pushes onto the nested CDT context path.
     *
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onListRankRange(int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK_RANGE, rank);
        return this;
    }
    /**
     * Select a list element by rank; pushes onto the nested CDT context path.
     *
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onListRankRange(int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK_RANGE, rank, count);
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(boolean startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(List<?> startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(boolean startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(List<?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    /**
     * Restrict to list values in {@code [startIncl, endExcl)}; pushes onto the nested context path.
     *
     * @param startIncl range bound (inclusive start)
     * @param endExcl range bound (exclusive end)
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    /**
     * Select list elements matching any of the given values; pushes onto the nested context path.
     *
     * @param values candidate values
     *
     * @return this builder for continued nested CDT operations
     */
    public CdtContextInvertableBuilder<T> onListValueList(List<?> values) {
        List<Value> valueList = new ArrayList<>();
        for (Object value : values) {
            valueList.add(Value.get(value));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_LIST, valueList);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    /**
     * Select list elements by value relative to rank range; pushes onto the nested context path.
     *
     * @param value reference value
     * @param rank server CDT index, rank, or count per operation semantics
     * @param count server CDT index, rank, or count per operation semantics
     *
     * @return this builder for continued nested CDT operations
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /**
     * Navigate to map items by a list of keys.
     * Server selects map items identified by keys.
     *
     * @param keys the list of keys to match
     * @return builder for continued chaining (invertable for list operations)
     */
    public CdtContextInvertableBuilder<T> onMapKeyList(List<?> keys) {
        List<Value> valueList = new ArrayList<>();
        for (Object key : keys) {
            valueList.add(Value.get(key));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_LIST, valueList);
        return this;
    }

    /**
     * Navigate to map items by a list of values.
     * Server selects map items identified by values.
     *
     * @param values the list of values to match
     * @return builder for continued chaining (invertable for list operations)
     */
    public CdtContextInvertableBuilder<T> onMapValueList(List<?> values) {
        List<Value> valueList = new ArrayList<>();
        for (Object value : values) {
            valueList.add(Value.get(value));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_LIST, valueList);
        return this;
    }

    /**
     * Read values for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherValues() {
        validateNotSingleElement("getAllOtherValues");
        return dispatchGet(MapReturnType.VALUE | MapReturnType.INVERTED, ListReturnType.VALUE | ListReturnType.INVERTED);
    }

    /**
     * Read keys for all map entries not matching the current selection (inverted; maps only).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherKeys() {
        validateNotSingleElement("getAllOtherKeys");
        validateMapOnly("getAllOtherKeys");
        return dispatchGet(MapReturnType.KEY | MapReturnType.INVERTED, 0);
    }

    /**
     * Read indexes for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherIndexes() {
        validateNotSingleElement("getAllOtherIndexes");
        return dispatchGet(MapReturnType.INDEX | MapReturnType.INVERTED, ListReturnType.INDEX | ListReturnType.INVERTED);
    }

    /**
     * Read reverse indexes for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherReverseIndexes() {
        validateNotSingleElement("getAllOtherReverseIndexes");
        return dispatchGet(MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED);
    }

    /**
     * Read ranks for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherRanks() {
        validateNotSingleElement("getAllOtherRanks");
        return dispatchGet(MapReturnType.RANK | MapReturnType.INVERTED, ListReturnType.RANK | ListReturnType.INVERTED);
    }

    /**
     * Read reverse ranks for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherReverseRanks() {
        validateNotSingleElement("getAllOtherReverseRanks");
        return dispatchGet(MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, ListReturnType.REVERSE_RANK | ListReturnType.INVERTED);
    }

    /**
     * Read key-value pairs for all map entries not matching the current selection (inverted; maps only).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherKeysAndValues() {
        validateNotSingleElement("getAllOtherKeysAndValues");
        validateMapOnly("getAllOtherKeysAndValues");
        return dispatchGet(MapReturnType.KEY_VALUE | MapReturnType.INVERTED, 0);
    }


    // ===============================
    // Setter methods after a mapIndex
    // ===============================

    /**
     * Build a MapPolicy using the MapOrder from the current operation params (if specified via
     * e.g. {@code onMapKey(key, MapOrder.UNORDERED)}), defaulting to {@code MapOrder.KEY_ORDERED}.
     * The MapOrder determines the type of map to create if the map does not already exist.
     * {@link MapWriteFlags}
     */
    private MapPolicy resolveMapPolicy(int baseFlags, MapWriteOptions<?> opts) {
        MapOrder order = (opts != null && opts.getMapOrder() != null) ? opts.getMapOrder()
                       : (params.getMapCreateType() != null) ? params.getMapCreateType()
                       : MapOrder.KEY_ORDERED;
        int flags = baseFlags;
        boolean persist = false;
        if (opts != null) {
            if (opts.isAllowFailures()) {
				flags |= MapWriteFlags.NO_FAIL;
			}
            if (opts instanceof MapBulkWriteOptions && ((MapBulkWriteOptions) opts).isAllowPartial()) {
                flags |= MapWriteFlags.PARTIAL;
            }
            persist = opts.isPersistIndex();
        }
        return cachedMapPolicy(order, flags, persist);
    }

    private MapEntryWriteOptions applyOptions(Consumer<MapEntryWriteOptions> options) {
        if (options == null) {
			return null;
		}
        MapEntryWriteOptions opts = new MapEntryWriteOptions();
        options.accept(opts);
        return opts;
    }


    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(long value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(String value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(byte[] value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(boolean value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(double value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(List<?> value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T setTo(Map<?,?> value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    /**
     * Set the list element at the selected index, or put a map entry at the selected key.
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     *
     * @return the parent operation builder for chaining
     */
    public <U> T setTo(U value, RecordMapper<U> mapper) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(mapper.toMap(value)), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
        }
    }

    // =================================
    // insert methods (CREATE_ONLY)
    // =================================

    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(long value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(String value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(byte[] value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(boolean value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(double value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(List<?> value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(Map<?,?> value) {
        return insert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     *
     * @return the parent operation builder for chaining
     */
    public <U> T insert(U value, RecordMapper<U> mapper) {
        return insert(value, mapper, (Consumer<MapEntryWriteOptions>) null);
    }

    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(long value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(String value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(byte[] value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(boolean value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(double value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(List<?> value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T insert(Map<?,?> value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Insert at the selected list index, or map {@code put} with CREATE_ONLY at the selected key.
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public <U> T insert(U value, RecordMapper<U> mapper, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(mapper.toMap(value)), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    // =================================
    // update methods (UPDATE_ONLY)
    // =================================

    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(long value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(String value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(byte[] value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(boolean value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(double value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(List<?> value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(Map<?,?> value) {
        return update(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     *
     * @return the parent operation builder for chaining
     */
    public <U> T update(U value, RecordMapper<U> mapper) {
        return update(value, mapper, (Consumer<MapEntryWriteOptions>) null);
    }

    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(long value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(String value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(byte[] value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(boolean value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(double value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(List<?> value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T update(Map<?,?> value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with UPDATE_ONLY at the selected key (not applicable to list-by-index).
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public <U> T update(U value, RecordMapper<U> mapper, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    // =================================
    // add methods (increment, DEFAULT flags)
    // =================================

    /**
     * Increment the numeric value at the selected list index or map key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T add(long value) {
        return add(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Increment the numeric value at the selected list index or map key.
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T add(double value) {
        return add(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Increment the numeric value at the selected list index or map key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T add(long value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.increment(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Increment the numeric value at the selected list index or map key.
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T add(double value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.increment(binName, params.getInt1(), Value.get(value), params.context()));
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }

    // =================================
    // upsert methods (DEFAULT flags)
    // =================================

    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(long value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(String value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(byte[] value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(boolean value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(double value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(List<?> value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(Map<?,?> value) {
        return upsert(value, (Consumer<MapEntryWriteOptions>) null);
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     *
     * @return the parent operation builder for chaining
     */
    public <U> T upsert(U value, RecordMapper<U> mapper) {
        return upsert(value, mapper, (Consumer<MapEntryWriteOptions>) null);
    }

    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(long value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(String value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(byte[] value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(boolean value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(double value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(List<?> value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param value value to write
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public T upsert(Map<?,?> value, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    /**
     * Map {@code put} with default semantics at the selected key (not applicable to list-by-index).
     *
     * @param <U> mapped Java type
     * @param value value to write
     * @param mapper converts {@code U} to storable fields
     * @param options value to write
     *
     * @return the parent operation builder for chaining
     */
    public <U> T upsert(U value, RecordMapper<U> mapper, Consumer<MapEntryWriteOptions> options) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            throw new IllegalArgumentException("upsert/update is not applicable for list operations");
        }
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    // =================================
    // exists() - returns true if the selected element exists
    // =================================

    /**
     * Test whether elements exist for the current selection (CDT EXISTS return).
     *
     * @return the parent operation builder for chaining
     */
    public T exists() {
        return dispatchGet(MapReturnType.EXISTS, ListReturnType.EXISTS);
    }

    // =================================
    // getAsMap / getAsOrderedMap
    // =================================

    /**
     * @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering.
     * Read results as an unordered map (map selections only).
     *
     * @return the parent operation builder for chaining
     */
    // TODO: Replace with AerospikeMap
    @Deprecated
    public T getAsMap() {
        validateMapOnly("getAsMap");
        return dispatchGet(MapReturnType.UNORDERED_MAP, 0);
    }

    /**
     * @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering.
     * Read results as an ordered map (map selections only).
     *
     * @return the parent operation builder for chaining
     */
    // TODO: Replace with AerospikeMap
    @Deprecated
    public T getAsOrderedMap() {
        validateMapOnly("getAsOrderedMap");
        return dispatchGet(MapReturnType.ORDERED_MAP, 0);
    }
}
