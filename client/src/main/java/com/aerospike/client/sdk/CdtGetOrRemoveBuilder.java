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
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.VALUE, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE, params.context()));
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.VALUE, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.VALUE, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.VALUE, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.VALUE, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        default:
            throw new IllegalArgumentException("getValues() does not know how to handle an operation of " + params.getOperation());
        }
    }

    // TODO: This should be limited so it can only get invoked on maps
    /**
     * Read map keys for the current selection. List selections throw at runtime; use only after map paths.
     *
     * @return the parent operation builder for chaining
     */
    public T getKeys() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.KEY, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
        default:
            throw new IllegalArgumentException("getKeys() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Return the count of elements matching the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T count() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.COUNT, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT, params.context()));
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        default:
            throw new IllegalArgumentException("count() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Return the count of elements not matching the current selection (inverted). Not supported after single-element paths such as {@code onMapIndex}, {@code onMapKey}, or {@code onListIndex}.
     *
     * @return the parent operation builder for chaining
     */
    public T countAllOthers() {
        switch (params.getOperation()) {
        // These three operation cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("countAllOthers cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: Th server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("countAllOthers() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Remove elements matching the current map or list selection.
     *
     * @return the parent operation builder for chaining
     */
    public T remove() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.removeByIndex(binName, params.getInt1(), MapReturnType.NONE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), MapReturnType.NONE, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.removeByKey(binName, params.getVal1(), MapReturnType.NONE, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.removeByKeyList(binName, params.getValues(), MapReturnType.NONE, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.removeByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.NONE, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.removeByRank(binName, params.getInt1(), MapReturnType.NONE, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.removeByValue(binName, params.getVal1(), MapReturnType.NONE, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.removeByValueList(binName, params.getValues(), MapReturnType.NONE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.NONE, params.context()));
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.removeByIndex(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByIndexRange(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.removeByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByRankRange(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.removeByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.removeByValueList(binName, params.getValues(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.NONE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.NONE, params.context()));
            }
        default:
            throw new IllegalArgumentException("remove() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Remove elements not matching the current selection (inverted). Not supported after single-element paths such as {@code onMapIndex}, {@code onMapKey}, or {@code onListIndex}.
     *
     * @return the parent operation builder for chaining
     */
    public T removeAllOthers() {
        switch (params.getOperation()) {
        // These three operation cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("countAllOthers cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: Th server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.removeByKeyList(binName, params.getValues(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.removeByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.removeByValue(binName, params.getVal1(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.removeByValueList(binName, params.getValues(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByIndexRange(binName, params.getInt1(), ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByRankRange(binName, params.getInt1(), ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.removeByValue(binName, params.getVal1(), ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.removeByValueList(binName, params.getValues(), ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("removeAllOthers() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read indexes for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getIndexes() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case LIST_BY_INDEX:
            throw new IllegalArgumentException("getIndexes() is not applicable for LIST_BY_INDEX (the index is already known)");
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.INDEX, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.INDEX, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.INDEX, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.INDEX, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.INDEX, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.INDEX, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.INDEX, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.INDEX, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.INDEX, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.INDEX, params.context()));
            }
        default:
            throw new IllegalArgumentException("getIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read reverse indexes for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getReverseIndexes() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case LIST_BY_INDEX:
            throw new IllegalArgumentException("getReverseIndexes() is not applicable for LIST_BY_INDEX (the index is already known)");
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_INDEX, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.REVERSE_INDEX, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.REVERSE_INDEX, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_INDEX, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.REVERSE_INDEX, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.REVERSE_INDEX, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.REVERSE_INDEX, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.REVERSE_INDEX, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.REVERSE_INDEX, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.REVERSE_INDEX, params.context()));
            }
        default:
            throw new IllegalArgumentException("getReverseIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read ranks for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getRanks() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.RANK, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.RANK, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.RANK, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.RANK, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.RANK, params.context()));
            }
        case LIST_BY_RANK:
            throw new IllegalArgumentException("getRanks() is not applicable for LIST_BY_RANK (the rank is already known)");
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.RANK, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.RANK, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.RANK, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.RANK, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.RANK, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.RANK, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.RANK, params.context()));
            }
        default:
            throw new IllegalArgumentException("getRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read reverse ranks for the current selection.
     *
     * @return the parent operation builder for chaining
     */
    public T getReverseRanks() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.REVERSE_RANK, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_RANK, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.REVERSE_RANK, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.REVERSE_RANK, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_RANK, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.REVERSE_RANK, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.REVERSE_RANK, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.REVERSE_RANK, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.REVERSE_RANK, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.REVERSE_RANK, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.REVERSE_RANK, params.context()));
            }
        default:
            throw new IllegalArgumentException("getReverseRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read key-value pairs for the current map selection (maps only).
     *
     * @return the parent operation builder for chaining
     */
    public T getKeysAndValues() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
        default:
            throw new IllegalArgumentException("getKeysAndValues() does not know how to handle an operation of " + params.getOperation());
        }
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
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("getAllOtherValues cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherValues() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read keys for all map entries not matching the current selection (inverted; maps only).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherKeys() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
        default:
            throw new IllegalArgumentException("getAllOtherKeys cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");
        }
    }

    /**
     * Read indexes for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherIndexes() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("getAllOtherIndexes cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read reverse indexes for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherReverseIndexes() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("getAllOtherReverseIndexes cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherReverseIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read ranks for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherRanks() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("getAllOtherRanks cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read reverse ranks for all elements not matching the current selection (inverted).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherReverseRanks() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException("getAllOtherReverseRanks cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
            }
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherReverseRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * Read key-value pairs for all map entries not matching the current selection (inverted; maps only).
     *
     * @return the parent operation builder for chaining
     */
    public T getAllOtherKeysAndValues() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
            throw new IllegalArgumentException("getAllOtherKeysAndValues cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherKeysAndValues() does not know how to handle an operation of " + params.getOperation());
        }
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
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.EXISTS, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.EXISTS, params.context()));
        case LIST_BY_INDEX:
            return opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.EXISTS, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
            }
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.EXISTS, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
            }
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE_LIST:
            return opBuilder.addOp(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE_RANGE:
            return opBuilder.addOp(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.EXISTS, params.context()));
            } else {
                return opBuilder.addOp(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.EXISTS, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.EXISTS, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.EXISTS, params.context()));
            }
        default:
            throw new IllegalArgumentException("exists() does not know how to handle an operation of " + params.getOperation());
        }
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
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
        default:
            throw new IllegalArgumentException("getAsMap() is only valid for map operations, not " + params.getOperation());
        }
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
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
            }
        case MAP_BY_KEY:
            return opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_KEY_LIST:
            return opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_KEY_RANGE:
            return opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_RANK:
            return opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_RANK_RANGE:
            return opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_VALUE:
            return opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_VALUE_LIST:
            return opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_VALUE_RANGE:
            return opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
            } else {
                return opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_INDEX_RANGE:
        case LIST_BY_RANK:
        case LIST_BY_RANK_RANGE:
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
        case LIST_BY_VALUE_RANGE:
        case LIST_BY_VALUE_REL_RANK_RANGE:
        default:
            throw new IllegalArgumentException("getAsOrderedMap() is only valid for map operations, not " + params.getOperation());
        }
    }
}
