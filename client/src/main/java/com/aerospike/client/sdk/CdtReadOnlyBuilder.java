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

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;

/**
 * Read-only CDT builder for query operations.
 * 
 * <p>This builder provides all CDT read operations without any write operations like
 * {@code remove()}, {@code setTo()}, {@code insert()}, {@code update()}, or {@code add()}.
 * It is designed for use in query contexts where only read operations are permitted.</p>
 *
 * <p>The builder uses the same {@link CdtOperationParams} infrastructure as
 * {@link CdtGetOrRemoveBuilder} but only exposes read methods, providing compile-time
 * safety that queries cannot accidentally perform write operations.</p>
 *
 * @param <T> the type of the parent builder to return for method chaining
 */
public class CdtReadOnlyBuilder<T> implements CdtReadContextBuilder<T>, 
                                               CdtReadContextInvertableBuilder<T> {
    
    private final String binName;
    private final CdtOperationAcceptor<T> acceptor;
    private final CdtOperationParams params;

    /**
     * Create a new read-only CDT builder.
     *
     * @param binName the name of the bin containing the CDT
     * @param acceptor the acceptor to add operations to
     * @param params the CDT operation parameters (selector, context, etc.)
     */
    public CdtReadOnlyBuilder(String binName, CdtOperationAcceptor<T> acceptor, CdtOperationParams params) {
        this.binName = binName;
        this.acceptor = acceptor;
        this.params = params;
    }

    // Helper to add operation and return parent builder
    private T addOpAndReturn(Operation op) {
        acceptor.acceptOp(op);
        return acceptor.getParentBuilder();
    }

    // ========================================
    // Terminal Read Operations (getValues, getKeys, count, etc.)
    // ========================================

    /** {@inheritDoc} */
    @Override
    public T getValues() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.VALUE, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE, params.context()));
        case LIST_BY_INDEX:
            return addOpAndReturn(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.VALUE, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
            }
        case LIST_BY_RANK:
            return addOpAndReturn(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.VALUE, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
            }
        case LIST_BY_VALUE:
            return addOpAndReturn(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE_LIST:
            return addOpAndReturn(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE_RANGE:
            return addOpAndReturn(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.VALUE, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.VALUE, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        default:
            throw new IllegalArgumentException("getValues() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T getKeys() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.KEY, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY, params.context()));
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

    /** {@inheritDoc} */
    @Override
    public T count() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.COUNT, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT, params.context()));
        case LIST_BY_INDEX:
            return addOpAndReturn(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case LIST_BY_RANK:
            return addOpAndReturn(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case LIST_BY_VALUE:
            return addOpAndReturn(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_LIST:
            return addOpAndReturn(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_RANGE:
            return addOpAndReturn(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.COUNT, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.COUNT, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        default:
            throw new IllegalArgumentException("count() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T getIndexes() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX, params.context()));
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
            throw new IllegalArgumentException("getIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T getReverseIndexes() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
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
            throw new IllegalArgumentException("getReverseIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T getRanks() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.RANK, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.RANK, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK, params.context()));
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
            throw new IllegalArgumentException("getRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T getReverseRanks() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
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
            throw new IllegalArgumentException("getReverseRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T getKeysAndValues() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
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

    /** {@inheritDoc} */
    @Override
    public T exists() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.EXISTS, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.EXISTS, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.EXISTS, params.context()));
        case LIST_BY_INDEX:
            return addOpAndReturn(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), ListReturnType.EXISTS, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
            }
        case LIST_BY_RANK:
            return addOpAndReturn(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), ListReturnType.EXISTS, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
            }
        case LIST_BY_VALUE:
            return addOpAndReturn(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE_LIST:
            return addOpAndReturn(ListOperation.getByValueList(binName, params.getValues(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE_RANGE:
            return addOpAndReturn(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), ListReturnType.EXISTS, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), ListReturnType.EXISTS, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.EXISTS, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.EXISTS, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.EXISTS, params.context()));
            }
        default:
            throw new IllegalArgumentException("exists() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering. */
    // TODO: Replace with AerospikeMap
    @Override
    @Deprecated
    public T getAsMap() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.UNORDERED_MAP, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.UNORDERED_MAP, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.UNORDERED_MAP, params.context()));
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

    /** @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering. */
    // TODO: Replace with AerospikeMap
    @Override
    @Deprecated
    public T getAsOrderedMap() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return addOpAndReturn(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
            }
        case MAP_BY_KEY:
            return addOpAndReturn(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_RANK:
            return addOpAndReturn(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.ORDERED_MAP, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.ORDERED_MAP, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.ORDERED_MAP, params.context()));
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

    // ========================================
    // Invertable Read Operations (getAllOther*)
    // ========================================

    /** {@inheritDoc} */
    @Override
    public T getAllOtherValues() {
        return getWithInverted(MapReturnType.VALUE, ListReturnType.VALUE, "getAllOtherValues");
    }

    /** {@inheritDoc} */
    @Override
    public T getAllOtherKeys() {
        return getWithInverted(MapReturnType.KEY, -1, "getAllOtherKeys");
    }

    /** {@inheritDoc} */
    @Override
    public T countAllOthers() {
        return getWithInverted(MapReturnType.COUNT, ListReturnType.COUNT, "countAllOthers");
    }

    /** {@inheritDoc} */
    @Override
    public T getAllOtherIndexes() {
        return getWithInverted(MapReturnType.INDEX, -1, "getAllOtherIndexes");
    }

    /** {@inheritDoc} */
    @Override
    public T getAllOtherReverseIndexes() {
        return getWithInverted(MapReturnType.REVERSE_INDEX, -1, "getAllOtherReverseIndexes");
    }

    /** {@inheritDoc} */
    @Override
    public T getAllOtherRanks() {
        return getWithInverted(MapReturnType.RANK, -1, "getAllOtherRanks");
    }

    /** {@inheritDoc} */
    @Override
    public T getAllOtherReverseRanks() {
        return getWithInverted(MapReturnType.REVERSE_RANK, -1, "getAllOtherReverseRanks");
    }

    /** {@inheritDoc} */
    @Override
    public T getAllOtherKeysAndValues() {
        return getWithInverted(MapReturnType.KEY_VALUE, -1, "getAllOtherKeysAndValues");
    }

    private T getWithInverted(int mapReturnType, int listReturnType, String methodName) {
        int invertedMapType = mapReturnType | MapReturnType.INVERTED;
        int invertedListType = listReturnType >= 0 ? listReturnType | ListReturnType.INVERTED : -1;
        
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
            throw new IllegalArgumentException(methodName + " cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), invertedMapType, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByIndexRange(binName, params.getInt1(), invertedMapType, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return addOpAndReturn(MapOperation.getByKeyList(binName, params.getValues(), invertedMapType, params.context()));
        case MAP_BY_KEY_RANGE:
            return addOpAndReturn(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), invertedMapType, params.context()));
        case MAP_BY_RANK_RANGE:
            return addOpAndReturn(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), invertedMapType, params.context()));
        case MAP_BY_VALUE:
            return addOpAndReturn(MapOperation.getByValue(binName, params.getVal1(), invertedMapType, params.context()));
        case MAP_BY_VALUE_LIST:
            return addOpAndReturn(MapOperation.getByValueList(binName, params.getValues(), invertedMapType, params.context()));
        case MAP_BY_VALUE_RANGE:
            return addOpAndReturn(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), invertedMapType, params.context()));
        case LIST_BY_VALUE:
        case LIST_BY_VALUE_LIST:
            if (invertedListType < 0) {
                throw new IllegalArgumentException(methodName + " is not supported for list operations");
            }
            if (params.getOperation() == CdtOperation.LIST_BY_VALUE) {
                return addOpAndReturn(ListOperation.getByValue(binName, params.getVal1(), invertedListType, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByValueList(binName, params.getValues(), invertedListType, params.context()));
            }
        case LIST_BY_INDEX_RANGE:
            if (invertedListType < 0) {
                throw new IllegalArgumentException(methodName + " is not supported for list operations");
            }
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), invertedListType, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByIndexRange(binName, params.getInt1(), invertedListType, params.context()));
            }
        case LIST_BY_RANK_RANGE:
            if (invertedListType < 0) {
                throw new IllegalArgumentException(methodName + " is not supported for list operations");
            }
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), invertedListType, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByRankRange(binName, params.getInt1(), invertedListType, params.context()));
            }
        case LIST_BY_VALUE_RANGE:
            if (invertedListType < 0) {
                throw new IllegalArgumentException(methodName + " is not supported for list operations");
            }
            return addOpAndReturn(ListOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), invertedListType, params.context()));
        case LIST_BY_VALUE_REL_RANK_RANGE:
            if (invertedListType < 0) {
                throw new IllegalArgumentException(methodName + " is not supported for list operations");
            }
            if (params.hasInt2()) {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), invertedListType, params.context()));
            } else {
                return addOpAndReturn(ListOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), invertedListType, params.context()));
            }
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), invertedMapType, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), invertedMapType, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), invertedMapType, params.context()));
            } else {
                return addOpAndReturn(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), invertedMapType, params.context()));
            }
        default:
            throw new IllegalArgumentException(methodName + " does not know how to handle an operation of " + params.getOperation());
        }
    }

    /** {@inheritDoc} */
    @Override
    public T mapSize() {
        params.pushCurrentToContext();
        return addOpAndReturn(MapOperation.size(binName, params.context()));
    }

    /** {@inheritDoc} */
    @Override
    public T listSize() {
        params.pushCurrentToContext();
        return addOpAndReturn(ListOperation.size(binName, params.context()));
    }

    /** {@inheritDoc} */
    @Override
    public T listGet(int index) {
        params.pushCurrentToContext();
        return addOpAndReturn(ListOperation.get(binName, index, params.context()));
    }

    /** {@inheritDoc} */
    @Override
    public T listGetRange(int index) {
        params.pushCurrentToContext();
        return addOpAndReturn(ListOperation.getRange(binName, index, params.context()));
    }

    /** {@inheritDoc} */
    @Override
    public T listGetRange(int index, int count) {
        params.pushCurrentToContext();
        return addOpAndReturn(ListOperation.getRange(binName, index, count, params.context()));
    }

    // ========================================
    // Context Navigation Methods
    // ========================================

    /** @see CdtGetOrRemoveBuilder#onMapIndex(int index) */
    @Override
    public CdtReadContextBuilder<T> onMapIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX, index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapIndexRange(int index, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapIndexRange(int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX_RANGE, index, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapIndexRange(int index) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapIndexRange(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX_RANGE, index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKey(long key) */
    @Override
    public CdtReadContextBuilder<T> onMapKey(long key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKey(String key) */
    @Override
    public CdtReadContextBuilder<T> onMapKey(String key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKey(byte[] key) */
    @Override
    public CdtReadContextBuilder<T> onMapKey(byte[] key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(long startIncl, long endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(String startIncl, String endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(byte[] startIncl, byte[] endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(double startIncl, double endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(SpecialValue startIncl, long endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(SpecialValue startIncl, String endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(SpecialValue startIncl, byte[] endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(SpecialValue startIncl, double endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(long startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(String startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(byte[] startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRange(double startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRelativeIndexRange(long key, int index) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRelativeIndexRange(String key, int index) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRelativeIndexRange(byte[] key, int index) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRelativeIndexRange(long key, int index, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRelativeIndexRange(String key, int index, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapKeyRelativeIndexRange(byte[] key, int index, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapRank(int index) */
    @Override
    public CdtReadContextBuilder<T> onMapRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK, index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapRankRange(int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapRankRange(int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK_RANGE, rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapRankRange(int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapRankRange(int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK_RANGE, rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(long value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(String value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(byte[] value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(double value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(double value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(boolean value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(boolean value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(List<?> value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(List<?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(Map<?,?> value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(Map<?,?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValue(SpecialValue value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, value.toAerospikeValue());
        return this;
    }

    // Map value range methods - all overloads
    /** @see CdtGetOrRemoveBuilder#onMapValueRange(long startIncl, long endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(String startIncl, String endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(byte[] startIncl, byte[] endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(double startIncl, double endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(boolean startIncl, boolean endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(List<?> startIncl, List<?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, long endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, String endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, byte[] endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, double endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, boolean endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, List<?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(long startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(String startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(byte[] startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(double startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(boolean startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(List<?> startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    // Map value relative rank range - all overloads
    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(long value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(String value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(byte[] value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(double value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(boolean value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(List<?> value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(Map<?,?> value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(SpecialValue value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(long value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(String value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(byte[] value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(double value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(boolean value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(List<?> value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueRelativeRankRange(SpecialValue value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count);
        return this;
    }

    // Map key and value list
    /** @see CdtGetOrRemoveBuilder#onMapKeyList(List<?> keys) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapKeyList(List<?> keys) {
        List<Value> values = new ArrayList<>(keys.size());
        for (Object key : keys) {
            values.add(Value.get(key));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_LIST, values);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onMapValueList(List<?> valueList) */
    @Override
    public CdtReadContextInvertableBuilder<T> onMapValueList(List<?> valueList) {
        List<Value> values = new ArrayList<>(valueList.size());
        for (Object v : valueList) {
            values.add(Value.get(v));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_LIST, values);
        return this;
    }

    // List operations
    /** @see CdtGetOrRemoveBuilder#onListIndex(int index) */
    @Override
    public CdtReadContextBuilder<T> onListIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListIndex(int index, ListOrder order, boolean pad) */
    @Override
    public CdtReadContextBuilder<T> onListIndex(int index, ListOrder order, boolean pad) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index, order, pad);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListRank(int index) */
    @Override
    public CdtReadContextBuilder<T> onListRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK, index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(long value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(String value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(byte[] value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(SpecialValue value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, value.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(double value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(double value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(boolean value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(boolean value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(List<?> value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(List<?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValue(Map<?,?> value) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValue(Map<?,?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListIndexRange(int index, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListIndexRange(int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX_RANGE, index, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListIndexRange(int index) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListIndexRange(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX_RANGE, index);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListRankRange(int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListRankRange(int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK_RANGE, rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListRankRange(int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListRankRange(int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK_RANGE, rank);
        return this;
    }

    // List value range methods - all overloads
    /** @see CdtGetOrRemoveBuilder#onListValueRange(long startIncl, long endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(String startIncl, String endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(byte[] startIncl, byte[] endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(double startIncl, double endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, long endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, String endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, byte[] endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, double endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(long startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(String startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(byte[] startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(double startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(boolean startIncl, boolean endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(boolean startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(List<?> startIncl, List<?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(List<?> startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(Map<?,?> startIncl, Map<?,?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, boolean endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(boolean startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(boolean startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, List<?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(List<?> startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(List<?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(SpecialValue startIncl, Map<?,?> endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRange(Map<?,?> startIncl, SpecialValue endExcl) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueList(List<?> valueList) */
    @Override
    public CdtReadContextInvertableBuilder<T> onListValueList(List<?> valueList) {
        List<Value> values = new ArrayList<>(valueList.size());
        for (Object v : valueList) {
            values.add(Value.get(v));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_LIST, values);
        return this;
    }

    // List value relative rank range - all overloads
    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(long value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(String value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(byte[] value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(double value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(SpecialValue value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(long value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(String value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(byte[] value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(double value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(SpecialValue value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(boolean value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(List<?> value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(Map<?,?> value, int rank) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(boolean value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(List<?> value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    /** @see CdtGetOrRemoveBuilder#onListValueRelativeRankRange(Map<?,?> value, int rank, int count) */
    @Override
    public CdtReadActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
}
