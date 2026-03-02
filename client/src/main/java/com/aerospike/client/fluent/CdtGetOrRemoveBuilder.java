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
package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.cdt.ListOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.ListReturnType;
import com.aerospike.client.fluent.cdt.MapOperation;
import com.aerospike.client.fluent.cdt.MapOrder;
import com.aerospike.client.fluent.cdt.MapPolicy;
import com.aerospike.client.fluent.cdt.MapReturnType;
import com.aerospike.client.fluent.cdt.MapWriteFlags;

public class CdtGetOrRemoveBuilder<T extends AbstractOperationBuilder<T>> extends AbstractCdtBuilder<T> 
                                implements CdtActionInvertableBuilder<T>, CdtActionNonInvertableBuilder<T>, 
                                            CdtContextInvertableBuilder<T>, CdtContextNonInvertableBuilder<T>,
                                            CdtSetterInvertableBuilder<T>, CdtSetterNonInvertableBuilder<T> {
    
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
        LIST_BY_RANK,
        LIST_BY_VALUE
    }
    
    public CdtGetOrRemoveBuilder(String binName, T opBuilder, CdtOperationParams params) {
        super(opBuilder, binName, params);
    }

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
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.VALUE, params.context()));
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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getKeys() does not know how to handle an operation of " + params.getOperation());
        }
    }
    
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
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
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
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
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
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.removeByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.removeByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
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
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.removeByValue(binName, params.getVal1(), ListReturnType.INVERTED, params.context()));
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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getIndex() does not know how to handle an operation of " + params.getOperation());
        }
    }

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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getReverseIndex() does not know how to handle an operation of " + params.getOperation());
        }
    }

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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getRank() does not know how to handle an operation of " + params.getOperation());
        }
    }

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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getReverseRank() does not know how to handle an operation of " + params.getOperation());
        }
    }

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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getKeyAndValue() does not know how to handle an operation of " + params.getOperation());
        }
    }

    /**
     * These methods can called with anything that can be a context, like onMapIndex. This can be an operation (get or remove) in it's own
     * right, or a step in a context path.
     */
    public CdtContextNonInvertableBuilder<T> onMapIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX, index);
        return this;
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    public CdtContextNonInvertableBuilder<T> onMapRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK, index);
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(double value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(boolean value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(List<?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(Map<?,?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onMapValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, value.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    // SpecialValue combinations for onMapKeyRange
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    // SpecialValue combinations for onMapValueRange
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
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
    
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }
    
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
    
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }
    
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
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }
    
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
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }
    
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
    
    public CdtContextNonInvertableBuilder<T> onListIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index);
        return this;
    }
    public CdtContextNonInvertableBuilder<T> onListIndex(int index, ListOrder order, boolean pad) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index, order, pad);
        return this;
    }
    public CdtContextNonInvertableBuilder<T> onListRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK, index);
        return this;
    }
    public CdtContextInvertableBuilder<T> onListValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onListValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onListValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder<T> onListValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, value.toAerospikeValue());
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
        case LIST_BY_RANK:
        default:
            throw new IllegalArgumentException("getAllOtherKeys cannot be called after onMapIndex, onMapKey, onMapRank, onListIndex or onListRank: The server does not support this");
        }
    }

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

    public T getAllOtherKeysAndValues() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur
        case MAP_BY_INDEX:
        case MAP_BY_KEY:
        case MAP_BY_RANK:
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
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
            if (opts.isAllowFailures()) flags |= MapWriteFlags.NO_FAIL;
            if (opts instanceof MapBulkWriteOptions && ((MapBulkWriteOptions) opts).isAllowPartial()) {
                flags |= MapWriteFlags.PARTIAL;
            }
            persist = opts.isPersistIndex();
        }
        return cachedMapPolicy(order, flags, persist);
    }

    private MapEntryWriteOptions applyOptions(java.util.function.Consumer<MapEntryWriteOptions> options) {
        if (options == null) return null;
        MapEntryWriteOptions opts = new MapEntryWriteOptions();
        options.accept(opts);
        return opts;
    }

    
    public T setTo(long value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    public T setTo(String value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    public T setTo(byte[] value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    public T setTo(boolean value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    public T setTo(double value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    public T setTo(List<?> value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
    public T setTo(Map<?,?> value) {
        if (params.getOperation() == CdtOperation.LIST_BY_INDEX) {
            return this.opBuilder.addOp(ListOperation.set(binName, params.getInt1(), Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.put(resolveMapPolicy(MapWriteFlags.DEFAULT, null), binName, params.getVal1(), Value.get(value), params.context()));
        }
    }
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

    public T insert(long value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T insert(String value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T insert(byte[] value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T insert(boolean value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T insert(double value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T insert(List<?> value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T insert(Map<?,?> value) {
        return insert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public <U> T insert(U value, RecordMapper<U> mapper) {
        return insert(value, mapper, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }

    public T insert(long value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T insert(String value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T insert(byte[] value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T insert(boolean value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T insert(double value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T insert(List<?> value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T insert(Map<?,?> value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <U> T insert(U value, RecordMapper<U> mapper, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.CREATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    // =================================
    // update methods (UPDATE_ONLY)
    // =================================

    public T update(long value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T update(String value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T update(byte[] value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T update(boolean value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T update(double value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T update(List<?> value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T update(Map<?,?> value) {
        return update(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public <U> T update(U value, RecordMapper<U> mapper) {
        return update(value, mapper, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }

    public T update(long value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T update(String value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T update(byte[] value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T update(boolean value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T update(double value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T update(List<?> value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T update(Map<?,?> value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <U> T update(U value, RecordMapper<U> mapper, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.UPDATE_ONLY, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    // =================================
    // add methods (increment, DEFAULT flags)
    // =================================

    public T add(long value) {
        return add(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T add(double value) {
        return add(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T add(long value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T add(double value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }

    // =================================
    // upsert methods (DEFAULT flags)
    // =================================

    public T upsert(long value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T upsert(String value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T upsert(byte[] value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T upsert(boolean value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T upsert(double value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T upsert(List<?> value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public T upsert(Map<?,?> value) {
        return upsert(value, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }
    public <U> T upsert(U value, RecordMapper<U> mapper) {
        return upsert(value, mapper, (java.util.function.Consumer<MapEntryWriteOptions>) null);
    }

    public T upsert(long value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T upsert(String value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T upsert(byte[] value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T upsert(boolean value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T upsert(double value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T upsert(List<?> value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public T upsert(Map<?,?> value, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <U> T upsert(U value, RecordMapper<U> mapper, java.util.function.Consumer<MapEntryWriteOptions> options) {
        MapPolicy mp = resolveMapPolicy(MapWriteFlags.DEFAULT, applyOptions(options));
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    // =================================
    // exists() - returns true if the selected element exists
    // =================================

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
        case LIST_BY_RANK:
            return opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.EXISTS, params.context()));
        case LIST_BY_VALUE:
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.EXISTS, params.context()));
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

    /** @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering. */
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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getAsMap() is only valid for map operations, not " + params.getOperation());
        }
    }

    /** @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering. */
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
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getAsOrderedMap() is only valid for map operations, not " + params.getOperation());
        }
    }
}
