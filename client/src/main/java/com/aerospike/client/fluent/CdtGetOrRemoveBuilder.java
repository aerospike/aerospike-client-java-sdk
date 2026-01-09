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

@SuppressWarnings("rawtypes")
public class CdtGetOrRemoveBuilder extends AbstractCdtBuilder
                                implements CdtActionInvertableBuilder, CdtActionNonInvertableBuilder,
                                            CdtContextInvertableBuilder, CdtContextNonInvertableBuilder,
                                            CdtSetterInvertableBuilder, CdtSetterNonInvertableBuilder {

    protected static enum CdtOperation {
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

    @SuppressWarnings("unchecked")
	public CdtGetOrRemoveBuilder(String binName, AbstractOperationBuilder opBuilder, CdtOperationParams params) {
        super(opBuilder, binName, params);
    }

    public OperationBuilder getValues() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.VALUE, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.VALUE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE, params.context()));
        case LIST_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.VALUE, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.VALUE, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE, params.context()));
            }
        default:
            throw new IllegalArgumentException("getValues() does not know how to handle an operation of " + params.getOperation());
        }
    }

    // TODO: This should be limited so it can only get invoked on maps
    public OperationBuilder getKeys() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.KEY, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getKeys() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder count() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.COUNT, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.COUNT, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT, params.context()));
        case LIST_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByIndex(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT, params.context()));
            }
        default:
            throw new IllegalArgumentException("count() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder countAllOthers() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.COUNT | ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.COUNT | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("countAllOthers() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder remove() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByIndex(binName, params.getInt1(), MapReturnType.NONE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), MapReturnType.NONE, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKey(binName, params.getVal1(), MapReturnType.NONE, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyList(binName, params.getValues(), MapReturnType.NONE, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.NONE, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByRank(binName, params.getInt1(), MapReturnType.NONE, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValue(binName, params.getVal1(), MapReturnType.NONE, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueList(binName, params.getValues(), MapReturnType.NONE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.NONE, params.context()));
        case LIST_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(ListOperation.removeByIndex(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(ListOperation.removeByRank(binName, params.getInt1(), ListReturnType.COUNT, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.removeByValue(binName, params.getVal1(), ListReturnType.COUNT, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.NONE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.NONE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.NONE, params.context()));
            }
        default:
            throw new IllegalArgumentException("remove() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder removeAllOthers() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByIndexRange(binName, params.getInt1(), MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyList(binName, params.getValues(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValue(binName, params.getVal1(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueList(binName, params.getValues(), MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.removeByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("removeAllOthers() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getIndex() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.INDEX, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getIndex() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getReverseIndex() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getReverseIndex() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getRank() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.RANK, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.RANK, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.RANK, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getRank() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getReverseRank() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK, params.context()));
            }
        case LIST_BY_INDEX:
        case LIST_BY_RANK:
        case LIST_BY_VALUE:
        default:
            throw new IllegalArgumentException("getReverseRank() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getKeyAndValue() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case MAP_BY_KEY:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKey(binName, params.getVal1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_RANK:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRank(binName, params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE, params.context()));
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
    public CdtContextNonInvertableBuilder onMapIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_INDEX, index);
        return this;
    }
    public CdtSetterNonInvertableBuilder onMapKey(long key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    public CdtSetterNonInvertableBuilder onMapKey(long key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    public CdtSetterNonInvertableBuilder onMapKey(String key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    public CdtSetterNonInvertableBuilder onMapKey(String key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    public CdtSetterNonInvertableBuilder onMapKey(byte[] key) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key));
        return this;
    }
    public CdtSetterNonInvertableBuilder onMapKey(byte[] key, MapOrder createType) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get(key), createType);
        return this;
    }
    public CdtContextNonInvertableBuilder onMapRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK, index);
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(double value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(boolean value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(List<?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(Map<?,?> value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onMapValue(SpecialValue value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE, value.toAerospikeValue());
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }

    public CdtContextInvertableBuilder onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtContextInvertableBuilder onMapKeyRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(String startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(byte[] startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(double startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(boolean startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(List<?> startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    // SpecialValue combinations for onMapValueRange
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, String endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, byte[] endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, double endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, boolean endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, List<?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl));
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(long startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(String startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(byte[] startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(double startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(boolean startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(List<?> startIncl, SpecialValue endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue());
        return this;
    }
    public CdtActionInvertableBuilder onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
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
    public CdtActionInvertableBuilder onMapKeyRelativeIndexRange(long key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    public CdtActionInvertableBuilder onMapKeyRelativeIndexRange(String key, int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index);
        return this;
    }

    public CdtActionInvertableBuilder onMapKeyRelativeIndexRange(byte[] key, int index) {
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
    public CdtActionInvertableBuilder onMapKeyRelativeIndexRange(long key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapKeyRelativeIndexRange(String key, int index, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapKeyRelativeIndexRange(byte[] key, int index, int count) {
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
    public CdtActionInvertableBuilder onMapValueRelativeRankRange(long value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(String value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(byte[] value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(double value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(boolean value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(List<?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(Map<?,?> value, int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(SpecialValue value, int rank) {
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
    public CdtActionInvertableBuilder onMapValueRelativeRankRange(long value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(String value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(byte[] value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(double value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(boolean value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(List<?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count);
        return this;
    }

    public CdtActionInvertableBuilder onMapValueRelativeRankRange(SpecialValue value, int rank, int count) {
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
    public CdtActionInvertableBuilder onMapIndexRange(int index, int count) {
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
    public CdtActionInvertableBuilder onMapIndexRange(int index) {
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
    public CdtActionInvertableBuilder onMapRankRange(int rank, int count) {
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
    public CdtActionInvertableBuilder onMapRankRange(int rank) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_RANK_RANGE, rank);
        return this;
    }

    public CdtContextNonInvertableBuilder onListIndex(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index);
        return this;
    }
    public CdtContextNonInvertableBuilder onListIndex(int index, ListOrder order, boolean pad) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_INDEX, index, order, pad);
        return this;
    }
    public CdtContextNonInvertableBuilder onListRank(int index) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_RANK, index);
        return this;
    }
    public CdtContextInvertableBuilder onListValue(long value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onListValue(String value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onListValue(byte[] value) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.LIST_BY_VALUE, Value.get(value));
        return this;
    }
    public CdtContextInvertableBuilder onListValue(SpecialValue value) {
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
    public CdtContextInvertableBuilder onMapKeyList(List<?> keys) {
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
    public CdtContextInvertableBuilder onMapValueList(List<?> values) {
        List<Value> valueList = new ArrayList<>();
        for (Object value : values) {
            valueList.add(Value.get(value));
        }
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_LIST, valueList);
        return this;
    }

    public OperationBuilder getAllOtherValues() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.VALUE | ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.VALUE | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherValues() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getAllOtherKeys() {
        switch (params.getOperation()) {
        // These operations cannot be used on the server to get the inverted value. This should not be allowed to occur

        case MAP_BY_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY | MapReturnType.INVERTED, params.context()));
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

    public OperationBuilder getAllOtherIndexes() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.INDEX | ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.INDEX | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getAllOtherReverseIndexes() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherReverseIndexes() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getAllOtherRanks() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.RANK | ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.RANK | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getAllOtherReverseRanks() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
        case LIST_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.REVERSE_RANK | ListReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherReverseRanks() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder getAllOtherKeysAndValues() {
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
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByIndexRange(binName, params.getInt1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_KEY_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyList(binName, params.getValues(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_RANK_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByRankRange(binName, params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValue(binName, params.getVal1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_LIST:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueList(binName, params.getValues(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_VALUE_RANGE:
            return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRange(binName, params.getVal1(), params.getVal2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
        case MAP_BY_KEY_REL_INDEX_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByKeyRelativeIndexRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            }
        case MAP_BY_VALUE_REL_RANK_RANGE:
            if (params.hasInt2()) {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), params.getInt2(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            } else {
                return (OperationBuilder) opBuilder.addOp(MapOperation.getByValueRelativeRankRange(binName, params.getVal1(), params.getInt1(), MapReturnType.KEY_VALUE | MapReturnType.INVERTED, params.context()));
            }
        default:
            throw new IllegalArgumentException("getAllOtherKeysAndValues() does not know how to handle an operation of " + params.getOperation());
        }
    }


    // ===============================
    // Setter methods after a mapIndex
    // ===============================

    // TODO: Fix map policy
    // TODO: Should they be part of the behavior? (No?)
    // TODO: What about the other MapWriteFlags values?

    public OperationBuilder setTo(long value) {
        if (params.getOperation() == CdtOperation.MAP_BY_KEY) {
            return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
        }
        else {
            // LIST_BY_INDEX
            return (OperationBuilder) this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
    }
    public OperationBuilder setTo(String value) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(byte[] value) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(boolean value) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(double value) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(List<?> value) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(Map<?,?> value) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <T> OperationBuilder setTo(T value, RecordMapper<T> mapper) {
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    public OperationBuilder insert(long value) {
        return insert(value, false);
    }
    public OperationBuilder insert(String value) {
        return insert(value, false);
    }
    public OperationBuilder insert(byte[] value) {
        return insert(value, false);
    }
    public OperationBuilder insert(boolean value) {
        return insert(value, false);
    }
    public OperationBuilder insert(double value) {
        return insert(value, false);
    }
    public OperationBuilder insert(List<?> value) {
        return insert(value, false);
    }
    public OperationBuilder insert(Map<?,?> value) {
        return insert(value, false);
    }
    public <T> OperationBuilder insert(T value, RecordMapper<T> mapper) {
        return insert(value, mapper, false);
    }


    public OperationBuilder insert(long value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(String value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(byte[] value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(boolean value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(double value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(List<?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(Map<?,?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <T> OperationBuilder insert(T value, RecordMapper<T> mapper, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }


    public OperationBuilder update(long value) {
        return update(value, false);
    }
    public OperationBuilder update(String value) {
        return update(value, false);
    }
    public OperationBuilder update(byte[] value) {
        return update(value, false);
    }
    public OperationBuilder update(boolean value) {
        return update(value, false);
    }
    public OperationBuilder update(double value) {
        return update(value, false);
    }
    public OperationBuilder update(List<?> value) {
        return update(value, false);
    }
    public OperationBuilder update(Map<?,?> value) {
        return update(value, false);
    }
    public <T> OperationBuilder update(T value, RecordMapper<T> mapper) {
        return update(value, mapper, false);
    }


    public OperationBuilder update(long value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(String value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(byte[] value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(boolean value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(double value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(List<?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(Map<?,?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <T> OperationBuilder update(T value, RecordMapper<T> mapper, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    public OperationBuilder add(long value) {
        return add(value, false);
    }
    public OperationBuilder add(double value) {
        return add(value, false);
    }
    public OperationBuilder add(long value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder add(double value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return (OperationBuilder) this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
}
