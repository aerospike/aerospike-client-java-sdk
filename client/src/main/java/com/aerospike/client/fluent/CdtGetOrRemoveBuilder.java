package com.aerospike.client.fluent;

import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.cdt.ListOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.ListReturnType;
import com.aerospike.client.fluent.cdt.MapOperation;
import com.aerospike.client.fluent.cdt.MapOrder;
import com.aerospike.client.fluent.cdt.MapPolicy;
import com.aerospike.client.fluent.cdt.MapReturnType;

public class CdtGetOrRemoveBuilder extends AbstractCdtBuilder
                                implements CdtActionInvertableBuilder, CdtActionNonInvertableBuilder,
                                            CdtContextInvertableBuilder, CdtContextNonInvertableBuilder,
                                            CdtSetterInvertableBuilder, CdtSetterNonInvertableBuilder {

    protected static enum CdtOperation {
        MAP_BY_INDEX,
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

    public CdtGetOrRemoveBuilder(String binName, OperationBuilder opBuilder, CdtOperationParams params) {
        super(opBuilder, binName, params);
    }

    public OperationBuilder getValues() {
        switch (params.getOperation()) {
        case CdtOperation.MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.VALUE, params.context()));
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
        default:
            throw new IllegalArgumentException("getValues() does not know how to handle an operation of " + params.getOperation());
        }
    }

    // TODO: This should be limited so it can only get invoked on maps
    public OperationBuilder getKeys() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.KEY, params.context()));
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
            return opBuilder.addOp(MapOperation.getByIndex(binName, params.getInt1(), MapReturnType.COUNT, params.context()));
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
        default:
            throw new IllegalArgumentException("countAllOthers() does not know how to handle an operation of " + params.getOperation());
        }
    }

    public OperationBuilder remove() {
        switch (params.getOperation()) {
        case MAP_BY_INDEX:
            return opBuilder.addOp(MapOperation.removeByIndex(binName, params.getInt1(), MapReturnType.NONE, params.context()));
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
            return opBuilder.addOp(ListOperation.getByValue(binName, params.getVal1(), ListReturnType.INVERTED, params.context()));
        default:
            throw new IllegalArgumentException("remove() does not know how to handle an operation of " + params.getOperation());
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
    public CdtContextInvertableBuilder onMapKeuRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl));
        return this;
    }
    public CdtContextInvertableBuilder onMapValueRange(long startIncl, long endExcl) {
        params.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl));
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




    // ===============================
    // Setter methods after a mapIndex
    // ===============================

    // TODO: Fix map policy
    // TODO: Should they be part of the behavior? (No?)
    // TODO: What about the other MapWriteFlags values?

    public OperationBuilder setTo(long value) {
        if (params.getOperation() == CdtOperation.MAP_BY_KEY) {
            return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
        }
        else {
            // LIST_BY_INDEX
            return this.opBuilder.addOp(ListOperation.insert(binName, params.getInt1(), Value.get(value), params.context()));
        }
    }
    public OperationBuilder setTo(String value) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(byte[] value) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(boolean value) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(double value) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(List<?> value) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder setTo(Map<?,?> value) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <T> OperationBuilder setTo(T value, RecordMapper<T> mapper) {
        return this.opBuilder.addOp(MapOperation.put(MapPolicy.Default, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
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
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(String value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(byte[] value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(boolean value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(double value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(List<?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder insert(Map<?,?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_CREATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <T> OperationBuilder insert(T value, RecordMapper<T> mapper, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_CREATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
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
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(String value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(byte[] value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(boolean value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(double value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(List<?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder update(Map<?,?> value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public <T> OperationBuilder update(T value, RecordMapper<T> mapper, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.put(mp, binName, params.getVal1(), Value.get(mapper.toMap(value)), params.context()));
    }

    public OperationBuilder add(long value) {
        return add(value, false);
    }
    public OperationBuilder add(double value) {
        return add(value, false);
    }
    public OperationBuilder add(long value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
    public OperationBuilder add(double value, boolean allowFailures) {
        MapPolicy mp = allowFailures ? KEY_ORDERED_UPDATE_ONLY_NO_FAIL : KEY_ORDERED_UPDATE_ONLY;
        return this.opBuilder.addOp(MapOperation.increment(mp, binName, params.getVal1(), Value.get(value), params.context()));
    }
}
