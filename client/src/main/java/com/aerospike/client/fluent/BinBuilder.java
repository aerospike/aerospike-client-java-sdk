package com.aerospike.client.fluent;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.aerospike.client.fluent.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.MapOrder;

public class BinBuilder extends AbstractCdtBuilder{
    public BinBuilder(OperationBuilder opBuilder, String binName) {
        super(opBuilder, binName, null);
    }

    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(String value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(int value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(long value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(float value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(double value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(boolean value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(byte[] value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(List<?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(Map<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder setTo(SortedMap<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }
    /**
     * Create bin-remove operation.
     */
    public OperationBuilder remove() {
        return opBuilder.setTo(Bin.asNull(binName));
    }
    /**
     * Create set database operation.
     */
    public OperationBuilder get() {
        return opBuilder.get(binName);
    }
    /**
     * Create string append database operation.
     */
    public OperationBuilder append(String value) {
        return opBuilder.append(new Bin(binName, value));
    }
    /**
     * Create string prepend database operation.
     */
    public OperationBuilder prepend(String value) {
        return opBuilder.prepend(new Bin(binName, value));
    }
    /**
     * Create integer add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public OperationBuilder add(int amount) {
        return opBuilder.add(new Bin(binName, amount));
    }
    /**
     * Create long add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public OperationBuilder add(long amount) {
        return opBuilder.add(new Bin(binName, amount));
    }
    /**
     * Create float add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public OperationBuilder add(float amount) {
        return opBuilder.add(new Bin(binName, amount));
    }
    /**
     * Create double add database operation. If the record or bin does not exist, the
     * record/bin will be created by default with the value to be added.
     */
    public OperationBuilder add(double amount) {
        return opBuilder.add(new Bin(binName, amount));
    }

    // ==================================================================
    // CDT Operations. Note: make sure to mirror these operations to
    // CdtContextNonInvertableBuilder and CdtContextInvertableBuilder
    // ==================================================================
    public CdtContextNonInvertableBuilder onMapIndex(int index) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX, index));
    }
    public CdtSetterNonInvertableBuilder onMapKey(long key) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    public CdtSetterNonInvertableBuilder onMapKey(long key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }
    public CdtSetterNonInvertableBuilder onMapKey(String key) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    public CdtSetterNonInvertableBuilder onMapKey(String key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }
    public CdtSetterNonInvertableBuilder onMapKey(byte[] key) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }
    public CdtSetterNonInvertableBuilder onMapKey(byte[] key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }
    public CdtContextNonInvertableBuilder onMapRank(int index) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK, index));
    }
    public CdtContextInvertableBuilder onMapValue(long value) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder onMapValue(String value) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder onMapValue(byte[] value) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }
    public CdtActionInvertableBuilder onMapKeyRange(String startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtActionInvertableBuilder onMapValueRange(long startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    public CdtContextNonInvertableBuilder onListIndex(int index) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }
    public CdtContextNonInvertableBuilder onListIndex(int index, ListOrder order, boolean pad) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index, order, pad));
    }
    public CdtContextNonInvertableBuilder onListRank(int index) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_RANK, index));
    }
    public CdtContextInvertableBuilder onListValue(long value) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder onListValue(String value) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }
    public CdtContextInvertableBuilder onListValue(byte[] value) {
        return new CdtGetOrRemoveBuilder(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }
}