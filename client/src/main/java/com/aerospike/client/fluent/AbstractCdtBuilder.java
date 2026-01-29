package com.aerospike.client.fluent;

import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.cdt.ListOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.ListPolicy;
import com.aerospike.client.fluent.cdt.ListWriteFlags;
import com.aerospike.client.fluent.cdt.MapOperation;
import com.aerospike.client.fluent.cdt.MapOrder;
import com.aerospike.client.fluent.cdt.MapPolicy;
import com.aerospike.client.fluent.cdt.MapWriteFlags;

public class AbstractCdtBuilder<T extends AbstractOperationBuilder<T>> {
    public static final MapPolicy KEY_ORDERED = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    public static final MapPolicy KEY_ORDERED_CREATE_ONLY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY);
    public static final MapPolicy KEY_ORDERED_CREATE_ONLY_NO_FAIL = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL);
    public static final MapPolicy KEY_ORDERED_UPDATE_ONLY = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.UPDATE_ONLY);
    public static final MapPolicy KEY_ORDERED_UPDATE_ONLY_NO_FAIL = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.UPDATE_ONLY | MapWriteFlags.NO_FAIL);
    public static final ListPolicy LIST_ORD = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT);
    public static final ListPolicy LIST_UNORD = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);
    public static final ListPolicy LIST_ORD_UNIQUE = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE);
    public static final ListPolicy LIST_UNORD_UNIQUE = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);
    public static final ListPolicy LIST_ORD_UNIQUE_NO_FAIL = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
    public static final ListPolicy LIST_UNORD_UNIQUE_NO_FAIL = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);

    protected final T opBuilder;
    protected final String binName;
    protected final CdtOperationParams params;

    public AbstractCdtBuilder(T opBuilder, String binName, CdtOperationParams params) {
        this.opBuilder = opBuilder;
        this.binName = binName;
        this.params = params;
    }

    public T mapClear() {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.clear(binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.clear(binName));
        }
    }

    public T mapSize() {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.size(binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.size(binName));
        }
    }

    public T listAppend(Value value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.append(binName, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.append(binName, value));
        }
    }
    public T listAppend(long value) {
        return listAppend(Value.get(value));
    }
    public T listAppend(String value) {
        return listAppend(Value.get(value));
    }
    public T listAppend(double value) {
        return listAppend(Value.get(value));
    }
    public T listAppend(boolean value) {
        return listAppend(Value.get(value));
    }
    public T listAppend(byte[] value) {
        return listAppend(Value.get(value));
    }
    public T listAppend(List<?> value) {
        return listAppend(Value.get(value));
    }
    public T listAppend(Map<?,?> value) {
        return listAppend(Value.get(value));
    }

    public T listAppendUnique(Value value, boolean allowFailures) {
        if (params != null) {
            params.pushCurrentToContext();
            if (allowFailures) {
                return this.opBuilder.addOp(ListOperation.append(LIST_UNORD_UNIQUE_NO_FAIL, binName, value, params.context()));
            }
            else {
                return this.opBuilder.addOp(ListOperation.append(LIST_UNORD_UNIQUE, binName, value, params.context()));
            }
        }
        else {
            if (allowFailures) {
                return this.opBuilder.addOp(ListOperation.append(LIST_UNORD_UNIQUE_NO_FAIL, binName, value));
            }
            else {
                return this.opBuilder.addOp(ListOperation.append(LIST_UNORD_UNIQUE, binName, value));
            }
        }
    }
    public T listAppendUnique(long value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public T listAppendUnique(String value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public T listAppendUnique(double value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public T listAppendUnique(boolean value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public T listAppendUnique(byte[] value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public T listAppendUnique(List<?> value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public T listAppendUnique(Map<?,?> value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }

    public T listAdd(Value value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.append(LIST_ORD, binName, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.append(LIST_ORD, binName, value));
        }
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(long value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(String value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(double value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(boolean value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(byte[] value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(List<?> value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(Map<?,?> value) {
        return listAdd(Value.get(value));
    }

    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(Value value, boolean allowFailures) {
        if (params != null) {
            params.pushCurrentToContext();
            if (allowFailures) {
                return this.opBuilder.addOp(ListOperation.append(LIST_ORD_UNIQUE_NO_FAIL, binName, value, params.context()));
            }
            else {
                return this.opBuilder.addOp(ListOperation.append(LIST_ORD_UNIQUE, binName, value, params.context()));
            }
        }
        else {
            if (allowFailures) {
                return this.opBuilder.addOp(ListOperation.append(LIST_ORD_UNIQUE_NO_FAIL, binName, value));
            }
            else {
                return this.opBuilder.addOp(ListOperation.append(LIST_ORD_UNIQUE, binName, value));
            }
        }
    }

    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(long value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(String value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(double value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(boolean value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(byte[] value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(List<?> value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(Map<?,?> value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
}
