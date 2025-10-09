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

public class AbstractCdtBuilder {
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

    protected final OperationBuilder opBuilder;
    protected final String binName;
    protected final CdtOperationParams params;

    public AbstractCdtBuilder(OperationBuilder opBuilder, String binName, CdtOperationParams params) {
        this.opBuilder = opBuilder;
        this.binName = binName;
        this.params = params;
    }

    public OperationBuilder mapClear() {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.clear(binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.clear(binName));
        }
    }

    public OperationBuilder mapSize() {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.size(binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.size(binName));
        }
    }

    public OperationBuilder listAppend(Value value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.append(binName, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.append(binName, value));
        }
    }
    public OperationBuilder listAppend(long value) {
        return listAppend(Value.get(value));
    }
    public OperationBuilder listAppend(String value) {
        return listAppend(Value.get(value));
    }
    public OperationBuilder listAppend(double value) {
        return listAppend(Value.get(value));
    }
    public OperationBuilder listAppend(boolean value) {
        return listAppend(Value.get(value));
    }
    public OperationBuilder listAppend(byte[] value) {
        return listAppend(Value.get(value));
    }
    public OperationBuilder listAppend(List<?> value) {
        return listAppend(Value.get(value));
    }
    public OperationBuilder listAppend(Map<?,?> value) {
        return listAppend(Value.get(value));
    }

    public OperationBuilder listAppendUnique(Value value, boolean allowFailures) {
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
    public OperationBuilder listAppendUnique(long value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public OperationBuilder listAppendUnique(String value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public OperationBuilder listAppendUnique(double value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public OperationBuilder listAppendUnique(boolean value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public OperationBuilder listAppendUnique(byte[] value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public OperationBuilder listAppendUnique(List<?> value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }
    public OperationBuilder listAppendUnique(Map<?,?> value, boolean allowFailures) {
        return listAppendUnique(Value.get(value), allowFailures);
    }

    public OperationBuilder listAdd(Value value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.append(LIST_ORD, binName, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.append(LIST_ORD, binName, value));
        }
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(long value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(String value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(double value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(boolean value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(byte[] value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(List<?> value) {
        return listAdd(Value.get(value));
    }
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(Map<?,?> value) {
        return listAdd(Value.get(value));
    }

    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(Value value, boolean allowFailures) {
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
    public OperationBuilder listAddUnique(long value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(String value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(double value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(boolean value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(byte[] value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(List<?> value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(Map<?,?> value, boolean allowFailures) {
        return listAddUnique(Value.get(value), allowFailures);
    }
}
