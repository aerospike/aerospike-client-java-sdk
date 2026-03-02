package com.aerospike.client.fluent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.aerospike.client.fluent.cdt.ListOperation;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.ListPolicy;
import com.aerospike.client.fluent.cdt.ListWriteFlags;
import com.aerospike.client.fluent.cdt.MapOperation;
import com.aerospike.client.fluent.cdt.MapOrder;
import com.aerospike.client.fluent.cdt.MapPolicy;
import com.aerospike.client.fluent.cdt.MapWriteFlags;

public class AbstractCdtBuilder<T extends AbstractOperationBuilder<T>> {
    public static final ListPolicy LIST_ORD = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT);
    public static final ListPolicy LIST_UNORD = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);
    public static final ListPolicy LIST_ORD_UNIQUE = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE);
    public static final ListPolicy LIST_UNORD_UNIQUE = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);
    public static final ListPolicy LIST_ORD_UNIQUE_NO_FAIL = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
    public static final ListPolicy LIST_UNORD_UNIQUE_NO_FAIL = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);

    private static final ConcurrentHashMap<Long, MapPolicy> MAP_POLICY_CACHE = new ConcurrentHashMap<>();

    /**
     * Return a cached, immutable {@link MapPolicy} for the given combination of order, write flags, and persist-index.
     * There are at most ~72 distinct permutations (3 orders x 12 flag combos x 2 persistIndex) so the cache stays tiny.
     */
    static MapPolicy cachedMapPolicy(MapOrder order, int writeFlags, boolean persistIndex) {
        long key = ((long) order.attributes << 32) | ((long) writeFlags << 1) | (persistIndex ? 1L : 0L);
        return MAP_POLICY_CACHE.computeIfAbsent(key, k ->
            persistIndex ? new MapPolicy(order, writeFlags, true) : new MapPolicy(order, writeFlags));
    }

    protected final T opBuilder;
    protected final String binName;
    protected final CdtOperationParams params;

    public AbstractCdtBuilder(T opBuilder, String binName, CdtOperationParams params) {
        this.opBuilder = opBuilder;
        this.binName = binName;
        this.params = params;
    }

    /**
     * Create a map at the current context level with the specified order.
     * If no context is present, this sets the order of the top-level bin map.
     *
     * @param order the map ordering to use (e.g. {@code MapOrder.KEY_ORDERED})
     */
    public T mapCreate(MapOrder order) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.create(binName, order, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.create(binName, order));
        }
    }

    /**
     * Create a map at the current context level with the specified order and persist index option.
     * If no context is present, this sets the order of the top-level bin map.
     * A persisted index improves lookup performance but requires more storage.
     * Only supported for top-level ordered maps.
     *
     * @param order the map ordering to use
     * @param persistIndex if true, persist the map index for faster lookups
     */
    public T mapCreate(MapOrder order, boolean persistIndex) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.create(binName, order, persistIndex, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.create(binName, order, persistIndex));
        }
    }

    /**
     * Set the map policy (ordering) for a map. If the map does not exist at the top level,
     * it will be created with the specified ordering. However, this method cannot create
     * nested maps -- use {@link #mapCreate(MapOrder)} for that.
     *
     * @param order the map ordering to set
     */
    public T mapSetPolicy(MapOrder order) {
        MapPolicy policy = cachedMapPolicy(order, MapWriteFlags.DEFAULT, false);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.setMapPolicy(policy, binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.setMapPolicy(policy, binName));
        }
    }

    /**
     * Set the map policy (ordering) for a map, with persist index option. If the map does
     * not exist at the top level, it will be created with the specified ordering. However,
     * this method cannot create nested maps -- use {@link #mapCreate(MapOrder, boolean)} for that.
     *
     * @param order the map ordering to set
     * @param persistIndex if true, persist the map index for faster lookups
     */
    public T mapSetPolicy(MapOrder order, boolean persistIndex) {
        MapPolicy policy = cachedMapPolicy(order, MapWriteFlags.DEFAULT, persistIndex);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.setMapPolicy(policy, binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.setMapPolicy(policy, binName));
        }
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

    // =================================
    // Bulk map write operations
    // =================================

    private MapBulkWriteOptions applyBulkOptions(Consumer<MapBulkWriteOptions> options) {
        if (options == null) return null;
        MapBulkWriteOptions opts = new MapBulkWriteOptions();
        options.accept(opts);
        return opts;
    }

    private MapPolicy resolveBulkMapPolicy(int baseFlags, MapWriteOptions<?> opts) {
        MapOrder order = (opts != null && opts.getMapOrder() != null) ? opts.getMapOrder()
                       : (params != null && params.getMapCreateType() != null) ? params.getMapCreateType()
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

    private Map<Value, Value> toValueMap(Map<?, ?> items) {
        HashMap<Value, Value> result = new HashMap<>(items.size());
        for (Map.Entry<?, ?> entry : items.entrySet()) {
            result.put(Value.get(entry.getKey()), Value.get(entry.getValue()));
        }
        return result;
    }

    /**
     * Upsert (create or update) multiple map entries in a single operation.
     * If the map does not already exist, it will be created with {@code MapOrder.KEY_ORDERED} by default.
     *
     * @param items the key-value pairs to upsert
     */
    public T mapUpsertItems(Map<?, ?> items) {
        return mapUpsertItems(items, null);
    }

    /**
     * Upsert (create or update) multiple map entries with options.
     *
     * @param items   the key-value pairs to upsert
     * @param options optional configuration (mapOrder, persistIndex, allowFailures, allowPartial)
     */
    public T mapUpsertItems(Map<?, ?> items, Consumer<MapBulkWriteOptions> options) {
        MapBulkWriteOptions opts = applyBulkOptions(options);
        MapPolicy mp = resolveBulkMapPolicy(MapWriteFlags.DEFAULT, opts);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.putItems(mp, binName, toValueMap(items), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.putItems(mp, binName, toValueMap(items)));
        }
    }

    /**
     * Insert (create only) multiple map entries in a single operation.
     * Fails if any key already exists, unless {@code allowFailures()} is set.
     * If the map does not already exist, it will be created with {@code MapOrder.KEY_ORDERED} by default.
     *
     * @param items the key-value pairs to insert
     */
    public T mapInsertItems(Map<?, ?> items) {
        return mapInsertItems(items, null);
    }

    /**
     * Insert (create only) multiple map entries with options.
     *
     * @param items   the key-value pairs to insert
     * @param options optional configuration (mapOrder, persistIndex, allowFailures, allowPartial)
     */
    public T mapInsertItems(Map<?, ?> items, Consumer<MapBulkWriteOptions> options) {
        MapBulkWriteOptions opts = applyBulkOptions(options);
        MapPolicy mp = resolveBulkMapPolicy(MapWriteFlags.CREATE_ONLY, opts);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.putItems(mp, binName, toValueMap(items), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.putItems(mp, binName, toValueMap(items)));
        }
    }

    /**
     * Update (update only) multiple map entries in a single operation.
     * Fails if any key does not exist, unless {@code allowFailures()} is set.
     * If the map does not already exist, it will be created with {@code MapOrder.KEY_ORDERED} by default.
     *
     * @param items the key-value pairs to update
     */
    public T mapUpdateItems(Map<?, ?> items) {
        return mapUpdateItems(items, null);
    }

    /**
     * Update (update only) multiple map entries with options.
     *
     * @param items   the key-value pairs to update
     * @param options optional configuration (mapOrder, persistIndex, allowFailures, allowPartial)
     */
    public T mapUpdateItems(Map<?, ?> items, Consumer<MapBulkWriteOptions> options) {
        MapBulkWriteOptions opts = applyBulkOptions(options);
        MapPolicy mp = resolveBulkMapPolicy(MapWriteFlags.UPDATE_ONLY, opts);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(MapOperation.putItems(mp, binName, toValueMap(items), params.context()));
        }
        else {
            return this.opBuilder.addOp(MapOperation.putItems(mp, binName, toValueMap(items)));
        }
    }
}
