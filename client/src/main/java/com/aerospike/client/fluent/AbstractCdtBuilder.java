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

    private static final ConcurrentHashMap<Long, MapPolicy> MAP_POLICY_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Long, ListPolicy> LIST_POLICY_CACHE = new ConcurrentHashMap<>();

    /**
     * Return a cached, immutable {@link MapPolicy} for the given combination of order, write flags, and persist-index.
     * There are at most ~72 distinct permutations (3 orders x 12 flag combos x 2 persistIndex) so the cache stays tiny.
     */
    static MapPolicy cachedMapPolicy(MapOrder order, int writeFlags, boolean persistIndex) {
        long key = ((long) order.attributes << 32) | ((long) writeFlags << 1) | (persistIndex ? 1L : 0L);
        return MAP_POLICY_CACHE.computeIfAbsent(key, k ->
            persistIndex ? new MapPolicy(order, writeFlags, true) : new MapPolicy(order, writeFlags));
    }

    /**
     * Return a cached, immutable {@link ListPolicy} for the given combination of order and write flags.
     */
    static ListPolicy cachedListPolicy(ListOrder order, int writeFlags) {
        long key = ((long) order.attributes << 32) | (long) writeFlags;
        return LIST_POLICY_CACHE.computeIfAbsent(key, k -> new ListPolicy(order, writeFlags));
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

    // =================================
    // List write helpers
    // =================================

    private ListEntryWriteOptions applyListOptions(Consumer<ListEntryWriteOptions> options) {
        if (options == null) return null;
        ListEntryWriteOptions opts = new ListEntryWriteOptions();
        options.accept(opts);
        return opts;
    }

    private ListBulkWriteOptions applyListBulkOptions(Consumer<ListBulkWriteOptions> options) {
        if (options == null) return null;
        ListBulkWriteOptions opts = new ListBulkWriteOptions();
        options.accept(opts);
        return opts;
    }

    private ListPolicy resolveListPolicy(ListOrder order, ListWriteOptions<?> opts) {
        int flags = ListWriteFlags.DEFAULT;
        if (opts != null) {
            if (opts.isAddUnique()) flags |= ListWriteFlags.ADD_UNIQUE;
            if (opts.isInsertBounded()) flags |= ListWriteFlags.INSERT_BOUNDED;
            if (opts.isAllowFailures()) flags |= ListWriteFlags.NO_FAIL;
            if (opts instanceof ListBulkWriteOptions && ((ListBulkWriteOptions) opts).isAllowPartial()) {
                flags |= ListWriteFlags.PARTIAL;
            }
        }
        return cachedListPolicy(order, flags);
    }

    private java.util.List<Value> toValueList(java.util.List<?> items) {
        java.util.ArrayList<Value> result = new java.util.ArrayList<>(items.size());
        for (Object item : items) {
            result.add(Value.get(item));
        }
        return result;
    }

    // =================================
    // listAppend -- append to unordered list
    // =================================

    /** Append an item to the end of an unordered list. */
    public T listAppend(Value value) {
        return listAppend(value, (Consumer<ListEntryWriteOptions>) null);
    }
    public T listAppend(long value) { return listAppend(Value.get(value)); }
    public T listAppend(String value) { return listAppend(Value.get(value)); }
    public T listAppend(double value) { return listAppend(Value.get(value)); }
    public T listAppend(boolean value) { return listAppend(Value.get(value)); }
    public T listAppend(byte[] value) { return listAppend(Value.get(value)); }
    public T listAppend(List<?> value) { return listAppend(Value.get(value)); }
    public T listAppend(Map<?,?> value) { return listAppend(Value.get(value)); }

    /** Append an item to the end of an unordered list with options (e.g. addUnique, allowFailures). */
    public T listAppend(Value value, Consumer<ListEntryWriteOptions> options) {
        ListEntryWriteOptions opts = applyListOptions(options);
        if (opts != null && (opts.isAddUnique() || opts.isInsertBounded() || opts.isAllowFailures())) {
            ListPolicy policy = resolveListPolicy(ListOrder.UNORDERED, opts);
            if (params != null) {
                params.pushCurrentToContext();
                return this.opBuilder.addOp(ListOperation.append(policy, binName, value, params.context()));
            }
            else {
                return this.opBuilder.addOp(ListOperation.append(policy, binName, value));
            }
        }
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.append(binName, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.append(binName, value));
        }
    }
    public T listAppend(long value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }
    public T listAppend(String value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }
    public T listAppend(double value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }
    public T listAppend(boolean value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }
    public T listAppend(byte[] value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }
    public T listAppend(List<?> value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }
    public T listAppend(Map<?,?> value, Consumer<ListEntryWriteOptions> options) { return listAppend(Value.get(value), options); }

    // =================================
    // listAdd -- add to ordered list
    // =================================

    /** Add an item to the appropriate spot in an ordered list. */
    public T listAdd(Value value) {
        return listAdd(value, (Consumer<ListEntryWriteOptions>) null);
    }
    public T listAdd(long value) { return listAdd(Value.get(value)); }
    public T listAdd(String value) { return listAdd(Value.get(value)); }
    public T listAdd(double value) { return listAdd(Value.get(value)); }
    public T listAdd(boolean value) { return listAdd(Value.get(value)); }
    public T listAdd(byte[] value) { return listAdd(Value.get(value)); }
    public T listAdd(List<?> value) { return listAdd(Value.get(value)); }
    public T listAdd(Map<?,?> value) { return listAdd(Value.get(value)); }

    /** Add an item to the appropriate spot in an ordered list with options (e.g. addUnique, allowFailures). */
    public T listAdd(Value value, Consumer<ListEntryWriteOptions> options) {
        ListEntryWriteOptions opts = applyListOptions(options);
        ListPolicy policy = resolveListPolicy(ListOrder.ORDERED, opts);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.append(policy, binName, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.append(policy, binName, value));
        }
    }
    public T listAdd(long value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }
    public T listAdd(String value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }
    public T listAdd(double value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }
    public T listAdd(boolean value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }
    public T listAdd(byte[] value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }
    public T listAdd(List<?> value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }
    public T listAdd(Map<?,?> value, Consumer<ListEntryWriteOptions> options) { return listAdd(Value.get(value), options); }

    // =================================
    // Bulk list append/add
    // =================================

    /** Append multiple items to the end of an unordered list. */
    public T listAppendItems(List<?> items) {
        return listAppendItems(items, null);
    }

    /** Append multiple items to the end of an unordered list with options. */
    public T listAppendItems(List<?> items, Consumer<ListBulkWriteOptions> options) {
        ListBulkWriteOptions opts = applyListBulkOptions(options);
        java.util.List<Value> valueList = toValueList(items);
        if (opts != null && (opts.isAddUnique() || opts.isInsertBounded() || opts.isAllowFailures() || opts.isAllowPartial())) {
            ListPolicy policy = resolveListPolicy(ListOrder.UNORDERED, opts);
            if (params != null) {
                params.pushCurrentToContext();
                return this.opBuilder.addOp(ListOperation.appendItems(policy, binName, valueList, params.context()));
            }
            else {
                return this.opBuilder.addOp(ListOperation.appendItems(policy, binName, valueList));
            }
        }
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.appendItems(binName, valueList, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.appendItems(binName, valueList));
        }
    }

    /** Add multiple items to the appropriate spots in an ordered list. */
    public T listAddItems(List<?> items) {
        return listAddItems(items, null);
    }

    /** Add multiple items to the appropriate spots in an ordered list with options. */
    public T listAddItems(List<?> items, Consumer<ListBulkWriteOptions> options) {
        ListBulkWriteOptions opts = applyListBulkOptions(options);
        ListPolicy policy = resolveListPolicy(ListOrder.ORDERED, opts);
        java.util.List<Value> valueList = toValueList(items);
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.appendItems(policy, binName, valueList, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.appendItems(policy, binName, valueList));
        }
    }

    // =================================
    // List structural operations
    // =================================

    /** Return the number of items in the list. */
    public T listSize() {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.size(binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.size(binName));
        }
    }

    /** Remove all items from the list without deleting the bin. */
    public T listClear() {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.clear(binName, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.clear(binName));
        }
    }

    /** Sort the list using default sort flags. */
    public T listSort() {
        return listSort(0);
    }

    /**
     * Sort the list.
     * @param sortFlags sort flags (e.g. {@code ListSortFlags.DROP_DUPLICATES})
     */
    public T listSort(int sortFlags) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.sort(binName, sortFlags, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.sort(binName, sortFlags));
        }
    }

    /**
     * Create a list with the specified ordering.
     * Defaults to pad = {@code true} and persistIndex = {@code false}.
     *
     * @param order the list ordering
     * @see #listCreate(ListOrder, Consumer) for non-default pad/persistIndex options
     */
    public T listCreate(ListOrder order) {
        return listCreate(order, (Consumer<ListCreateOptions>) null);
    }

    /**
     * Create a list with the specified ordering and options.
     *
     * <pre>{@code
     * .bin("scores").listCreate(ListOrder.ORDERED, opt -> opt.noPad().persistIndex())
     * }</pre>
     *
     * @param order the list ordering
     * @param options optional configuration (noPad, persistIndex); defaults are pad=true, persistIndex=false
     */
    public T listCreate(ListOrder order, Consumer<ListCreateOptions> options) {
        boolean pad = true;
        boolean persistIndex = false;
        if (options != null) {
            ListCreateOptions opts = new ListCreateOptions();
            options.accept(opts);
            pad = opts.isPad();
            persistIndex = opts.isPersistIndex();
        }
        if (persistIndex) {
            if (params != null) {
                params.pushCurrentToContext();
                return this.opBuilder.addOp(ListOperation.create(binName, order, pad, true, params.context()));
            }
            else {
                return this.opBuilder.addOp(ListOperation.create(binName, order, pad, true));
            }
        }
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.create(binName, order, pad, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.create(binName, order, pad));
        }
    }

    /**
     * Set the ordering of an existing list.
     * @param order the list ordering to set
     */
    public T listSetOrder(ListOrder order) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.setOrder(binName, order, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.setOrder(binName, order));
        }
    }

    /**
     * Set the ordering of an existing list with persist index option.
     * @param order the list ordering to set
     * @param persistIndex if true, persist the list index for faster lookups
     */
    public T listSetOrder(ListOrder order, boolean persistIndex) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.setOrder(binName, order, persistIndex, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.setOrder(binName, order, persistIndex));
        }
    }

    // =================================
    // List index-based write operations
    // =================================

    /** Insert a value at the specified list index, shifting subsequent elements right. */
    public T listInsert(int index, Value value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.insert(binName, index, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.insert(binName, index, value));
        }
    }
    public T listInsert(int index, long value) { return listInsert(index, Value.get(value)); }
    public T listInsert(int index, String value) { return listInsert(index, Value.get(value)); }
    public T listInsert(int index, double value) { return listInsert(index, Value.get(value)); }
    public T listInsert(int index, boolean value) { return listInsert(index, Value.get(value)); }
    public T listInsert(int index, byte[] value) { return listInsert(index, Value.get(value)); }

    /** Insert multiple items at the specified list index. */
    public T listInsertItems(int index, List<?> items) {
        return listInsertItems(index, items, null);
    }

    /** Insert multiple items at the specified list index with options. */
    public T listInsertItems(int index, List<?> items, Consumer<ListBulkWriteOptions> options) {
        ListBulkWriteOptions opts = applyListBulkOptions(options);
        java.util.List<Value> valueList = toValueList(items);
        if (opts != null && (opts.isAddUnique() || opts.isInsertBounded() || opts.isAllowFailures() || opts.isAllowPartial())) {
            ListPolicy policy = resolveListPolicy(ListOrder.UNORDERED, opts);
            if (params != null) {
                params.pushCurrentToContext();
                return this.opBuilder.addOp(ListOperation.insertItems(policy, binName, index, valueList, params.context()));
            }
            else {
                return this.opBuilder.addOp(ListOperation.insertItems(policy, binName, index, valueList));
            }
        }
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.insertItems(binName, index, valueList, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.insertItems(binName, index, valueList));
        }
    }

    /** Set (replace) the value at the specified list index. */
    public T listSet(int index, Value value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.set(binName, index, value, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.set(binName, index, value));
        }
    }
    public T listSet(int index, long value) { return listSet(index, Value.get(value)); }
    public T listSet(int index, String value) { return listSet(index, Value.get(value)); }
    public T listSet(int index, double value) { return listSet(index, Value.get(value)); }
    public T listSet(int index, boolean value) { return listSet(index, Value.get(value)); }
    public T listSet(int index, byte[] value) { return listSet(index, Value.get(value)); }

    /** Increment the value at the specified list index by 1. */
    public T listIncrement(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.increment(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.increment(binName, index));
        }
    }

    /** Increment the value at the specified list index by the given amount. */
    public T listIncrement(int index, long value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.increment(binName, index, Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.increment(binName, index, Value.get(value)));
        }
    }

    /** Increment the value at the specified list index by the given amount. */
    public T listIncrement(int index, double value) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.increment(binName, index, Value.get(value), params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.increment(binName, index, Value.get(value)));
        }
    }

    // =================================
    // List index-based read operations
    // =================================

    /** Get the value at the specified list index. */
    public T listGet(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.get(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.get(binName, index));
        }
    }

    /** Get values from the specified list index to the end of the list. */
    public T listGetRange(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.getRange(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.getRange(binName, index));
        }
    }

    /** Get {@code count} values starting at the specified list index. */
    public T listGetRange(int index, int count) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.getRange(binName, index, count, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.getRange(binName, index, count));
        }
    }

    // =================================
    // List index-based remove operations
    // =================================

    /** Remove and discard the item at the specified list index. */
    public T listRemove(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.remove(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.remove(binName, index));
        }
    }

    /** Remove and discard items from the specified index to the end of the list. */
    public T listRemoveRange(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.removeRange(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.removeRange(binName, index));
        }
    }

    /** Remove and discard {@code count} items starting at the specified index. */
    public T listRemoveRange(int index, int count) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.removeRange(binName, index, count, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.removeRange(binName, index, count));
        }
    }

    /** Remove and return the item at the specified list index. */
    public T listPop(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.pop(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.pop(binName, index));
        }
    }

    /** Remove and return items from the specified index to the end of the list. */
    public T listPopRange(int index) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.popRange(binName, index, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.popRange(binName, index));
        }
    }

    /** Remove and return {@code count} items starting at the specified index. */
    public T listPopRange(int index, int count) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.popRange(binName, index, count, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.popRange(binName, index, count));
        }
    }

    /** Keep {@code count} items starting at the specified index, removing all others. */
    public T listTrim(int index, int count) {
        if (params != null) {
            params.pushCurrentToContext();
            return this.opBuilder.addOp(ListOperation.trim(binName, index, count, params.context()));
        }
        else {
            return this.opBuilder.addOp(ListOperation.trim(binName, index, count));
        }
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
