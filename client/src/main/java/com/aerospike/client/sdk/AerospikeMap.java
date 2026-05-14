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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.aerospike.client.sdk.cdt.MapOrder;

/**
 * {@link Map} implementation that records how entries should be encoded as an Aerospike map. The
 * backing collection is chosen from the {@link MapOrder}: a {@link HashMap} for
 * {@link MapOrder#UNORDERED}, or a {@link TreeMap} keyed by {@link AerospikeComparator} for
 * {@link MapOrder#KEY_ORDERED} and {@link MapOrder#KEY_VALUE_ORDERED}.
 * <p>
 * Use {@link #withOrder(MapOrder)} before writes when you need to change encoding without
 * rebuilding the map manually. {@link #persistIndex()} applies only to non-unordered maps.
 * <p>
 * Ordinary {@link Map} operations delegate to the backing map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class AerospikeMap<K,V> implements Map<K,V> {
    private static final AerospikeComparator Comparator = new AerospikeComparator();
    private enum Backing {HASH, TREE, LINKED}

    /**
     * Create an empty map with the given {@link MapOrder} and initial capacity. {@link MapOrder#UNORDERED}
     * produces a {@link HashMap} backing; ordered variants produce a {@link TreeMap} keyed by
     * {@link AerospikeComparator}.
     *
     * @param order    map encoding order
     * @param capacity initial capacity hint for the backing map (used only for hash backing)
     * @return a new empty {@code AerospikeMap}
     */
    public static <K,V> AerospikeMap<K,V> of(MapOrder order, int capacity) {
        return new AerospikeMap<>(order, capacity);
    }

    /**
     * Create a map with the given {@link MapOrder} pre-populated from {@code src}. Entries are copied
     * into the backing map appropriate for the chosen order.
     *
     * @param order map encoding order
     * @param src   source map whose entries are copied
     * @return a new {@code AerospikeMap} containing the entries of {@code src}
     */
    public static <K,V> AerospikeMap<K,V> of(MapOrder order, Map<? extends K,? extends V> src) {
        return new AerospikeMap<>(order, src);
    }

    /**
     * Wrap an existing {@link HashMap} and tag it as {@link MapOrder#UNORDERED}. The map is stored
     * by reference; subsequent mutations through either reference are visible to both.
     *
     * @param map backing map (stored by reference)
     * @return a new {@code AerospikeMap} delegating to {@code map}
     */
    public static <K,V> AerospikeMap<K,V> of(HashMap<K,V> map) {
        return new AerospikeMap<>(map);
    }

    /**
     * Wrap an existing {@link TreeMap} and tag it as {@link MapOrder#KEY_ORDERED}. Prefer a
     * {@link TreeMap} constructed with {@link AerospikeComparator} so key order matches the server.
     * The map is stored by reference.
     *
     * @param map backing map (stored by reference)
     * @return a new {@code AerospikeMap} delegating to {@code map}
     */
    public static <K,V> AerospikeMap<K,V> of(TreeMap<K,V> map) {
        return new AerospikeMap<>(map);
    }

    /**
     * Wrap an existing {@link LinkedHashMap} and tag it as {@link MapOrder#KEY_ORDERED}. The map is
     * stored by reference. Note that subsequent mutating {@link Map} operations may convert the
     * backing collection to a {@link TreeMap} to maintain key order.
     *
     * @param map backing map (stored by reference)
     * @return a new {@code AerospikeMap} delegating to {@code map}
     */
    public static <K,V> AerospikeMap<K,V> of(LinkedHashMap<K,V> map) {
        return new AerospikeMap<>(map);
    }

    /**
     * Store LinkedHashMap directly without implicit TreeMap conversion.
     * For internal use only.
     */
    public static <K,V> AerospikeMap<K,V> forInternalUnpack(
        LinkedHashMap<K,V> map, MapOrder order, boolean persistIndex
    ) {
        return new AerospikeMap<>(map, order, persistIndex);
    }

    private Map<K,V> map;
    private Backing backing;
    private MapOrder order;
    private boolean persistIndex;

    private AerospikeMap(MapOrder order, Map<? extends K,? extends V> src) {
        this(order, src.size());
        putAll(src);
    }

    private AerospikeMap(MapOrder order, int capacity) {
        this.order = order;
        this.persistIndex = false;

        switch (order) {
            case UNORDERED -> {
                backing = Backing.HASH;
                map = new HashMap<>(capacity);
            }
            case KEY_ORDERED, KEY_VALUE_ORDERED -> {
                backing = Backing.TREE;
                map = new TreeMap<>(Comparator);
            }
            default -> throw new AssertionError(order);
        }
    }

    private AerospikeMap(HashMap<K,V> map) {
        this.map = map;
        this.backing = Backing.HASH;
        this.order = MapOrder.UNORDERED;
        this.persistIndex = false;
    }

    private AerospikeMap(TreeMap<K,V> map) {
        this.map = map;
        this.backing = Backing.TREE;
        this.order = MapOrder.KEY_ORDERED;
        this.persistIndex = false;
    }

    private AerospikeMap(LinkedHashMap<K,V> map) {
        this.map = map;
        this.backing = Backing.LINKED;
        this.order = MapOrder.KEY_ORDERED;
        this.persistIndex = false;
    }

    private AerospikeMap(LinkedHashMap<K,V> map, MapOrder order, boolean persistIndex) {
        this.map = map;
        this.backing = Backing.LINKED;
        this.order = order;
        this.persistIndex = persistIndex;
    }

    /**
     * Change the {@link MapOrder} and rebuild the backing collection if needed. Switching to
     * {@link MapOrder#UNORDERED} moves entries into a {@link HashMap}; switching to
     * {@link MapOrder#KEY_ORDERED} or {@link MapOrder#KEY_VALUE_ORDERED} moves them into a
     * {@link TreeMap} keyed by {@link AerospikeComparator}. A no-op when {@code newOrder} matches
     * the current order.
     *
     * @param newOrder target encoding order
     * @return this instance for chaining
     */
    public AerospikeMap<K,V> withOrder(MapOrder newOrder) {
        if (this.order == newOrder) {
            return this;
        }

        this.order = newOrder;

        if (newOrder == MapOrder.UNORDERED) {
            if (backing != Backing.HASH) {
                this.map = new HashMap<>(this.map);
                this.backing = Backing.HASH;
            }
        }
        else {
            // KEY_ORDERED or KEY_VALUE_ORDERED
            if (backing != Backing.TREE) {
                TreeMap<K,V> t = new TreeMap<>(Comparator);
                t.putAll(this.map);
                this.map = t;
                this.backing = Backing.TREE;
            }
        }
        return this;
    }

    /**
     * Request that a map index be persisted when encoding {@link MapOrder#KEY_VALUE_ORDERED}
     * maps. A persisted index can improve lookup performance on the server at the cost of extra storage.
     *
     * @return this instance for chaining
     */
    public AerospikeMap<K,V> persistIndex() {
        this.persistIndex = true;
        return this;
    }

    /**
     * Return the backing {@link Map} instance.
     */
    public Map<K,V> getMap() {
        return map;
    }

    /**
     * Return the Aerospike map order used when packing this value.
     */
    public MapOrder getOrder() {
        return order;
    }

    /**
     * Return whether persisted map index is requested for linked / ordered encoding
     */
    public boolean isPersistIndex() {
        return persistIndex;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    /** {@inheritDoc} */
    @Override
    public V get(Object key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public V put(K key, V value) {
        ensureTreeForOrderedMutation();
        return map.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public V remove(Object key) {
        return map.remove(key);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        ensureTreeForOrderedMutation();
        map.putAll(m);
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        map.clear();
    }

    /** {@inheritDoc} */
    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<V> values() {
        return map.values();
    }

    /** {@inheritDoc} */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return map.toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
        return map.equals(other);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return map.hashCode();
    }

    private void ensureTreeForOrderedMutation() {
        if (backing == Backing.LINKED) {
            TreeMap<K,V> t = new TreeMap<>(Comparator);
            t.putAll(map);
            map = t;
            backing = Backing.TREE;
        }
    }
}
