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
 * {@link Map} implementation that records how entries should be encoded as an Aerospike map
 * (unordered, insertion-ordered / linked, or key-sorted). The backing collection matches the
 * {@link Type}: {@link HashMap}, {@link LinkedHashMap}, or {@link TreeMap} ordered by
 * {@link AerospikeComparator} for {@link Type#TREE}.
 * <p>
 * Use {@link #withType(Type)} before writes when you need to change encoding without rebuilding
 * the map manually. {@link Type#LINKED} and {@link Type#TREE} are packed as key-ordered maps on
 * the wire; {@link #persistIndex()} applies only to those modes.
 * <p>
 * Ordinary {@link Map} operations delegate to the backing map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class AerospikeMap<K,V> implements Map<K,V> {
    private static final AerospikeComparator Comparator = new AerospikeComparator();

    /**
     * Aerospike map wire layout: unordered map, insertion-ordered (linked) map, or map sorted by
     * Aerospike value ordering on keys.
     */
    public enum Type {
        /** Unordered map ({@link HashMap} backing). */
        HASH,
        /** Insertion-ordered map ({@link LinkedHashMap} backing). */
        LINKED,
        /** Key-sorted map ({@link TreeMap} with {@link AerospikeComparator}). */
        TREE
    }

    /**
     * Create an empty map of the given {@link Type} and initial capacity.
     *
     * @param type     map encoding kind
     * @param capacity initial capacity hint for the backing map
     */
    public static <K,V> AerospikeMap<K,V> of(Type type, int capacity) {
        return new AerospikeMap<>(type, capacity);
    }

    /**
     * Wrap an existing {@link HashMap} as {@link Type#HASH}.
     *
     * @param map backing map (stored by reference)
     */
    public static <K,V> AerospikeMap<K,V> of(HashMap<K,V> map) {
        return new AerospikeMap<>(map);
    }

    /**
     * Wraps an existing {@link LinkedHashMap} as {@link Type#LINKED}.
     *
     * @param map backing map (stored by reference)
     */
    public static <K,V> AerospikeMap<K,V> of(LinkedHashMap<K,V> map) {
        return new AerospikeMap<>(map);
    }

    /**
     * Wraps an existing {@link TreeMap} as {@link Type#TREE}. Prefer a {@link TreeMap} constructed
     * with {@link AerospikeComparator} so key order matches the server.
     *
     * @param map backing map (stored by reference)
     */
    public static <K,V> AerospikeMap<K,V> of(TreeMap<K,V> map) {
        return new AerospikeMap<>(map);
    }

    private Map<K,V> map;
    private Type type;
    private MapOrder order;
    private boolean persistIndex;

    private AerospikeMap(Type type, int capacity) {
        this.type = type;
        this.persistIndex = false;

        switch (type) {
        default:
        case HASH:
            this.map = new HashMap<>(capacity);
            this.order = MapOrder.UNORDERED;
            break;
        case LINKED:
            this.map = new LinkedHashMap<>(capacity);
            this.order = MapOrder.KEY_ORDERED;
            break;
        case TREE:
            this.map = new TreeMap<>(new AerospikeComparator());
            this.order = MapOrder.KEY_ORDERED;
            break;
        }
    }

    private AerospikeMap(HashMap<K,V> map) {
        this.map = map;
        this.type = Type.HASH;
        this.order = MapOrder.UNORDERED;
        this.persistIndex = false;
    }

    private AerospikeMap(LinkedHashMap<K,V> map) {
        this.map = map;
        this.type = Type.LINKED;
        this.order = MapOrder.KEY_ORDERED;
        this.persistIndex = false;
    }

    private AerospikeMap(TreeMap<K,V> map) {
        this.map = map;
        this.type = Type.TREE;
        this.order = MapOrder.KEY_ORDERED;
        this.persistIndex = false;
    }

    /**
     * Change {@link Type} and replace the backing map when needed. Moving to {@link Type#LINKED}
     * from {@link Type#HASH} copies entries sorted by {@link AerospikeComparator}; from
     * {@link Type#TREE} it preserves iteration order in a new {@link LinkedHashMap}. Moving to
     * {@link Type#TREE} rebuilds a {@link TreeMap} with {@link AerospikeComparator}.
     *
     * @param newType target encoding; no-op if equal to the current type
     * @return this instance for chaining
     */
    public AerospikeMap<K,V> withType(Type newType) {
        if (this.type == newType) {
            return this;
        }

        switch (newType) {
        case HASH:
            this.map = new HashMap<>(this.map);
            this.order = MapOrder.UNORDERED;
            break;

        case LINKED:
            if (this.type == Type.HASH) {
                LinkedHashMap<K,V> tmp = new LinkedHashMap<>();

                this.map.entrySet()
                  .stream()
                  .sorted(new AerospikeComparator())
                  .forEachOrdered(entry -> tmp.put(entry.getKey(), entry.getValue()));

                this.map = tmp;
                this.order = MapOrder.KEY_ORDERED;
            }
            else if (this.type == Type.TREE) {
                this.map = new LinkedHashMap<>(this.map);
                // LINKED uses the same order as TREE.
            }
            break;

        case TREE:
            TreeMap<K,V> tmp = new TreeMap<>(new AerospikeComparator());
            tmp.putAll(this.map);
            this.map = tmp;

            if (this.type == Type.HASH) {
                this.order = MapOrder.KEY_ORDERED;
            }
            break;
        }

        this.type = newType;
        return this;
    }

    /**
     * Set the Aerospike map order used when packing this value. {@link Type#HASH} maps must use
     * {@link MapOrder#UNORDERED}.
     *
     * @param order map order to set on wire encoding
     * @return this instance for chaining
     * @throws IllegalArgumentException if {@code order} is not {@link MapOrder#UNORDERED} on a
     *                                  {@link Type#HASH} map
     */
    public AerospikeMap<K,V> order(MapOrder order) {
        if (this.type == Type.HASH && order != MapOrder.UNORDERED) {
            throw new IllegalArgumentException("Hashmap order must be UNORDERED");
        }
        this.order = order;
        return this;
    }

    /**
     * Request that a map index be persisted when encoding non-{@link Type#HASH} maps. A persisted
     * index can improve lookup performance on the server at the cost of extra storage. Hash maps
     * ignore this flag when packed.
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
     * Return the Aerospike map type used when packing this value.
     */
    public Type getType() {
        return type;
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
        if (type == Type.LINKED && map.size() > 0) {
            LinkedHashMap<K,V> tmp = (LinkedHashMap<K,V>)map;
            K lastKey = tmp.lastEntry().getKey();

            if (Comparator.compare(key, lastKey) <= 0) {
                throw new IllegalArgumentException("Linked map must be inserted in AerospikeComparator order");
            }
        }
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
}
