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

/**
 * {@link Map} implementation that records how entries should be encoded as an Aerospike map
 * (unordered, insertion-ordered / linked, or key-sorted). The backing collection matches the
 * {@link Type}: {@link HashMap}, {@link LinkedHashMap}, or {@link TreeMap} ordered by
 * {@link AerospikeComparator} for {@link Type#ORDERED}.
 * <p>
 * Use {@link #setType(Type)} before writes when you need to change encoding without rebuilding
 * the map manually. {@link Type#LINKED} and {@link Type#ORDERED} are packed as key-ordered maps
 * on the wire; {@link #setPersistIndex(boolean)} applies only to those modes.
 * <p>
 * Ordinary {@link Map} operations delegate to the backing map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class AerospikeMap<K,V> implements Map<K,V>{

	/**
	 * Aerospike map wire layout: unordered map, insertion-ordered (linked) map, or map sorted by
	 * Aerospike value ordering on keys.
	 */
	public enum Type {
		/** Unordered map ({@link HashMap} backing). */
		UNORDERED,
		/** Insertion-ordered map ({@link LinkedHashMap} backing). */
		LINKED,
		/** Key-sorted map ({@link TreeMap} with {@link AerospikeComparator}). */
		ORDERED
	}

	private Map<K,V> map;
	private Type type;
	private boolean persistIndex;

	/**
	 * Creates an empty map of the given {@link Type} and initial capacity.
	 *
	 * @param type     map encoding kind
	 * @param capacity initial capacity hint for the backing map
	 */
	public AerospikeMap(Type type, int capacity) {
		this.type = type;
		this.persistIndex = false;

		switch (type) {
		default:
		case UNORDERED:
			map = new HashMap<>(capacity);
			break;
		case LINKED:
			map = new LinkedHashMap<>(capacity);
			break;
		case ORDERED:
			map = new TreeMap<>(new AerospikeComparator());
			break;
		}
	}

	/**
	 * Wraps an existing {@link HashMap} as {@link Type#UNORDERED}.
	 *
	 * @param map backing map (stored by reference)
	 */
	public AerospikeMap(HashMap<K,V> map) {
		this.map = map;
		this.type = Type.UNORDERED;
		this.persistIndex = false;
	}

	/**
	 * Wraps an existing {@link LinkedHashMap} as {@link Type#LINKED}.
	 *
	 * @param map backing map (stored by reference)
	 */
	public AerospikeMap(LinkedHashMap<K,V> map) {
		this.map = map;
		this.type = Type.LINKED;
		this.persistIndex = false;
	}

	/**
	 * Wraps an existing {@link TreeMap} as {@link Type#ORDERED}. Prefer a {@link TreeMap} constructed
	 * with {@link AerospikeComparator} so key order matches the server.
	 *
	 * @param map backing map (stored by reference)
	 */
	public AerospikeMap(TreeMap<K,V> map) {
		this.map = map;
		this.type = Type.ORDERED;
		this.persistIndex = false;
	}

	/**
	 * Returns the backing {@link Map} instance.
	 *
	 * @return the delegate map
	 */
	public Map<K,V> getMap() {
		return map;
	}

	/**
	 * Changes {@link Type} and replaces the backing map when needed. Moving to {@link Type#LINKED}
	 * from {@link Type#UNORDERED} copies entries sorted by {@link AerospikeComparator}; from
	 * {@link Type#ORDERED} it preserves iteration order in a new {@link LinkedHashMap}. Moving to
	 * {@link Type#ORDERED} rebuilds a {@link TreeMap} with {@link AerospikeComparator}.
	 *
	 * @param newType target encoding; no-op if equal to the current type
	 */
	public void setType(Type newType) {
		if (this.type == newType) {
			return;
		}

		if (newType == Type.LINKED) {
			if (this.type == Type.UNORDERED) {
				LinkedHashMap<K,V> tmp = new LinkedHashMap<>();

				map.entrySet()
				  .stream()
				  .sorted(new AerospikeComparator())
				  .forEachOrdered(entry -> tmp.put(entry.getKey(), entry.getValue()));

				map = tmp;
			}
			else if (this.type == Type.ORDERED) {
				map = new LinkedHashMap<>(map);
			}
		}
		else if (newType == Type.ORDERED) {
			TreeMap<K,V> tmp = new TreeMap<>(new AerospikeComparator());
			tmp.putAll(map);
			map = tmp;
		}

		this.type = newType;
	}

	/**
	 * Returns the Aerospike map type used when packing this value.
	 *
	 * @return current {@link Type}
	 */
	public Type getType() {
		return type;
	}

	/**
	 * Whether to persist a map index when encoding non-{@link Type#UNORDERED} maps. A persisted
	 * index can improve lookup performance on the server at the cost of extra storage. Unordered
	 * maps ignore this flag when packed.
	 *
	 * @param persistIndex {@code true} to set the persisted-index attribute on wire encoding
	 */
	public void setPersistIndex(boolean persistIndex) {
		this.persistIndex = persistIndex;
	}

	/**
	 * @return whether persisted map index is requested for linked / ordered encoding
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
