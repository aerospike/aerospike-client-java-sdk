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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class AerospikeMap<K,V> implements Map<K,V>{
	public enum Type {
		UNORDERED,
		LINKED,
		ORDERED
	}

	private Map<K,V> map;
	private Type type;
	private boolean persistIndex;

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

	public AerospikeMap(HashMap<K,V> map) {
		this.map = map;
		this.type = Type.UNORDERED;
		this.persistIndex = false;
	}

	public AerospikeMap(LinkedHashMap<K,V> map) {
		this.map = map;
		this.type = Type.LINKED;
		this.persistIndex = false;
	}

	public AerospikeMap(TreeMap<K,V> map) {
		this.type = Type.ORDERED;
		this.map = map;
		this.persistIndex = false;
	}

	public Map<K,V> getMap() {
		return map;
	}

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

	public Type getType() {
		return type;
	}

	/**
	 * Set whether to create an index on list creation. A list index improves lookup performance,
	 * but requires more storage. A list index can be created for a top-level ordered list only.
	 * Nested and unordered list indexes are not supported.
	 *
	 * @param persistIndex	if true, persist list index.
	 */
	public void setPersistIndex(boolean persistIndex) {
		this.persistIndex = persistIndex;
	}

	public boolean isPersistIndex() {
		return persistIndex;
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		return map.put(key, value);
	}

	@Override
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return map.entrySet();
	}
}
