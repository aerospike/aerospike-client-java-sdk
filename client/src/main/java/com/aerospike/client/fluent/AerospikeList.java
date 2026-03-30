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

import java.util.ArrayList;
import java.util.Collections;

import com.aerospike.client.fluent.cdt.ListOrder;

/**
 * {@link ArrayList} that records whether elements are stored in Aerospike ordered-list form.
 * Use {@link #sort()} or {@link #setOrder(ListOrder)} with {@link ListOrder#ORDERED} to match
 * server list sort ordering before writing bins.
 *
 * @param <T> element type
 */
public class AerospikeList<T> extends ArrayList<T> {
	private static final long serialVersionUID = 1L;

	private ListOrder order;
	private boolean persistIndex;

	/**
	 * Creates an empty unordered list with the given capacity.
	 * Equivalent to {@code new AerospikeList(capacity, ListOrder.UNORDERED)}.
	 *
	 * @param capacity initial capacity of the backing array
	 */
	public AerospikeList(int capacity) {
		super(capacity);
		this.order = ListOrder.UNORDERED;
		this.persistIndex = false;
	}

	/**
	 * Creates an empty list with the given capacity and {@link ListOrder}.
	 *
	 * @param capacity initial capacity of the backing array
	 * @param order    unordered or ordered list flag ({@link ListOrder#UNORDERED} or {@link ListOrder#ORDERED})
	 */
	public AerospikeList(int capacity, ListOrder order) {
		super(capacity);
		this.order = order;
		this.persistIndex = false;
	}

	/**
	 * Sets {@link ListOrder} for Aerospike list encoding. Transitioning from
	 * {@link ListOrder#UNORDERED} to {@link ListOrder#ORDERED} sorts elements in place with
	 * {@link #sort()}. All other changes update only the stored flag (no sort).
	 *
	 * @param order new list order
	 */
	public void setOrder(ListOrder order) {
		if (this.order == ListOrder.UNORDERED && order == ListOrder.ORDERED) {
			sort();
		}
		else {
			this.order = order;
		}
	}

	/**
	 * Returns the list order used when encoding this list for the server.
	 *
	 * @return current {@link ListOrder}
	 */
	public ListOrder getOrder() {
		return order;
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

	/**
	 * Returns if the server should create an index when the list is ordered.
	 */
	public boolean isPersistIndex() {
		return persistIndex;
	}

	/**
	 * Sorts this list in place using {@link AerospikeComparator} (Aerospike value ordering, aligned
	 * with server list sort), then sets {@link ListOrder#ORDERED}.
	 */
	public void sort() {
		Collections.sort(this, new AerospikeComparator());
		this.order = ListOrder.ORDERED;
	}
}
