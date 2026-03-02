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

import com.aerospike.client.fluent.cdt.MapOrder;

/**
 * Options for map CDT write operations (insert, update, upsert, add).
 *
 * <p>When no options are specified (or when {@code mapOrder} is not set), write operations that
 * create a new map will default to {@code MapOrder.KEY_ORDERED}.</p>
 *
 * <p>Used with lambda configuration pattern:</p>
 * <pre>{@code
 * session.update(key)
 *     .bin("map").onMapKey("k1").insert("v1", opt -> opt
 *         .mapOrder(MapOrder.KEY_VALUE_ORDERED)
 *         .persistIndex()
 *         .allowFailures())
 *     .execute();
 * }</pre>
 *
 * @param <T> self-referencing type for fluent chaining in subclasses
 */
public class MapWriteOptions<T extends MapWriteOptions<T>> {
    private MapOrder mapOrder;
    private boolean persistIndex;
    private boolean allowFailures;

    @SuppressWarnings("unchecked")
    protected T self() { return (T) this; }

    /**
     * Set the map ordering to use if the map does not already exist. Defaults to {@code KEY_ORDERED}.
     *
     * @param order the map ordering
     * @return this options object for method chaining
     */
    public T mapOrder(MapOrder order) { this.mapOrder = order; return self(); }

    /**
     * Persist the map index for faster lookups. Only supported for top-level ordered maps.
     *
     * @return this options object for method chaining
     */
    public T persistIndex() { this.persistIndex = true; return self(); }

    /**
     * Silently ignore failures instead of throwing exceptions (maps to {@code NO_FAIL}).
     *
     * @return this options object for method chaining
     */
    public T allowFailures() { this.allowFailures = true; return self(); }

    public MapOrder getMapOrder() { return mapOrder; }
    public boolean isPersistIndex() { return persistIndex; }
    public boolean isAllowFailures() { return allowFailures; }
}
