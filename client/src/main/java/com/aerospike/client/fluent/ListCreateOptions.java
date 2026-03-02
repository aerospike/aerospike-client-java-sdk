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

/**
 * Options for {@code listCreate()} operations.
 *
 * <p>Defaults: pad = {@code true}, persistIndex = {@code false}.</p>
 *
 * <pre>{@code
 * // Just the order (common case -- uses defaults)
 * session.update(key)
 *     .bin("scores").listCreate(ListOrder.ORDERED)
 *     .execute();
 *
 * // Disable padding
 * session.update(key)
 *     .bin("slots").listCreate(ListOrder.ORDERED, opt -> opt.noPad())
 *     .execute();
 *
 * // Persist the index for faster lookups
 * session.update(key)
 *     .bin("logs").listCreate(ListOrder.ORDERED, opt -> opt.persistIndex())
 *     .execute();
 * }</pre>
 */
public class ListCreateOptions {
    private boolean pad = true;
    private boolean persistIndex = false;

    /**
     * Disable padding. By default the list is padded with nil values when
     * writing to an out-of-bounds index. Call this to fail instead.
     *
     * @return this options object for method chaining
     */
    public ListCreateOptions noPad() { this.pad = false; return this; }

    /**
     * Persist the list index for faster lookups. Only meaningful for
     * top-level ordered lists.
     *
     * @return this options object for method chaining
     */
    public ListCreateOptions persistIndex() { this.persistIndex = true; return this; }

    public boolean isPad() { return pad; }
    public boolean isPersistIndex() { return persistIndex; }
}
