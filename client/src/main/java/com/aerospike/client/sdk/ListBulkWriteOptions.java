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

/**
 * Options for bulk (multi-item) list write operations (listAppendItems, listAddItems, listInsertItems).
 *
 * <p>Extends {@link ListWriteOptions} with {@code allowPartial()} which permits other valid list items
 * to be committed even when some are denied due to write flag constraints.</p>
 *
 * <pre>{@code
 * session.update(key)
 *     .bin("myList").listAppendItems(items, opt -> opt
 *         .addUnique()
 *         .allowFailures()
 *         .allowPartial())
 *     .execute();
 * }</pre>
 */
public class ListBulkWriteOptions extends ListWriteOptions<ListBulkWriteOptions> {
    private boolean allowPartial;

    /**
     * Allow other valid list items to be committed even if some are denied
     * due to write flag constraints (maps to {@code PARTIAL}). Only meaningful
     * for bulk/multi-item operations.
     *
     * @return this options object for method chaining
     */
    public ListBulkWriteOptions allowPartial() { this.allowPartial = true; return this; }

    /**
     * Whether {@link #allowPartial()} was set, i.e. bulk writes may commit valid items when others are denied.
     *
     * @return {@code true} if partial success is allowed for bulk list writes
     */
    public boolean isAllowPartial() { return allowPartial; }
}
