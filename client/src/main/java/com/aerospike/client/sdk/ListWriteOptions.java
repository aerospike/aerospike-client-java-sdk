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
 * Options for list CDT write operations (listAppend, listAdd, listInsert, etc.).
 *
 * <p>Used with lambda configuration pattern:</p>
 * <pre>{@code
 * session.update(key)
 *     .bin("myList").listAppend("value", opt -> opt
 *         .addUnique()
 *         .allowFailures())
 *     .execute();
 * }</pre>
 *
 * @param <T> self-referencing type for chaining in subclasses
 */
public class ListWriteOptions<T extends ListWriteOptions<T>> {
    private boolean addUnique;
    private boolean insertBounded;
    private boolean allowFailures;

    @SuppressWarnings("unchecked")
    protected T self() { return (T) this; }

    /**
     * Reject duplicate values. Only valid for lists with unique constraints.
     * Maps to {@code ADD_UNIQUE}.
     *
     * @return this options object for method chaining
     */
    public T addUnique() { this.addUnique = true; return self(); }

    /**
     * Fail if the index is out of bounds instead of extending the list.
     * Maps to {@code INSERT_BOUNDED}.
     *
     * @return this options object for method chaining
     */
    public T insertBounded() { this.insertBounded = true; return self(); }

    /**
     * Silently ignore failures instead of throwing exceptions.
     * Maps to {@code NO_FAIL}.
     *
     * @return this options object for method chaining
     */
    public T allowFailures() { this.allowFailures = true; return self(); }

    public boolean isAddUnique() { return addUnique; }
    public boolean isInsertBounded() { return insertBounded; }
    public boolean isAllowFailures() { return allowFailures; }
}
