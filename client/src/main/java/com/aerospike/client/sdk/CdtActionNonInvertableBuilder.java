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
 * This interface defines the operations available at the end of a CDT path, other than
 * the set(). There are two fundamental operations, get() and remove() but there are
 * different varieties of these based on the return type (for example {@code count()} and
 * {@code getValues()} are both read operations).
 * <p>
 * These methods correspond to MapReturnType values:
 * <ul>
 *   <li>VALUE → getValues()</li>
 *   <li>KEY → getKeys()</li>
 *   <li>COUNT → count()</li>
 *   <li>INDEX → getIndex()</li>
 *   <li>REVERSE_INDEX → getReverseIndex()</li>
 *   <li>RANK → getRank()</li>
 *   <li>REVERSE_RANK → getReverseRank()</li>
 *   <li>KEY_VALUE → getKeyAndValue()</li>
 *   <li>NONE → remove()</li>
 * </ul>
 */
public interface CdtActionNonInvertableBuilder<T extends AbstractOperationBuilder<T>> {

    /**
     * Read the values of the selected map or list elements (CDT return type {@code VALUE}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getValues();

    /**
     * Read the keys of the selected map entries (CDT return type {@code KEY}). Not used for list bins.
     *
     * @return the parent operation builder for continued chaining
     */
    public T getKeys();

    /**
     * Read how many elements match the current selection (CDT return type {@code COUNT}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T count();

    /**
     * Read the key or list index positions of the selected elements (CDT return type {@code INDEX}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getIndexes();

    /**
     * Read positions in reverse index order (CDT return type {@code REVERSE_INDEX}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getReverseIndexes();

    /**
     * Read value ranks for the selected elements (CDT return type {@code RANK}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getRanks();

    /**
     * Read ranks in reverse value order (CDT return type {@code REVERSE_RANK}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getReverseRanks();

    /**
     * Read key–value pairs for the selected map entries into an {@link AerospikeMap} backed by a
     * {@link java.util.LinkedHashMap} (CDT return type {@code KEY_VALUE}).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getKeysAndValues();

    /**
     * Remove the selected elements without returning their data (CDT return type {@code NONE}).
     *
     * @return the parent operation builder for continued chaining
     * @see #removeAnd() to remove and also return information about removed elements
     */
    public T remove();

    /**
     * Begin a remove operation that also returns data about the removed elements.
     * Chain a return-type method to specify what the server should return:
     * <pre>{@code
     * .onMapValueRange(1, 4).removeAnd().getValues()   // remove and return VALUES
     * .onMapValueRange(1, 4).removeAnd().count()        // remove and return COUNT
     * .onMapValueRange(1, 4).removeAnd().getKeys()      // remove and return KEYS (maps only)
     * }</pre>
     *
     * @return a {@link RemoveResultBuilder} for specifying the return type
     * @see #remove() for fire-and-forget removal with no return data
     */
    public RemoveResultBuilder<T> removeAnd();

    /**
     * Check whether the selected element(s) exist. The server evaluates existence; the client exposes the
     * result as a boolean when the record is read.
     *
     * @return the parent operation builder for continued chaining
     */
    public T exists();

    /**
     * Read the selected map data as an {@link AerospikeMap} backed by a {@link java.util.HashMap} (unordered).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getAsMap();

    /**
     * Read the selected map data as an {@link AerospikeMap} backed by a {@link java.util.LinkedHashMap}
     * (insertion-ordered).
     *
     * @return the parent operation builder for continued chaining
     */
    public T getAsOrderedMap();
}
