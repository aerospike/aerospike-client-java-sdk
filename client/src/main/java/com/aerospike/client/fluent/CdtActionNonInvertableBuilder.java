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
 * This interface defines the operations available at the end of a CDT path, other than
 * the set(). There are two fundamental operations, get() and remove() but there are 
 * different varieties of these based on the return type (eg count() and getValue() are
 * both get() operations.
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
    public T getValues();
    public T getKeys();
    public T count();
    public T getIndexes();
    public T getReverseIndexes();
    public T getRanks();
    public T getReverseRanks();
    public T getKeysAndValues();
    public T remove();

    /**
     * Check if the selected element(s) exist. Returns true if count > 0.
     */
    public T exists();

    /** @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering. */
    // TODO: Replace with AerospikeMap
    @Deprecated
    public T getAsMap();

    /** @deprecated Will be replaced by AerospikeMap which intrinsically supports ordering. */
    // TODO: Replace with AerospikeMap
    @Deprecated
    public T getAsOrderedMap();
}
