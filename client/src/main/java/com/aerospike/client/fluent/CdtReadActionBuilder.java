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
 * Read-only CDT action interface for query operations.
 * 
 * <p>This interface provides terminal read operations for CDT (Collection Data Type) paths.
 * Unlike {@link CdtActionNonInvertableBuilder}, this interface does not include any write
 * operations like {@code remove()}, making it safe for use in query contexts.</p>
 *
 * <p>These methods correspond to MapReturnType/ListReturnType values:</p>
 * <ul>
 *   <li>VALUE → {@link #getValues()}</li>
 *   <li>KEY → {@link #getKeys()}</li>
 *   <li>COUNT → {@link #count()}</li>
 *   <li>INDEX → {@link #getIndexes()}</li>
 *   <li>REVERSE_INDEX → {@link #getReverseIndexes()}</li>
 *   <li>RANK → {@link #getRanks()}</li>
 *   <li>REVERSE_RANK → {@link #getReverseRanks()}</li>
 *   <li>KEY_VALUE → {@link #getKeysAndValues()}</li>
 * </ul>
 *
 * @param <T> the type of the parent builder to return for method chaining
 * @see CdtReadActionInvertableBuilder for invertable operations
 * @see CdtActionNonInvertableBuilder for the read/write version
 */
public interface CdtReadActionBuilder<T> {
    /**
     * Get the value(s) at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getValues();

    /**
     * Get the key(s) at the current CDT path (map operations only).
     * @return the parent builder for method chaining
     */
    T getKeys();

    /**
     * Get the count of elements at the current CDT path.
     * @return the parent builder for method chaining
     */
    T count();

    /**
     * Get the index(es) of elements at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getIndexes();

    /**
     * Get the reverse index(es) of elements at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getReverseIndexes();

    /**
     * Get the rank(s) of elements at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getRanks();

    /**
     * Get the reverse rank(s) of elements at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getReverseRanks();

    /**
     * Get the key-value pairs at the current CDT path (map operations only).
     * @return the parent builder for method chaining
     */
    T getKeysAndValues();
}
