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
 * Read-only CDT action interface with invertable operations.
 * 
 * <p>This interface extends {@link CdtReadActionBuilder} with "all others" operations
 * that use the INVERTED flag to return all elements except those matching the selector.
 * Unlike {@link CdtActionInvertableBuilder}, this interface does not include any write
 * operations like {@code removeAllOthers()}, making it safe for use in query contexts.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * // Get all values EXCEPT those at key "secret"
 * session.query(key)
 *     .bin("settings").onMapKey("secret").getAllOtherValues()
 *     .execute();
 * }</pre>
 *
 * @param <T> the type of the parent builder to return for method chaining
 * @see CdtReadActionBuilder for non-invertable operations
 * @see CdtActionInvertableBuilder for the read/write version
 */
public interface CdtReadActionInvertableBuilder<T> extends CdtReadActionBuilder<T> {
    /**
     * Get all values EXCEPT those at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getAllOtherValues();

    /**
     * Get all keys EXCEPT those at the current CDT path (map operations only).
     * @return the parent builder for method chaining
     */
    T getAllOtherKeys();

    /**
     * Count all elements EXCEPT those at the current CDT path.
     * @return the parent builder for method chaining
     */
    T countAllOthers();

    /**
     * Get the indexes of all elements EXCEPT those at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getAllOtherIndexes();

    /**
     * Get the reverse indexes of all elements EXCEPT those at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getAllOtherReverseIndexes();

    /**
     * Get the ranks of all elements EXCEPT those at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getAllOtherRanks();

    /**
     * Get the reverse ranks of all elements EXCEPT those at the current CDT path.
     * @return the parent builder for method chaining
     */
    T getAllOtherReverseRanks();

    /**
     * Get all key-value pairs EXCEPT those at the current CDT path (map operations only).
     * @return the parent builder for method chaining
     */
    T getAllOtherKeysAndValues();
}
