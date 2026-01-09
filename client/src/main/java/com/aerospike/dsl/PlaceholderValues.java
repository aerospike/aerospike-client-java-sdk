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
package com.aerospike.dsl;

/**
 * This class stores values to be matched with placeholders by indexes
 */
public class PlaceholderValues {

    private final Object[] values;

    private PlaceholderValues(Object... values) {
        this.values = values != null ? values : new Object[0];
    }

    /**
     * Create a new {@link PlaceholderValues} object with
     */
    public static PlaceholderValues of(Object... values) {
        return new PlaceholderValues(values);
    }

    /**
     * Get value of the placeholder with the particular index
     *
     * @param index Index of the placeholder
     * @return Value of the placeholder with the given index
     * @throws IllegalArgumentException if placeholder index is out of bounds (fewer values than placeholders)
     */
    public Object getValue(int index) {
        if (index < 0 || index >= values.length) {
            throw new IllegalArgumentException("Missing value for placeholder ?" + index);
        }
        return values[index];
    }

    /**
     * Get overall amount of the given values
     */
    public int size() {
        return values.length;
    }
}
