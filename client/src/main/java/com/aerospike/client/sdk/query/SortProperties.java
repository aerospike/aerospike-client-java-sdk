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
package com.aerospike.client.sdk.query;

/**
 * Represents sorting properties for a field including sort direction and case sensitivity.
 * 
 * <p>This record defines how a field should be sorted. It includes:
 * <ul>
 *   <li><b>name</b> - the field name to sort by</li>
 *   <li><b>sortDir</b> - the sort direction (ascending or descending)</li>
 *   <li><b>caseInsensitive</b> - whether string comparison should be case-insensitive</li>
 * </ul>
 * 
 * <h2>Usage Examples</h2>
 * 
 * <h3>Using Constructor (full control)</h3>
 * <pre>{@code
 * SortProperties sort = new SortProperties("age", SortDir.SORT_DESC, true);
 * }</pre>
 * 
 * <h3>Using Static Factory Methods (convenient)</h3>
 * <pre>{@code
 * // Ascending, case-sensitive (default)
 * SortProperties byName = SortProperties.ascending("name");
 * 
 * // Descending, case-sensitive
 * SortProperties byAge = SortProperties.descending("age");
 * 
 * // Ascending, case-insensitive
 * SortProperties byCityIgnoreCase = SortProperties.ascendingIgnoreCase("city");
 * 
 * // Descending, case-insensitive
 * SortProperties byCountryIgnoreCase = SortProperties.descendingIgnoreCase("country");
 * 
 * // Multi-column sort example
 * navigatable.sortBy(List.of(
 *     SortProperties.ascending("lastName"),
 *     SortProperties.ascending("firstName"),
 *     SortProperties.descending("age")
 * ));
 * }</pre>
 * 
 * @param name the field name to sort by
 * @param sortDir the sort direction
 * @param caseInsensitive true for case-insensitive string comparison, false for case-sensitive
 */
public record SortProperties(String name, SortDir sortDir, boolean caseInsensitive) {

    /**
     * Creates a compact canonical constructor that validates inputs.
     */
    public SortProperties {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (sortDir == null) {
            throw new IllegalArgumentException("Sort direction cannot be null");
        }
    }

    /**
     * Creates a SortProperties for ascending sort with case-sensitive comparison.
     * 
     * @param name the field name to sort by
     * @return a SortProperties configured for ascending, case-sensitive sort
     */
    public static SortProperties ascending(String name) {
        return new SortProperties(name, SortDir.SORT_ASC, false);
    }

    /**
     * Creates a SortProperties for descending sort with case-sensitive comparison.
     * 
     * @param name the field name to sort by
     * @return a SortProperties configured for descending, case-sensitive sort
     */
    public static SortProperties descending(String name) {
        return new SortProperties(name, SortDir.SORT_DESC, false);
    }

    /**
     * Creates a SortProperties for ascending sort with case-insensitive comparison.
     * 
     * <p>This is useful for sorting string fields where you want to ignore case differences.</p>
     * 
     * @param name the field name to sort by
     * @return a SortProperties configured for ascending, case-insensitive sort
     */
    public static SortProperties ascendingIgnoreCase(String name) {
        return new SortProperties(name, SortDir.SORT_ASC, true);
    }

    /**
     * Creates a SortProperties for descending sort with case-insensitive comparison.
     * 
     * <p>This is useful for sorting string fields where you want to ignore case differences.</p>
     * 
     * @param name the field name to sort by
     * @return a SortProperties configured for descending, case-insensitive sort
     */
    public static SortProperties descendingIgnoreCase(String name) {
        return new SortProperties(name, SortDir.SORT_DESC, true);
    }

}
