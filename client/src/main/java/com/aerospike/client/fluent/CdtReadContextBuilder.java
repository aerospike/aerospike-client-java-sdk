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

import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.cdt.ListOrder;

/**
 * Read-only CDT context navigation interface for query operations.
 * 
 * <p>This interface provides navigation methods for traversing CDT (Collection Data Type)
 * structures. Unlike {@link CdtContextNonInvertableBuilder}, this interface returns
 * read-only builders and does not include any write operations like {@code setTo()},
 * {@code mapClear()}, or {@code listAppend()}, making it safe for use in query contexts.</p>
 *
 * <p>Key differences from read/write version:</p>
 * <ul>
 *   <li>{@code onMapKey()} returns {@link CdtReadContextBuilder} instead of a setter builder</li>
 *   <li>No {@code mapClear()}, {@code listAppend()}, {@code listAdd()} methods</li>
 *   <li>Only {@code mapSize()} is available as a terminal read operation</li>
 * </ul>
 *
 * <p>Example:</p>
 * <pre>{@code
 * // Navigate and read nested map values
 * session.query(key)
 *     .bin("settings").onMapKey("preferences").onMapKey("theme").getValues()
 *     .execute();
 * }</pre>
 *
 * @param <T> the type of the parent builder to return for method chaining
 * @see CdtReadContextInvertableBuilder for invertable context operations
 * @see CdtContextNonInvertableBuilder for the read/write version
 */
public interface CdtReadContextBuilder<T> extends CdtReadActionBuilder<T> {
    // Map index
    CdtReadContextBuilder<T> onMapIndex(int index);
    
    // Map index range operations
    CdtReadActionInvertableBuilder<T> onMapIndexRange(int index, int count);
    CdtReadActionInvertableBuilder<T> onMapIndexRange(int index);

    // Map key operations - returns context builder (not setter) for read-only
    CdtReadContextBuilder<T> onMapKey(long key);
    CdtReadContextBuilder<T> onMapKey(String key);
    CdtReadContextBuilder<T> onMapKey(byte[] key);

    // Map key range operations
    CdtReadActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl);
    // SpecialValue combinations for onMapKeyRange
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl);

    // Map key relative index range
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count);
    CdtReadActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count);

    // Map rank
    CdtReadContextBuilder<T> onMapRank(int index);

    // Map rank range operations
    CdtReadActionInvertableBuilder<T> onMapRankRange(int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapRankRange(int rank);

    // Map value operations
    CdtReadContextInvertableBuilder<T> onMapValue(long value);
    CdtReadContextInvertableBuilder<T> onMapValue(String value);
    CdtReadContextInvertableBuilder<T> onMapValue(byte[] value);
    CdtReadContextInvertableBuilder<T> onMapValue(double value);
    CdtReadContextInvertableBuilder<T> onMapValue(boolean value);
    CdtReadContextInvertableBuilder<T> onMapValue(List<?> value);
    CdtReadContextInvertableBuilder<T> onMapValue(Map<?,?> value);
    CdtReadContextInvertableBuilder<T> onMapValue(SpecialValue value);
    
    // Map value range
    CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl);
    // SpecialValue combinations for onMapValueRange
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl);
    CdtReadActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl);

    // Map value relative rank range
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count);
    CdtReadActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count);

    // Map key and value list operations
    CdtReadContextInvertableBuilder<T> onMapKeyList(List<?> keys);
    CdtReadContextInvertableBuilder<T> onMapValueList(List<?> values);

    // List operations
    CdtReadContextBuilder<T> onListIndex(int index);
    CdtReadContextBuilder<T> onListIndex(int index, ListOrder order, boolean pad);
    CdtReadContextBuilder<T> onListRank(int index);
    CdtReadContextInvertableBuilder<T> onListValue(long value);
    CdtReadContextInvertableBuilder<T> onListValue(String value);
    CdtReadContextInvertableBuilder<T> onListValue(byte[] value);
    CdtReadContextInvertableBuilder<T> onListValue(SpecialValue value);

    // Read-only terminal operations
    /**
     * Get the size of the map at the current CDT path.
     * @return the parent builder for method chaining
     */
    T mapSize();
    
    // Note: mapClear(), listAppend(), listAdd() are NOT available - these are write operations
}
