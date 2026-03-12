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
import java.util.function.Consumer;

import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.MapOrder;

/**
 * This interface handles operations at the end of contexts. Note that some of these methods
 * like onMapValueRangemust be at the end of a context and hence must be followed by an action
 * (CdtAction* return types), others (like onMapIndex) are context items which can be followed
 * either by other context paths or by an action (CdtContext* return types), and onMapKey which
 * can be followed a context path, an action (get or remove) or can be used to set the value, and
 * hence returns a CdtSetter* method.
 * <p/>
 * Note that some methods are invertable (ie can support the INVERTED flag) and others aren't.
 * For example, onMapIndex returns a single value, hence cannot support the INVERTED flag. 
 * onMapValue returns a list of values and hence can be inverted.
 * <p/>
 * Note that this is a paired interface with {@link CdtContextInvertableBuilder} and they have exactly
 * the same methods, differing only in the interface they extend.
 */
public interface CdtContextInvertableBuilder<T extends AbstractOperationBuilder<T>> extends CdtActionInvertableBuilder<T> {
    // Map index
    public CdtContextNonInvertableBuilder<T> onMapIndex(int index);
    
    // Map index range operations
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index, int count);
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index);

    // Map key operations
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key);
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key);
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key);
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key, MapOrder createType);
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key, MapOrder createType);
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key, MapOrder createType);

    // Map key range operations
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl);
    // SpecialValue combinations for onMapKeyRange
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl);

    // Map key relative rank range
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count);
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count);

    // Map rank
    public CdtContextNonInvertableBuilder<T> onMapRank(int index);

    // Map rank range operations
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank, int count);
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank);

    // Map value operations
    public CdtContextInvertableBuilder<T> onMapValue(long value);
    public CdtContextInvertableBuilder<T> onMapValue(String value);
    public CdtContextInvertableBuilder<T> onMapValue(byte[] value);
    public CdtContextInvertableBuilder<T> onMapValue(double value);
    public CdtContextInvertableBuilder<T> onMapValue(boolean value);
    public CdtContextInvertableBuilder<T> onMapValue(List<?> value);
    public CdtContextInvertableBuilder<T> onMapValue(Map<?,?> value);
    public CdtContextInvertableBuilder<T> onMapValue(SpecialValue value);
    
    // Map value range
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl);
    // SpecialValue combinations for onMapValueRange
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl);

    // Map value relative rank range
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count);
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count);

    // Map key and value list operations
    public CdtContextInvertableBuilder<T> onMapKeyList(List<?> keys);
    public CdtContextInvertableBuilder<T> onMapValueList(List<?> values);

    public CdtContextNonInvertableBuilder<T> onListIndex(int index);
    public CdtContextNonInvertableBuilder<T> onListIndex(int index, ListOrder order, boolean pad);
    public CdtContextNonInvertableBuilder<T> onListRank(int index);
    public CdtContextInvertableBuilder<T> onListValue(long value);
    public CdtContextInvertableBuilder<T> onListValue(String value);
    public CdtContextInvertableBuilder<T> onListValue(byte[] value);

    // List index range
    public CdtActionInvertableBuilder<T> onListIndexRange(int index);
    public CdtActionInvertableBuilder<T> onListIndexRange(int index, int count);

    // List rank range
    public CdtActionInvertableBuilder<T> onListRankRange(int rank);
    public CdtActionInvertableBuilder<T> onListRankRange(int rank, int count);

    // List value range
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, long endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, String endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, byte[] endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, double endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, SpecialValue endExcl);
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, SpecialValue endExcl);

    // List value list
    public CdtContextInvertableBuilder<T> onListValueList(java.util.List<?> values);

    // List value relative rank range
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank, int count);
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank, int count);

    public T mapClear();
    public T mapSize();

    // listAppend -- unordered list
    public T listAppend(long value);
    public T listAppend(String value);
    public T listAppend(double value);
    public T listAppend(boolean value);
    public T listAppend(byte[] value);
    public T listAppend(List<?> value);
    public T listAppend(Map<?,?> value);
    public T listAppend(long value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(String value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(double value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(boolean value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(byte[] value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(List<?> value, Consumer<ListEntryWriteOptions> options);
    public T listAppend(Map<?,?> value, Consumer<ListEntryWriteOptions> options);

    // listAdd -- ordered list
    public T listAdd(long value);
    public T listAdd(String value);
    public T listAdd(double value);
    public T listAdd(boolean value);
    public T listAdd(byte[] value);
    public T listAdd(List<?> value);
    public T listAdd(Map<?,?> value);
    public T listAdd(long value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(String value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(double value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(boolean value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(byte[] value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(List<?> value, Consumer<ListEntryWriteOptions> options);
    public T listAdd(Map<?,?> value, Consumer<ListEntryWriteOptions> options);

}
