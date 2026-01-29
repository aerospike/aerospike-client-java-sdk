package com.aerospike.client.fluent;

import java.util.List;
import java.util.Map;

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
public interface CdtContextNonInvertableBuilder<T extends AbstractOperationBuilder<T>> extends CdtActionNonInvertableBuilder<T> {
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
    public CdtContextInvertableBuilder<T> onListValue(SpecialValue value);

    public T mapClear();
    public T mapSize();
    
    /** Append an item to the end of an unordered list */
    public T listAppend(long value);
    /** Append an item to the end of an unordered list */
    public T listAppend(String value);
    /** Append an item to the end of an unordered list */
    public T listAppend(double value);
    /** Append an item to the end of an unordered list */
    public T listAppend(boolean value);
    /** Append an item to the end of an unordered list */
    public T listAppend(byte[] value);
    /** Append an item to the end of an unordered list */
    public T listAppend(List<?> value);
    /** Append an item to the end of an unordered list */
    public T listAppend(Map<?,?> value);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(long value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(String value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(double value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(boolean value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(byte[] value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(List<?> value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public T listAppendUnique(Map<?,?> value, boolean allowFailures);
    
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(long value);
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(String value);
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(double value);
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(boolean value);
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(byte[] value);
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(List<?> value);
    /** Add an item to the appropriate spot in an ordered list */
    public T listAdd(Map<?,?> value);

    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(long value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(String value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(double value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(boolean value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(byte[] value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(List<?> value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique 
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public T listAddUnique(Map<?,?> value, boolean allowFailures);

}
