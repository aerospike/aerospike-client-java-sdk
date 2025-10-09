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
public interface CdtContextInvertableBuilder extends CdtActionInvertableBuilder {
    public CdtContextNonInvertableBuilder onMapIndex(int index);
    public CdtSetterNonInvertableBuilder onMapKey(long key);
    public CdtSetterNonInvertableBuilder onMapKey(String key);
    public CdtSetterNonInvertableBuilder onMapKey(byte[] key);
    public CdtSetterNonInvertableBuilder onMapKey(long key, MapOrder createType);
    public CdtSetterNonInvertableBuilder onMapKey(String key, MapOrder createType);
    public CdtSetterNonInvertableBuilder onMapKey(byte[] key, MapOrder createType);
    public CdtContextNonInvertableBuilder onMapRank(int index);
    public CdtContextInvertableBuilder onMapValue(long value);
    public CdtContextInvertableBuilder onMapValue(String value);
    public CdtContextInvertableBuilder onMapValue(byte[] value);
    public CdtActionInvertableBuilder onMapValueRange(long startIncl, long endExcl);

    public CdtContextNonInvertableBuilder onListIndex(int index);
    public CdtContextNonInvertableBuilder onListIndex(int index, ListOrder order, boolean pad);
    public CdtContextNonInvertableBuilder onListRank(int index);
    public CdtContextInvertableBuilder onListValue(long value);
    public CdtContextInvertableBuilder onListValue(String value);
    public CdtContextInvertableBuilder onListValue(byte[] value);

    public OperationBuilder mapClear();
    public OperationBuilder mapSize();

    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(long value);
    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(String value);
    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(double value);
    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(boolean value);
    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(byte[] value);
    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(List<?> value);
    /** Append an item to the end of an unordered list */
    public OperationBuilder listAppend(Map<?,?> value);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(long value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(String value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(double value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(boolean value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(byte[] value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(List<?> value, boolean allowFailures);
    /** Append an item to the end of an unordered list with unique items, allowing for failures */
    public OperationBuilder listAppendUnique(Map<?,?> value, boolean allowFailures);

    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(long value);
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(String value);
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(double value);
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(boolean value);
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(byte[] value);
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(List<?> value);
    /** Add an item to the appropriate spot in an ordered list */
    public OperationBuilder listAdd(Map<?,?> value);

    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(long value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(String value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(double value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(boolean value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(byte[] value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(List<?> value, boolean allowFailures);
    /** Add an item to the appropriate spot in an ordered list. If the item is not unique
     * either an exception will be thrown or the error will be silently ignored, based on allowFailures */
    public OperationBuilder listAddUnique(Map<?,?> value, boolean allowFailures);

}