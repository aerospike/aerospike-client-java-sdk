package com.aerospike.client.fluent;

import java.util.List;
import java.util.Map;

/**
 * This interface provides terminal methods to set values at the end of a CDT chain. The
 * only valid way of setting a value is to use onMapKey().set(...) style methods. There
 * are different methods on this interface:
 * <ul>
 * <li>{@code setTo} methods which set the value and cannot fail</li>
 * <li>{@code insert} methods which will insert a value only if the key does not exist. These will
 * throw an exception by default if the key exists, but there is an overload which has a parameter to allow the
 * methods to fail silently</li>
 * <li>{@code update} methods which will insert a value only if the key already exists. These will
 * throw an exception by default if the key doesn't exit, but there is an overload which has a parameter to allow the
 * methods to fail silently</li>
 * </ul>
 * <p/>
 * Note that this is a paired interface with {@link CdtSetterInvertableBuilder} and they have exactly
 * the same methods, differing only in the interface they extend.
 */
public interface CdtSetterNonInvertableBuilder<T extends AbstractOperationBuilder<T>> extends CdtContextNonInvertableBuilder<T> {
    public T setTo(long value);
    public T setTo(String value);
    public T setTo(byte[] value);
    public T setTo(boolean value);
    public T setTo(double value);
    public T setTo(List<?> value);
    public T setTo(Map<?,?> value);
    public <U> T setTo(U value, RecordMapper<U> mapper);

    public T insert(long value);
    public T insert(String value);
    public T insert(byte[] value);
    public T insert(boolean value);
    public T insert(double value);
    public T insert(List<?> value);
    public T insert(Map<?,?> value);
    public <U> T insert(U value, RecordMapper<U> mapper);

    public T insert(long value, boolean allowFailures);
    public T insert(String value, boolean allowFailures);
    public T insert(byte[] value, boolean allowFailures);
    public T insert(boolean value, boolean allowFailures);
    public T insert(double value, boolean allowFailures);
    public T insert(List<?> value, boolean allowFailures);
    public T insert(Map<?,?> value, boolean allowFailures);
    public <U> T insert(U value, RecordMapper<U> mapper, boolean allowFailures);

    public T update(long value);
    public T update(String value);
    public T update(byte[] value);
    public T update(boolean value);
    public T update(double value);
    public T update(List<?> value);
    public T update(Map<?,?> value);
    public <U> T update(U value, RecordMapper<U> mapper);

    public T update(long value, boolean allowFailures);
    public T update(String value, boolean allowFailures);
    public T update(byte[] value, boolean allowFailures);
    public T update(boolean value, boolean allowFailures);
    public T update(double value, boolean allowFailures);
    public T update(List<?> value, boolean allowFailures);
    public T update(Map<?,?> value, boolean allowFailures);
    public <U> T update(U value, RecordMapper<U> mapper, boolean allowFailures);

    public T add(long value);
    public T add(double value);
    public T add(long value, boolean allowFailures);
    public T add(double value, boolean allowFailures);
}
