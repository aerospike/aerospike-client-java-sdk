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
public interface CdtSetterNonInvertableBuilder extends CdtContextNonInvertableBuilder {
    public OperationBuilder setTo(long value);
    public OperationBuilder setTo(String value);
    public OperationBuilder setTo(byte[] value);
    public OperationBuilder setTo(boolean value);
    public OperationBuilder setTo(double value);
    public OperationBuilder setTo(List<?> value);
    public OperationBuilder setTo(Map<?,?> value);
    public <T> OperationBuilder setTo(T value, RecordMapper<T> mapper);

    public OperationBuilder insert(long value);
    public OperationBuilder insert(String value);
    public OperationBuilder insert(byte[] value);
    public OperationBuilder insert(boolean value);
    public OperationBuilder insert(double value);
    public OperationBuilder insert(List<?> value);
    public OperationBuilder insert(Map<?,?> value);
    public <T> OperationBuilder insert(T value, RecordMapper<T> mapper);

    public OperationBuilder insert(long value, boolean allowFailures);
    public OperationBuilder insert(String value, boolean allowFailures);
    public OperationBuilder insert(byte[] value, boolean allowFailures);
    public OperationBuilder insert(boolean value, boolean allowFailures);
    public OperationBuilder insert(double value, boolean allowFailures);
    public OperationBuilder insert(List<?> value, boolean allowFailures);
    public OperationBuilder insert(Map<?,?> value, boolean allowFailures);
    public <T> OperationBuilder insert(T value, RecordMapper<T> mapper, boolean allowFailures);

    public OperationBuilder update(long value);
    public OperationBuilder update(String value);
    public OperationBuilder update(byte[] value);
    public OperationBuilder update(boolean value);
    public OperationBuilder update(double value);
    public OperationBuilder update(List<?> value);
    public OperationBuilder update(Map<?,?> value);
    public <T> OperationBuilder update(T value, RecordMapper<T> mapper);

    public OperationBuilder update(long value, boolean allowFailures);
    public OperationBuilder update(String value, boolean allowFailures);
    public OperationBuilder update(byte[] value, boolean allowFailures);
    public OperationBuilder update(boolean value, boolean allowFailures);
    public OperationBuilder update(double value, boolean allowFailures);
    public OperationBuilder update(List<?> value, boolean allowFailures);
    public OperationBuilder update(Map<?,?> value, boolean allowFailures);
    public <T> OperationBuilder update(T value, RecordMapper<T> mapper, boolean allowFailures);

    public OperationBuilder add(long value);
    public OperationBuilder add(double value);
    public OperationBuilder add(long value, boolean allowFailures);
    public OperationBuilder add(double value, boolean allowFailures);
}
