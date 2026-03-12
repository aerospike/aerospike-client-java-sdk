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

/**
 * This interface provides terminal methods to set values at the end of a CDT chain. The
 * only valid way of setting a value is to use onMapKey().set(...) style methods. There
 * are different methods on this interface:
 * <ul>
 * <li>{@code setTo} methods which set the value and cannot fail</li>
 * <li>{@code insert} methods which will insert a value only if the key does not exist. These will
 * throw an exception by default if the key exists, but there is an overload accepting
 * {@link MapEntryWriteOptions} to control this behavior</li>
 * <li>{@code update} methods which will update a value only if the key already exists. These will
 * throw an exception by default if the key doesn't exist, but there is an overload accepting
 * {@link MapEntryWriteOptions} to control this behavior</li>
 * <li>{@code upsert} methods which will create or update a map entry unconditionally</li>
 * <li>{@code add} methods which atomically increment a numeric map value</li>
 * </ul>
 * 
 * <p>When no {@link MapEntryWriteOptions} are specified, write operations that may create a new map
 * will default to {@code MapOrder.KEY_ORDERED}.</p>
 * 
 * Note that this is a paired interface with {@link CdtSetterNonInvertableBuilder} and they have exactly
 * the same methods, differing only in the interface they extend.
 */
public interface CdtSetterInvertableBuilder<T extends AbstractOperationBuilder<T>> extends CdtContextInvertableBuilder<T> {
    public T setTo(long value);
    public T setTo(String value);
    public T setTo(byte[] value);
    public T setTo(boolean value);
    public T setTo(double value);
    public T setTo(List<?> value);
    public T setTo(Map<?,?> value);

    public T insert(long value);
    public T insert(String value);
    public T insert(byte[] value);
    public T insert(boolean value);
    public T insert(double value);
    public T insert(List<?> value);
    public T insert(Map<?,?> value);

    public T insert(long value, Consumer<MapEntryWriteOptions> options);
    public T insert(String value, Consumer<MapEntryWriteOptions> options);
    public T insert(byte[] value, Consumer<MapEntryWriteOptions> options);
    public T insert(boolean value, Consumer<MapEntryWriteOptions> options);
    public T insert(double value, Consumer<MapEntryWriteOptions> options);
    public T insert(List<?> value, Consumer<MapEntryWriteOptions> options);
    public T insert(Map<?,?> value, Consumer<MapEntryWriteOptions> options);

    public T update(long value);
    public T update(String value);
    public T update(byte[] value);
    public T update(boolean value);
    public T update(double value);
    public T update(List<?> value);
    public T update(Map<?,?> value);

    public T update(long value, Consumer<MapEntryWriteOptions> options);
    public T update(String value, Consumer<MapEntryWriteOptions> options);
    public T update(byte[] value, Consumer<MapEntryWriteOptions> options);
    public T update(boolean value, Consumer<MapEntryWriteOptions> options);
    public T update(double value, Consumer<MapEntryWriteOptions> options);
    public T update(List<?> value, Consumer<MapEntryWriteOptions> options);
    public T update(Map<?,?> value, Consumer<MapEntryWriteOptions> options);

    public T upsert(long value);
    public T upsert(String value);
    public T upsert(byte[] value);
    public T upsert(boolean value);
    public T upsert(double value);
    public T upsert(List<?> value);
    public T upsert(Map<?,?> value);

    public T upsert(long value, Consumer<MapEntryWriteOptions> options);
    public T upsert(String value, Consumer<MapEntryWriteOptions> options);
    public T upsert(byte[] value, Consumer<MapEntryWriteOptions> options);
    public T upsert(boolean value, Consumer<MapEntryWriteOptions> options);
    public T upsert(double value, Consumer<MapEntryWriteOptions> options);
    public T upsert(List<?> value, Consumer<MapEntryWriteOptions> options);
    public T upsert(Map<?,?> value, Consumer<MapEntryWriteOptions> options);

    public T add(long value);
    public T add(double value);
    public T add(long value, Consumer<MapEntryWriteOptions> options);
    public T add(double value, Consumer<MapEntryWriteOptions> options);
}
