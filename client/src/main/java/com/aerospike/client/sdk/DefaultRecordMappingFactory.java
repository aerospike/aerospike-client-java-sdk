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
package com.aerospike.client.sdk;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default {@link RecordMappingFactory}: a thread-safe registry of {@link RecordMapper}
 * instances keyed by exact Java class.
 *
 * <p>Lookups use the requested class only. A mapper registered for {@code Customer} is
 * not returned for a subclass such as {@code PremiumCustomer}; register that type
 * explicitly if it needs its own mapping.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * DefaultRecordMappingFactory factory = new DefaultRecordMappingFactory()
 *     .add(Customer.class, new CustomerMapper())
 *     .add(Address.class, new AddressMapper());
 *
 * cluster.setRecordMappingFactory(factory);
 * }</pre>
 *
 * @see RecordMappingFactory
 * @see RecordMapper
 * @see Cluster#setRecordMappingFactory(RecordMappingFactory)
 */
public class DefaultRecordMappingFactory implements RecordMappingFactory {

    private final ConcurrentHashMap<Class<?>, RecordMapper<?>> map = new ConcurrentHashMap<>();

    /**
     * Creates an empty factory. Register mappers with {@link #add(Class, RecordMapper)}.
     */
    public DefaultRecordMappingFactory() {
    }

    /**
     * Creates a factory containing a copy of the given mappings.
     *
     * @param map Java class to {@link RecordMapper}; must not be {@code null}
     */
    public DefaultRecordMappingFactory(Map<Class<? extends Object>, RecordMapper<? extends Object>> map) {
        addAll(map);
    }

    /**
     * Builds a factory with a single class-to-mapper entry.
     *
     * @param clazz the Java type the mapper handles
     * @param mapper mapper for that type
     * @return a new factory
     */
    public static <T> DefaultRecordMappingFactory of(Class<T> clazz, RecordMapper<T> mapper) {
        return new DefaultRecordMappingFactory().add(clazz, mapper);
    }

    /**
     * Builds a factory with two class-to-mapper entries.
     *
     * @param clazz1 first Java type
     * @param mapper1 mapper for {@code clazz1}
     * @param clazz2 second Java type
     * @param mapper2 mapper for {@code clazz2}
     * @return a new factory
     */
    public static <T1, T2> DefaultRecordMappingFactory of(
            Class<T1> clazz1, RecordMapper<T1> mapper1,
            Class<T2> clazz2, RecordMapper<T2> mapper2) {
        return new DefaultRecordMappingFactory()
            .add(clazz1, mapper1)
            .add(clazz2, mapper2);
    }

    /**
     * Builds a factory with three class-to-mapper entries.
     *
     * @param clazz1 first Java type
     * @param mapper1 mapper for {@code clazz1}
     * @param clazz2 second Java type
     * @param mapper2 mapper for {@code clazz2}
     * @param clazz3 third Java type
     * @param mapper3 mapper for {@code clazz3}
     * @return a new factory
     */
    public static <T1, T2, T3> DefaultRecordMappingFactory of(
            Class<T1> clazz1, RecordMapper<T1> mapper1,
            Class<T2> clazz2, RecordMapper<T2> mapper2,
            Class<T3> clazz3, RecordMapper<T3> mapper3) {
        return new DefaultRecordMappingFactory()
            .add(clazz1, mapper1)
            .add(clazz2, mapper2)
            .add(clazz3, mapper3);
    }

    /**
     * Builds a factory with four class-to-mapper entries.
     *
     * @param clazz1 first Java type
     * @param mapper1 mapper for {@code clazz1}
     * @param clazz2 second Java type
     * @param mapper2 mapper for {@code clazz2}
     * @param clazz3 third Java type
     * @param mapper3 mapper for {@code clazz3}
     * @param clazz4 fourth Java type
     * @param mapper4 mapper for {@code clazz4}
     * @return a new factory
     */
    public static <T1, T2, T3, T4> DefaultRecordMappingFactory of(
            Class<T1> clazz1, RecordMapper<T1> mapper1,
            Class<T2> clazz2, RecordMapper<T2> mapper2,
            Class<T3> clazz3, RecordMapper<T3> mapper3,
            Class<T4> clazz4, RecordMapper<T4> mapper4) {
        return new DefaultRecordMappingFactory()
            .add(clazz1, mapper1)
            .add(clazz2, mapper2)
            .add(clazz3, mapper3)
            .add(clazz4, mapper4);
    }

    /**
     * Registers {@code mapper} for {@code clazz}. Fails if a mapper is already registered
     * for that class. Use {@link #add(Class, RecordMapper, boolean) add(clazz, mapper, true)}
     * to replace. Returns {@code this} so calls can be chained.
     *
     * @param clazz domain type
     * @param mapper mapper for that type
     * @return this factory
     * @throws IllegalStateException if {@code clazz} is already registered
     */
    public <T> DefaultRecordMappingFactory add(Class<T> clazz, RecordMapper<T> mapper) {
        return add(clazz, mapper, false);
    }

    /**
     * Registers {@code mapper} for {@code clazz}.
     *
     * @param clazz domain type
     * @param mapper mapper for that type
     * @param force {@code true} to replace an existing mapper; {@code false} to throw if
     *        {@code clazz} is already registered
     * @return this factory
     * @throws IllegalStateException if {@code force} is {@code false} and {@code clazz} is
     *         already registered
     */
    public <T> DefaultRecordMappingFactory add(Class<T> clazz, RecordMapper<T> mapper, boolean force) {
        Objects.requireNonNull(clazz, "clazz");
        Objects.requireNonNull(mapper, "mapper");
        if (force) {
            map.put(clazz, mapper);
            return this;
        }
        RecordMapper<?> existing = map.putIfAbsent(clazz, mapper);
        if (existing != null) {
            throw new IllegalStateException(
                "A RecordMapper is already registered for " + clazz.getName()
                    + ". Pass force=true to replace it.");
        }
        return this;
    }

    /**
     * Registers every entry in {@code mappings}. Each entry uses
     * {@link #add(Class, RecordMapper)} (no overwrite).
     *
     * @param mappings class-to-mapper entries
     * @return this factory
     * @throws IllegalStateException if any class is already registered
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DefaultRecordMappingFactory addAll(
            Map<? extends Class<?>, ? extends RecordMapper<?>> mappings) {
        Objects.requireNonNull(mappings, "mappings");
        for (Map.Entry<? extends Class<?>, ? extends RecordMapper<?>> entry : mappings.entrySet()) {
            add((Class) entry.getKey(), (RecordMapper) entry.getValue());
        }
        return this;
    }

    /**
     * Removes the mapper registered for {@code clazz}, if any.
     *
     * @param clazz domain type
     * @return this factory
     */
    public DefaultRecordMappingFactory remove(Class<?> clazz) {
        Objects.requireNonNull(clazz, "clazz");
        map.remove(clazz);
        return this;
    }

    /**
     * Returns the mapper registered for {@code clazz}, or {@code null} if none is registered.
     * Does not walk superclasses or interfaces of {@code clazz}: a mapper for
     * {@code Customer} is not used for {@code AuditingCustomer extends Customer}.
     *
     * @param <T> the type of object the mapper handles
     * @param clazz the class to get a mapper for
     * @return the RecordMapper for the class, or null if not found
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> RecordMapper<T> getMapper(Class<T> clazz) {
        return (RecordMapper<T>) this.map.get(clazz);
    }
}
