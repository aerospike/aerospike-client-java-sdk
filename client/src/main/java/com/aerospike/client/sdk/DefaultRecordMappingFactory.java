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

/**
 * Default implementation of {@link RecordMappingFactory} that uses a map to store
 * record mappers for different Java classes.
 *
 * <p>This factory provides a simple way to register record mappers for different
 * object types. It maintains a mapping from Java class to {@link RecordMapper}
 * instance, allowing automatic object serialization and deserialization when
 * working with typed datasets.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * CustomerMapper customerMapper = new CustomerMapper();
 * AddressMapper addressMapper = new AddressMapper();
 *
 * DefaultRecordMappingFactory factory = new DefaultRecordMappingFactory(Map.of(
 *     Customer.class, customerMapper,
 *     Address.class, addressMapper
 * ));
 *
 * cluster.setRecordMappingFactory(factory);
 * }</pre>
 *
 * @see RecordMappingFactory
 * @see RecordMapper
 * @see Cluster#setRecordMappingFactory(RecordMappingFactory)
 */
public class DefaultRecordMappingFactory implements RecordMappingFactory {

    private final Map<Class<? extends Object>, RecordMapper<? extends Object>> map;

    /**
     * Creates a new DefaultRecordMappingFactory with the specified mapper mappings.
     *
     * <p>The map should contain entries where the key is a Java class and the value
     * is the corresponding RecordMapper instance that can handle objects of that class.</p>
     *
     * @param map a map from Java class to RecordMapper instance
     */
    public DefaultRecordMappingFactory(Map<Class<? extends Object>, RecordMapper<? extends Object>> map) {
        this.map = map;
    }

    /**
     * Builds a factory with a single class-to-mapper entry.
     *
     * @param clazz the Java type the mapper handles
     * @param mapper mapper for that type
     * @return a new factory backed by {@link Map#of(Object, Object)}
     */
    public static <T> DefaultRecordMappingFactory of(Class<T> clazz, RecordMapper<T> mapper) {
        return new DefaultRecordMappingFactory(Map.of(clazz, mapper));
    }

    /**
     * Builds a factory with two class-to-mapper entries.
     *
     * @param clazz1 first Java type
     * @param mapper1 mapper for {@code clazz1}
     * @param clazz2 second Java type
     * @param mapper2 mapper for {@code clazz2}
     * @return a new factory backed by {@link Map#of(Object, Object, Object, Object)}
     */
    public static <T1, T2> DefaultRecordMappingFactory of(
            Class<T1> clazz1, RecordMapper<T1> mapper1,
            Class<T2> clazz2, RecordMapper<T2> mapper2) {
        return new DefaultRecordMappingFactory(Map.of(
                clazz1, mapper1,
                clazz2, mapper2
                ));
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
     * @return a new factory backed by a fixed-size map from {@link Map#of}
     */
    public static <T1, T2, T3> DefaultRecordMappingFactory of(
            Class<T1> clazz1, RecordMapper<T1> mapper1,
            Class<T2> clazz2, RecordMapper<T2> mapper2,
            Class<T3> clazz3, RecordMapper<T3> mapper3) {
        return new DefaultRecordMappingFactory(Map.of(
                clazz1, mapper1,
                clazz2, mapper2,
                clazz3, mapper3
                ));
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
     * @return a new factory backed by a fixed-size map from {@link Map#of}
     */
    public static <T1, T2, T3, T4> DefaultRecordMappingFactory of(
            Class<T1> clazz1, RecordMapper<T1> mapper1,
            Class<T2> clazz2, RecordMapper<T2> mapper2,
            Class<T3> clazz3, RecordMapper<T3> mapper3,
            Class<T4> clazz4, RecordMapper<T4> mapper4) {
        return new DefaultRecordMappingFactory(Map.of(
                clazz1, mapper1,
                clazz2, mapper2,
                clazz3, mapper3,
                clazz4, mapper4
                ));
    }

    /**
     * Gets the record mapper for the specified class.
     *
     * <p>This method looks up the appropriate RecordMapper for the given class
     * in the internal map. If no mapper is found for the class, null is returned.</p>
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
