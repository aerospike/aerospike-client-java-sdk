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

/**
 * Factory interface for providing record mappers that convert between Aerospike records
 * and Java objects.
 * 
 * <p>This interface defines a factory pattern for obtaining {@link RecordMapper} instances
 * that can handle the serialization and deserialization of Java objects to and from
 * Aerospike records. This enables automatic object mapping when working with typed datasets.</p>
 * 
 * <p>The factory is typically set on a {@link Cluster} instance and is used by the
 * client to automatically find appropriate mappers for different object types.</p>
 * 
 * <p>Example implementation:</p>
 * <pre>{@code
 * public class MyRecordMappingFactory implements RecordMappingFactory {
 *     private final Map<Class<?>, RecordMapper<?>> mappers = new HashMap<>();
 *     
 *     public MyRecordMappingFactory() {
 *         mappers.put(Customer.class, new CustomerMapper());
 *         mappers.put(Address.class, new AddressMapper());
 *     }
 *     
 *     @Override
 *     public <T> RecordMapper<T> getMapper(Class<T> clazz) {
 *         return (RecordMapper<T>) mappers.get(clazz);
 *     }
 * }
 * }</pre>
 * 
 * @see RecordMapper
 * @see DefaultRecordMappingFactory
 * @see Cluster#setRecordMappingFactory(RecordMappingFactory)
 */
public interface RecordMappingFactory {
    /**
     * Gets the record mapper for the specified class.
     * 
     * <p>This method should return a RecordMapper instance that can handle objects
     * of the specified class. If no mapper is available for the class, null should
     * be returned.</p>
     * 
     * @param <T> the type of object the mapper handles
     * @param clazz the class to get a mapper for
     * @return the RecordMapper for the class, or null if not found
     */
    <T> RecordMapper<T> getMapper(Class<T> clazz);
}
