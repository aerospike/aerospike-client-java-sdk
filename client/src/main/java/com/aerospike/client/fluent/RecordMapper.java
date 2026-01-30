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

import java.util.Map;

/**
 * Interface for converting between Aerospike records and Java objects.
 *
 * <p>This interface defines the contract for mappers that can serialize Java objects
 * to Aerospike records and deserialize Aerospike records back to Java objects. It
 * is used by the client to automatically handle object mapping when working with
 * typed datasets.</p>
 *
 * <p>Implementations of this interface should handle the conversion between a specific
 * Java class and its corresponding Aerospike record representation. The mapper is
 * responsible for:</p>
 * <ul>
 *   <li>Converting Java objects to a map of bin names and values for storage</li>
 *   <li>Converting Aerospike records back to Java objects</li>
 *   <li>Extracting the object's ID for key generation</li>
 * </ul>
 *
 * <p>Example implementation:</p>
 * <pre>{@code
 * public class CustomerMapper implements RecordMapper<Customer> {
 *     @Override
 *     public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
 *         return new Customer(
 *             (Long) map.get("id"),
 *             (String) map.get("name"),
 *             (Integer) map.get("age"),
 *             new Date((Long) map.get("dob"))
 *         );
 *     }
 *
 *     @Override
 *     public Map<String, Value> toMap(Customer customer) {
 *         Map<String, Value> map = new HashMap<>();
 *         map.put("id", Value.get(customer.getId()));
 *         map.put("name", Value.get(customer.getName()));
 *         map.put("age", Value.get(customer.getAge()));
 *         map.put("dob", Value.get(customer.getDob().getTime()));
 *         return map;
 *     }
 *
 *     @Override
 *     public Object id(Customer customer) {
 *         return customer.getId();
 *     }
 * }
 * }</pre>
 *
 * @param <T> the type of Java object this mapper handles
 * @see RecordMappingFactory
 * @see DefaultRecordMappingFactory
 */
public interface RecordMapper<T> {
    /**
     * Converts an Aerospike record to a Java object.
     *
     * <p>This method takes the bins from an Aerospike record and converts them
     * back to a Java object of type T. The record key and generation are also
     * provided for context.</p>
     *
     * @param map the map of bin names to values from the Aerospike record
     * @param recordKey the key of the record
     * @param generation the generation of the record
     * @return the Java object created from the record
     */
    T fromMap(Map<String, Object> map, Key recordKey, int generation);

    /**
     * Converts a Java object to a map of bin names and values for storage.
     *
     * <p>This method takes a Java object and converts it to a map where the keys
     * are bin names and the values are Aerospike Value objects. This map will be
     * used to store the object in Aerospike.</p>
     *
     * @param element the Java object to convert
     * @return a map of bin names to Aerospike Value objects
     */
    Map<String, Value> toMap(T element);

    /**
     * Extracts the ID from a Java object for key generation.
     *
     * <p>This method should return the unique identifier for the object that will
     * be used as part of the Aerospike key. The returned value should be of a type
     * that can be used with {@link DataSet#id(Object)}.</p>
     *
     * @param element the Java object
     * @return the ID of the object
     */
    Object id(T element);
}
