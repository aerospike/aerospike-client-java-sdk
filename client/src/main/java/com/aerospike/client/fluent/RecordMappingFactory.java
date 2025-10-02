package com.aerospike.client.fluent;

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
