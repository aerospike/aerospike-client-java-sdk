package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a dataset in Aerospike, which is a collection of records within a namespace.
 * A dataset is identified by a namespace and set name combination.
 *
 * <p>This class provides a fluent API for creating Aerospike keys for records within the dataset.
 * It supports various key types including String, Integer, Long, and byte array keys.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * DataSet users = DataSet.of("test", "users");
 * Key userKey = users.id("user123");
 * List<Key> userKeys = users.ids("user1", "user2", "user3");
 * }</pre>
 *
 * @author Aerospike
 * @since 1.0
 */
public class DataSet {
    private final String namespace;
    private final String setName;

    /**
     * Protected constructor for creating a DataSet instance.
     * Use {@link #of(String, String)} for public instantiation.
     *
     * @param namespace the Aerospike namespace
     * @param set the set name within the namespace
     */
    protected DataSet(String namespace, String set) {
        this.namespace = namespace;
        this.setName = set;
    }

    /**
     * Creates a new DataSet instance for the specified namespace and set.
     *
     * @param namespace the Aerospike namespace
     * @param set the set name within the namespace
     * @return a new DataSet instance
     * @throws IllegalArgumentException if namespace or set is null or empty
     */
    public static DataSet of(String namespace, String set) {
        return new DataSet(namespace, set);
    }

    /**
     * Gets the namespace of this dataset.
     *
     * @return the namespace name
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets the set name of this dataset.
     *
     * @return the set name
     */
    public String getSet() {
        return setName;
    }

    /**
     * Creates an Aerospike key with a String identifier.
     *
     * @param id the String identifier for the key
     * @return a new Key instance
     */
    public Key id(String id) {
        return new Key(namespace, setName, id);
    }

    /**
     * Creates an Aerospike key with a byte array identifier.
     *
     * @param id the byte array identifier for the key
     * @return a new Key instance
     */
    public Key id(byte[] id) {
        return new Key(namespace, setName, id);
    }

    /**
     * Creates an Aerospike key with a byte array identifier using a subset of the array.
     *
     * @param id the byte array containing the identifier
     * @param offset the starting offset in the array
     * @param length the number of bytes to use from the array
     * @return a new Key instance
     */
    public Key id(byte[] id, int offset, int length) {
        return new Key(namespace, setName, id, offset, length);
    }

    /**
     * Creates an Aerospike key with an Integer identifier.
     *
     * @param id the Integer identifier for the key
     * @return a new Key instance
     */
    public Key id(int id) {
        return new Key(namespace, setName, id);
    }

    /**
     * Creates an Aerospike key with a Long identifier.
     *
     * @param id the Long identifier for the key
     * @return a new Key instance
     */
    public Key id(long id) {
        return new Key(namespace, setName, id);
    }

    /**
     * Creates an Aerospike key from an Object identifier.
     * Supports String, Integer, Long, and byte array types.
     *
     * @param object the object to use as the key identifier
     * @return a new Key instance
     * @throws IllegalArgumentException if the object type is not supported
     */
    public Key idForObject(Object object) {
        if (object instanceof String) {
            return id((String)object);
        }
        else if (object instanceof Byte || object instanceof Short || object instanceof Integer || object instanceof Long) {
            return id(((Number)object).longValue());
        }
        else if (object.getClass().isArray() && Byte.class.isAssignableFrom(object.getClass().getComponentType())) {
            return id((byte[])object);
        }
        throw new IllegalArgumentException("Cannot construct a key for object of type " + object.getClass().getSimpleName() +
                ". Only String, int, long and byte[] are supported.");
    }

    /**
     * Creates a list of Aerospike keys from a list of objects.
     * Each object is converted to a key using {@link #idForObject(Object)}.
     *
     * @param ids the list of objects to convert to keys
     * @return a list of Key instances
     * @throws IllegalArgumentException if any object type is not supported
     */
    public List<Key> ids(List<? extends Object> ids) {
        List<Key> results = new ArrayList<>();
        for (Object id: ids) {
            results.add(idForObject(id));
        }
        return results;
    }

    /**
     * Creates a list of Aerospike keys from Integer identifiers.
     *
     * @param ids the Integer identifiers for the keys
     * @return a list of Key instances
     */
    public List<Key> ids(int ...ids) {
        List<Key> results = new ArrayList<>();
        for (int thisId: ids) {
            results.add(id(thisId));
        }
        return results;
    }

    /**
     * Creates a list of Aerospike keys from Long identifiers.
     *
     * @param ids the Long identifiers for the keys
     * @return a list of Key instances
     */
    public List<Key> ids(long ...ids) {
        List<Key> results = new ArrayList<>();
        for (long thisId: ids) {
            results.add(id(thisId));
        }
        return results;
    }

    /**
     * Creates a list of Aerospike keys from String identifiers.
     *
     * @param ids the String identifiers for the keys
     * @return a list of Key instances
     */
    public List<Key> ids(String ...ids) {
        List<Key> results = new ArrayList<>();
        for (String thisId: ids) {
            results.add(id(thisId));
        }
        return results;
    }

    /**
     * Creates a list of Aerospike keys from byte array identifiers.
     *
     * @param ids the byte array identifiers for the keys
     * @return a list of Key instances
     */
    public List<Key> ids(byte[] ...ids) {
        List<Key> results = new ArrayList<>();
        for (byte[] thisId: ids) {
            results.add(id(thisId));
        }
        return results;
    }

    public List<Key> idsFromDigests(byte[] ... digests) {
        List<Key> results = new ArrayList<>();
        for (byte[] digest: digests) {
            results.add(new Key(namespace, digest, setName, null));
        }
        return results;

    }

    public Key idFromDigest(byte[] digest) {
        return new Key(namespace, digest, setName, null);
    }


    /**
     * Returns a string representation of this DataSet.
     *
     * @return a string in the format "DataSet [namespace=..., setName=...]"
     */
    @Override
    public String toString() {
        return "DataSet [namespace=" + namespace + ", setName=" + setName + "]";
    }
}
