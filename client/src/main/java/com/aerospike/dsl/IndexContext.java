package com.aerospike.dsl;

import java.util.Collection;

/**
 * This class stores namespace and indexes required to build secondary index Filter.
 *
 * @param namespace Namespace to be used for creating secondary index Filter. Is matched with namespace of indexes
 * @param indexes   Collection of {@link Index} objects to be used for creating secondary index Filter.
 *                  Namespace of indexes is matched with the given {@link #namespace}, bin name and index type are matched
 *                  with bins in DSL String
 */
public record IndexContext(String namespace, Collection<Index> indexes) {

    public static IndexContext of(String namespace, Collection<Index> indexes) {
        return new IndexContext(namespace, indexes);
    }
}
