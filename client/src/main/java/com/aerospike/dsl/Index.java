package com.aerospike.dsl;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.query.Filter;
import com.aerospike.client.fluent.query.IndexCollectionType;
import com.aerospike.client.fluent.query.IndexType;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Represents an Aerospike secondary index.
 */
@Builder
@EqualsAndHashCode
@Getter
public class Index {

    /**
     * Namespace of the indexed bin
     */
    private final String namespace;
    /**
     * Name of the indexed bin
     */
    private final String bin;
    /**
     * Name of the index
     */
    private final String name;
    /**
     * Type of the index
     */
    private final IndexType indexType;
    /**
     * Secondary index filter definition
     */
    private final Filter filter;
    /**
     * Cardinality of the index calculated using "sindex-stat" command and looking at the ratio of entries
     * to unique bin values for the given secondary index on the node (entries_per_bval)
     */
    private int binValuesRatio;
    /**
     * {@link IndexCollectionType} of the index
     */
    private IndexCollectionType indexCollectionType;

    /**
     * Array of {@link CTX} representing context of the index
     */
    private CTX[] ctx;
}
