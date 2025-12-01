package com.aerospike.client.fluent.query;

/**
 * Interface for query builders that support index (primary or secondary) based operations.
 * These operations are only available when querying from a DataSet
 */
public interface IndexBasedQueryBuilderInterface<T extends IndexBasedQueryBuilderInterface<T>> extends BaseQueryBuilder<T> {
    
    /**
     * Rate limit the records per second returned from the server. Note that this will force
     * this to be a "long" query, allowing it to be tracked on the server.
     *  
     * @return this QueryBuilder for method chaining
     */
    T recordsPerSecond(int recordsPerSecond);
}
