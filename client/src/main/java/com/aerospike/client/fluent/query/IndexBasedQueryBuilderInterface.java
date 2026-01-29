package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.policy.QueryDuration;

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
    
    /**
     * Sets the expected query duration. The server optimizes query handling
     * based on this hint.
     * 
     * @param duration the expected duration (LONG, SHORT, or LONG_RELAX_AP)
     * @return this QueryBuilder for method chaining
     */
    T expectedQueryDuration(QueryDuration duration);
}
