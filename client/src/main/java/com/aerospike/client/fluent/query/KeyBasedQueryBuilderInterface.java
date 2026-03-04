package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.ResultCode;

/**
 * Interface for query builders that support key-based operations.
 * These operations are only available when querying specific keys (single key or list of keys).
 */
public interface KeyBasedQueryBuilderInterface<T extends KeyBasedQueryBuilderInterface<T>> extends BaseQueryBuilder<T> {
    /**
     * If the query has a `where` clause and is provided either a single key or a list of keys,
     * any records which are filtered out will appear in the
     * stream against an exception code of {@link ResultCode.FILTERED_OUT} rather than just not 
     * appearing in the result stream.
     * @return this QueryBuilder for method chaining
     */
    T failOnFilteredOut();
    
    /**
     * By default, if a key is provided (or is part of a list of keys) but the key does not map to a record
     * then nothing will be returned in the stream against that key. However, if this flag is specified, {@code null} will be
     * in the stream again that key.
     * @return this QueryBuilder for method chaining
     */
    T includeMissingKeys();
}
