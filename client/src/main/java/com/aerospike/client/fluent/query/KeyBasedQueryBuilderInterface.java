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
