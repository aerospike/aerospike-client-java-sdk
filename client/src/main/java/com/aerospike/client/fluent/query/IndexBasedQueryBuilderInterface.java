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
