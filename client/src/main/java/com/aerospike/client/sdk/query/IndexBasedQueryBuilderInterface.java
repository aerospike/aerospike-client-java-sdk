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
package com.aerospike.client.sdk.query;

import java.util.function.Function;

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
     * Provides hints to influence secondary index selection and query duration for the
     * {@code where} clause on this query.
     *
     * <p>The hint is configured via a lambda that receives a {@link QueryHint.Start} builder.
     * Available options (all optional, compose in any order):</p>
     * <ul>
     *   <li>{@code forIndex(name)} &ndash; use a specific secondary index by name</li>
     *   <li>{@code forBin(name)} &ndash; prefer the secondary index on a specific bin</li>
     *   <li>{@code queryDuration(d)} &ndash; override expected query duration</li>
     * </ul>
     *
     * <p>{@code forIndex} and {@code forBin} are mutually exclusive; attempting to call both
     * will not compile.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * session.query(dataSet)
     *     .where("$.age > 30")
     *     .withHint(hint -> hint.forIndex("age_idx").queryDuration(QueryDuration.SHORT))
     *     .execute();
     * }</pre>
     *
     * @param configurator a function that configures the hint
     * @return this QueryBuilder for method chaining
     * @throws IllegalArgumentException if called more than once
     */
    T withHint(Function<QueryHint.Start, ? extends QueryHint.Result> configurator);
}
