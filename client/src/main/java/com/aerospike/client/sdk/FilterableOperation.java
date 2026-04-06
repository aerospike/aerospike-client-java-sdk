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
package com.aerospike.client.sdk;

import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedDsl;

/**
 * Interface for operations that support filtering with where clauses and key-based options.
 * This interface is implemented by both query and update operation builders to provide
 * consistent filtering capabilities across read and write operations.
 *
 * @param <T> the implementing class type for method chaining
 */
public interface FilterableOperation<T extends FilterableOperation<T>> {

    /**
     * Adds a filter condition using a AEL string.
     *
     * <p>This method allows you to specify a filter condition using Aerospike's
     * Domain Specific Language (AEL). The AEL provides a SQL-like syntax for
     * expressing complex filter conditions.</p>
     *
     * <p>Example AEL expressions:</p>
     * <ul>
     *   <li><code>"$.name == 'Tim'"</code> - exact string match</li>
     *   <li><code>"$.age > 30"</code> - numeric comparison</li>
     *   <li><code>"$.name == 'Tim' and $.age > 30"</code> - logical AND</li>
     *   <li><code>"$.name == 'Tim' or $.name == 'Jane'"</code> - logical OR</li>
     * </ul>
     *
     * <p>Only one filter condition can be specified per operation. Multiple calls
     * to this method or other where variants will throw an exception.</p>
     *
     * @param dsl the AEL filter expression
     * @param params The params used to replace arguments in the AEL string (used by {@code String.format(dsl, params)})
     * @return this builder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(String dsl, Object ... params);

    /**
     * Adds a filter condition using a BooleanExpression.
     *
     * <p>This method allows you to specify a filter condition using the programmatic
     * BooleanExpression API. This provides type safety and compile-time checking
     * compared to AEL strings.</p>
     *
     * <p>Only one filter condition can be specified per operation. Multiple calls
     * to this method or other where variants will throw an exception.</p>
     *
     * @param dsl the BooleanExpression filter
     * @return this builder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(BooleanExpression dsl);

    /**
     * Adds a filter condition using a PreparedDsl.
     *
     * <p>Only one filter condition can be specified per operation. Multiple calls
     * to this method or other where variants will throw an exception.</p>
     *
     * @param dsl the PreparedDsl filter
     * @param params parameters to bind to the prepared AEL
     * @return this builder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(PreparedDsl dsl, Object ... params);

    /**
     * Adds a filter condition using an Exp operation.
     *
     * <p>Note: This method may be deprecated in the future -- use a string version instead.</p>
     * <p>Note: If this method is used, no secondary index can be used.</p>
     *
     * <p>Only one filter condition can be specified per operation. Multiple calls
     * to this method or other where variants will throw an exception.</p>
     *
     * @param exp The expression to validate the records against
     * @return this builder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(Exp exp);

    /**
     * Adds a filter condition using an Expression operation.
     *
     * <p>Note: This method may be deprecated in the future -- use a string version instead.</p>
     * <p>Note: If this method is used, no secondary index can be used.</p>
     *
     * <p>Only one filter condition can be specified per operation. Multiple calls
     * to this method or other where variants will throw an exception.</p>
     *
     * @param e The expression to validate the records against
     * @return this builder for method chaining
     * @throws IllegalArgumentException if multiple filter conditions are specified
     */
    T where(Expression e);

    /**
     * If the operation has a `where` clause and is provided either a single key or a list of keys,
     * any records which are filtered out will appear in the result stream with an exception code of
     * {@link com.aerospike.client.ResultCode#FILTERED_OUT} rather than just not appearing in the result stream.
     *
     * <p>This is only applicable to key-based operations (operations with specific keys, not dataset scans).</p>
     *
     * @return this builder for method chaining
     */
    T failOnFilteredOut();

    /**
     * By default, if a key is provided (or is part of a list of keys) but the key does not map to a record
     * then nothing will be returned in the stream against that key. However, if this flag is specified, {@code null} will be
     * in the stream for that key.
     *
     * <p>This is only applicable to key-based operations (operations with specific keys, not dataset scans).</p>
     *
     * @return this builder for method chaining
     */
    T includeMissingKeys();
}
