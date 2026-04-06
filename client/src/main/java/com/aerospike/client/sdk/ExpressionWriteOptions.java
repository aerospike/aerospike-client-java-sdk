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

import com.aerospike.client.sdk.exp.ExpWriteFlags;

/**
 * Options for expression write operations (insertFrom, updateFrom, upsertFrom).
 * 
 * <p>Used with lambda configuration pattern:</p>
 * <pre>{@code
 * session.upsert(key)
 *     .bin("computed").upsertFrom("$.a + $.b", opt -> opt
 *         .deleteIfNull()
 *         .ignoreOpFailure()
 *         .ignoreEvalFailure())
 *     .execute();
 * }</pre>
 */
public class ExpressionWriteOptions {
    private int flags;

    ExpressionWriteOptions(int baseFlags) {
        this.flags = baseFlags;
    }

    /**
     * If the expression result is nil/null, delete the bin instead of failing.
     * 
     * @return this options object for method chaining
     */
    public ExpressionWriteOptions deleteIfNull() {
        this.flags |= ExpWriteFlags.ALLOW_DELETE;
        return this;
    }

    /**
     * Do not raise an error if the operation is denied due to the base policy
     * (e.g., CREATE_ONLY when bin exists, or UPDATE_ONLY when bin doesn't exist).
     * 
     * @return this options object for method chaining
     */
    public ExpressionWriteOptions ignoreOpFailure() {
        this.flags |= ExpWriteFlags.POLICY_NO_FAIL;
        return this;
    }

    /**
     * Ignore failures caused by the expression resolving to unknown or a non-bin type.
     * 
     * @return this options object for method chaining
     */
    public ExpressionWriteOptions ignoreEvalFailure() {
        this.flags |= ExpWriteFlags.EVAL_NO_FAIL;
        return this;
    }

    /**
     * Get the combined flags value.
     */
    public int getFlags() {
        return flags;
    }
}
