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

import com.aerospike.client.sdk.exp.ExpReadFlags;

/**
 * Options for expression read operations (selectFrom).
 * 
 * <p>Used with lambda configuration pattern:</p>
 * <pre>{@code
 * session.query(key)
 *     .bin("computed").selectFrom("$.age + 20", opt -> opt.ignoreEvalFailure())
 *     .execute();
 * }</pre>
 */
public class ExpressionReadOptions {
    private int flags = ExpReadFlags.DEFAULT;

    /**
     * Ignore failures caused by the expression resolving to unknown or a non-bin type.
     * 
     * @return this options object for method chaining
     */
    public ExpressionReadOptions ignoreEvalFailure() {
        this.flags |= ExpReadFlags.EVAL_NO_FAIL;
        return this;
    }

    /**
     * Get the combined flags value.
     */
    public int getFlags() {
        return flags;
    }
}
