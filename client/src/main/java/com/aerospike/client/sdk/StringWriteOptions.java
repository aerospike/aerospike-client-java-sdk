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

import com.aerospike.client.sdk.operation.StringWriteFlags;

/**
 * Write options for string modify operations on {@link BinBuilder}. Passed as a lambda
 * to overloads such as {@link BinBuilder#upper(java.util.function.Consumer)}.
 *
 * <pre>{@code
 * session.update(key)
 *     .bin("name").upper(opt -> opt.noFail())
 *     .execute();
 * }</pre>
 */
public final class StringWriteOptions {
    private int flags = StringWriteFlags.DEFAULT;

    /**
     * Do not raise an error if the operation cannot be applied to the bin (for example
     * wrong bin type). The bin is left unchanged and a null result is returned for that
     * operation. See server docs: {@code NO_FAIL} does not bypass CDT path failures for
     * nested string contexts.
     */
    public StringWriteOptions noFail() {
        this.flags |= StringWriteFlags.NO_FAIL;
        return this;
    }

    /**
     * Bitwise-OR additional {@link StringWriteFlags} (use only when composing flags
     * not exposed as methods).
     */
    public StringWriteOptions withFlags(int extraFlags) {
        this.flags |= extraFlags;
        return this;
    }

    int toFlags() {
        return flags;
    }
}
