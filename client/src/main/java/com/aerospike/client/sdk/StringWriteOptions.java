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
 * <p>{@code createOnly()} and {@code updateOnly()} are mutually exclusive;
 * calling both throws {@link IllegalStateException} immediately at the call site.
 * {@code createOnly()} is invalid for string operations with a CDT path.</p>
 *
 * <pre>{@code
 * session.update(key)
 *     .bin("name").upper(opt -> opt.updateOnly().noFail())
 *     .execute();
 * }</pre>
 */
public final class StringWriteOptions {
    private int writeMode = StringWriteFlags.DEFAULT;
    private boolean noFail = false;
    private int extraFlags = StringWriteFlags.DEFAULT;

    /**
     * Apply only if the string bin does not already exist.
     * Mutually exclusive with {@link #updateOnly()}.
     */
    public StringWriteOptions createOnly() {
        if (writeMode != StringWriteFlags.DEFAULT) {
            throw new IllegalStateException(
                "createOnly() and updateOnly() are mutually exclusive");
        }
        this.writeMode = StringWriteFlags.CREATE_ONLY;
        return this;
    }

    /**
     * Apply only if the string bin already exists. If the bin is missing, the
     * operation is a no-op and does not create the bin.
     * Mutually exclusive with {@link #createOnly()}.
     */
    public StringWriteOptions updateOnly() {
        if (writeMode != StringWriteFlags.DEFAULT) {
            throw new IllegalStateException(
                "createOnly() and updateOnly() are mutually exclusive");
        }
        this.writeMode = StringWriteFlags.UPDATE_ONLY;
        return this;
    }

    /**
     * Do not raise an error if a parsed modify operation cannot be applied. Does
     * not suppress wrong bin type, invalid UTF-8, invalid flags, malformed CDT paths,
     * or {@link #createOnly()} with a CDT {@code CTX}. A suppressed modify leaves the
     * string unchanged.
     */
    public StringWriteOptions noFail() {
        this.noFail = true;
        return this;
    }

    /**
     * Bitwise-OR additional {@link StringWriteFlags}. This is an advanced escape
     * hatch for raw flag composition; invalid combinations are rejected when the
     * string operation is built.
     */
    public StringWriteOptions withFlags(int extraFlags) {
        this.extraFlags |= extraFlags;
        return this;
    }

    int toFlags() {
        int flags = writeMode | extraFlags;
        if (noFail) {
            flags |= StringWriteFlags.NO_FAIL;
        }
        return flags;
    }
}
