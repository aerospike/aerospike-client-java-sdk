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

import com.aerospike.client.sdk.operation.BitWriteFlags;

/**
 * Write options for bit modify operations on {@link BinBuilder}. Passed as a lambda
 * to overloads such as {@link BinBuilder#bitSet(int, int, byte[], java.util.function.Consumer)}.
 *
 * <p>{@code createOnly()} and {@code updateOnly()} are mutually exclusive;
 * calling both throws {@link IllegalStateException} immediately at the call site.</p>
 *
 * <pre>{@code
 * session.update(key)
 *     .bin("flags").bitOr(17, 6, mask, opt -> opt.updateOnly().noFail())
 *     .execute();
 * }</pre>
 */
public final class BitWriteOptions {
    private int writeMode = BitWriteFlags.DEFAULT;
    private boolean noFail = false;
    private boolean partial = false;

    /**
     * Fail if the bin already exists.
     * Mutually exclusive with {@link #updateOnly()}.
     */
    public BitWriteOptions createOnly() {
        if (writeMode != BitWriteFlags.DEFAULT) {
            throw new IllegalStateException(
                "createOnly() and updateOnly() are mutually exclusive");
        }
        this.writeMode = BitWriteFlags.CREATE_ONLY;
        return this;
    }

    /**
     * Fail if the bin does not already exist.
     * Mutually exclusive with {@link #createOnly()}.
     */
    public BitWriteOptions updateOnly() {
        if (writeMode != BitWriteFlags.DEFAULT) {
            throw new IllegalStateException(
                "createOnly() and updateOnly() are mutually exclusive");
        }
        this.writeMode = BitWriteFlags.UPDATE_ONLY;
        return this;
    }

    /**
     * Do not raise an error if the operation is denied due to write mode constraints.
     */
    public BitWriteOptions noFail() {
        this.noFail = true;
        return this;
    }

    /**
     * Allow other valid operations to be committed if this operation is denied due to
     * flag constraints.
     */
    public BitWriteOptions partial() {
        this.partial = true;
        return this;
    }

    int toFlags() {
        int flags = writeMode;
        if (noFail) {
            flags |= BitWriteFlags.NO_FAIL;
        }
        if (partial) {
            flags |= BitWriteFlags.PARTIAL;
        }
        return flags;
    }
}
