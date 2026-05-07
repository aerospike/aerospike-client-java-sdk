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

import com.aerospike.client.sdk.operation.HLLWriteFlags;

/**
 * Write options for HLL operations. Passed as a lambda to HLL write methods.
 *
 * <p>{@code createOnly()} and {@code updateOnly()} are mutually exclusive;
 * calling both throws {@link IllegalStateException} immediately at the call site.</p>
 *
 * <pre>{@code
 * .bin("hll").hllInit(HllConfig.of(10), opt -> opt.createOnly().noFail())
 * }</pre>
 */
public class HllWriteOptions {
    private int writeMode = HLLWriteFlags.DEFAULT;
    private boolean noFail = false;
    private boolean allowFold = false;

    /**
     * Fail if the HLL bin already exists.
     * Mutually exclusive with {@link #updateOnly()}.
     */
    public HllWriteOptions createOnly() {
        if (writeMode != HLLWriteFlags.DEFAULT) {
            throw new IllegalStateException(
                "createOnly() and updateOnly() are mutually exclusive");
        }
        this.writeMode = HLLWriteFlags.CREATE_ONLY;
        return this;
    }

    /**
     * Fail if the HLL bin does not already exist.
     * Mutually exclusive with {@link #createOnly()}.
     */
    public HllWriteOptions updateOnly() {
        if (writeMode != HLLWriteFlags.DEFAULT) {
            throw new IllegalStateException(
                "createOnly() and updateOnly() are mutually exclusive");
        }
        this.writeMode = HLLWriteFlags.UPDATE_ONLY;
        return this;
    }

    /**
     * Do not raise an error if the operation is denied due to write mode constraints.
     */
    public HllWriteOptions noFail() {
        this.noFail = true;
        return this;
    }

    /**
     * Allow the resulting set to use the minimum of provided index bits.
     * Also allows less precise HLL algorithms when minhash bits of
     * participating sets do not match.
     */
    public HllWriteOptions allowFold() {
        this.allowFold = true;
        return this;
    }

    int toFlags() {
        int flags = writeMode;
        if (noFail) {
            flags |= HLLWriteFlags.NO_FAIL;
        }
        if (allowFold) {
            flags |= HLLWriteFlags.ALLOW_FOLD;
        }
        return flags;
    }
}
