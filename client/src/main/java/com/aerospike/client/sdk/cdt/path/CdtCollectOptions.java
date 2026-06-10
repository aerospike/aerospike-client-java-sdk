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
package com.aerospike.client.sdk.cdt.path;

import com.aerospike.client.sdk.cdt.SelectFlags;

/**
 * Mutable options bag passed to {@code collect*()} overloads that accept a {@link java.util.function.Consumer}
 * on a CDT path built with {@code onEachChild()}.
 *
 * <p>Use this to OR {@link SelectFlags#NO_FAIL} (and future select flags) into the flags sent to
 * {@link com.aerospike.client.sdk.cdt.CdtOperation#selectByPath}.</p>
 */
public final class CdtCollectOptions {

    private boolean noFail;

    /**
     * Enable or disable {@link SelectFlags#NO_FAIL} for the upcoming {@code collect*} terminal.
     *
     * <p>When {@code true}, the select uses {@code baseFlags | NO_FAIL} so missing path segments do not fail the
     * whole operation (server semantics per Aerospike docs).</p>
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.query(key).bin("catalog").onMapKey("items").onEachChild().collectValues(o -> o.noFail(true)).execute();
     * }</pre>
     *
     * @param value {@code true} to merge {@link SelectFlags#NO_FAIL} into select flags
     * @return {@code this} for fluent configuration
     */
    public CdtCollectOptions noFail(boolean value) {
        this.noFail = value;
        return this;
    }

    /**
     * Returns whether {@link #noFail(boolean)} was set to {@code true}.
     *
     * @return current {@code noFail} flag
     */
    public boolean isNoFail() {
        return noFail;
    }

    /**
     * Merge configured flags into the base bitmask produced by the builder (SDK internal use and for advanced callers).
     *
     * @param baseFlags select flags from the terminal (e.g. {@link SelectFlags#VALUE}) before options are applied
     * @return {@code baseFlags} OR {@link SelectFlags#NO_FAIL} when {@link #noFail(boolean) noFail(true)} was set
     */
    public int mergeSelectFlags(int baseFlags) {
        return noFail ? (baseFlags | SelectFlags.NO_FAIL) : baseFlags;
    }
}
