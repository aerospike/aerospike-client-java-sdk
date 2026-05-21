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

import com.aerospike.client.sdk.cdt.ModifyFlags;

/**
 * Mutable options bag passed to {@code modifyBy} / {@code removeMatches} overloads that accept a
 * {@link java.util.function.Consumer} on a CDT path built with {@code onEachChild()}.
 *
 * <p>Use this to OR {@link ModifyFlags#NO_FAIL} into the flags sent to
 * {@link com.aerospike.client.sdk.cdt.CdtOperation#modifyByPath}.</p>
 */
public final class CdtModifyOptions {

    private boolean noFail;

    /**
     * Enable or disable {@link ModifyFlags#NO_FAIL} for the upcoming {@code modifyBy} / {@code removeMatches} terminal.
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.upsert(key)
     *     .bin("nums").onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0)))
     *     .removeMatches(o -> o.noFail(true))
     *     .execute();
     * }</pre>
     *
     * @param value {@code true} to merge {@link ModifyFlags#NO_FAIL} into modify flags
     * @return {@code this} for fluent configuration
     */
    public CdtModifyOptions noFail(boolean value) {
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
     * Merge configured flags into the base bitmask for {@code modifyByPath} (SDK internal use).
     *
     * @param baseFlags modify flags from the terminal (typically {@link ModifyFlags#DEFAULT}) before options apply
     * @return {@code baseFlags} OR {@link ModifyFlags#NO_FAIL} when {@link #noFail(boolean) noFail(true)} was set
     */
    public int mergeModifyFlags(int baseFlags) {
        return noFail ? (baseFlags | ModifyFlags.NO_FAIL) : baseFlags;
    }
}
