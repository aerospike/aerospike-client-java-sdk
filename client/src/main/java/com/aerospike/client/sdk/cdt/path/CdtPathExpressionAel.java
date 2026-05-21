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

import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Centralized stubs for AEL-based CDT path fragments until the AEL compiler understands path-scoped
 * references (see {@code docs/ael/path-expressions.md} and {@code docs/sdk-documentation/cdt-path-expressions-api.md}).
 */
public final class CdtPathExpressionAel {

    private static final String MSG =
            "AEL and PreparedAel are not yet supported for CDT path filters or modify bodies; use Exp. "
                    + "See docs/ael/path-expressions.md and docs/sdk-documentation/cdt-path-expressions-api.md.";

    private CdtPathExpressionAel() {
    }

    /**
     * Throws {@link UnsupportedOperationException} for raw AEL path overloads (for example {@code onEachChild(String)}).
     *
     * <p>Call sites compile but must not be reached until AEL path support exists; use {@link com.aerospike.client.sdk.exp.Exp}
     * overloads instead.</p>
     *
     * @throws UnsupportedOperationException always with guidance to use {@code Exp} and read the path-expression docs
     */
    public static void throwAelNotSupported() {
        throw new UnsupportedOperationException(MSG);
    }

    /**
     * Throws {@link UnsupportedOperationException} for {@link PreparedAel} path overloads.
     *
     * @param ael prepared template (ignored except for message context)
     * @param params optional bind parameters (ignored)
     * @throws UnsupportedOperationException always
     */
    @SuppressWarnings("unused")
    public static void throwPreparedAelNotSupported(PreparedAel ael, Object... params) {
        throw new UnsupportedOperationException(MSG);
    }
}
