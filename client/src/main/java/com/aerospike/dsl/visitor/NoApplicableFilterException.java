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
package com.aerospike.dsl.visitor;

import com.aerospike.client.sdk.query.Filter;

/**
 * Indicates that no applicable {@link Filter} could be generated for a given DSL expression. For internal use.
 *
 * <p>This exception is typically thrown when attempting to create a Filter for a DSL expression
 * but the structure or types of the expression do not match any supported filtering patterns
 * (e.g., comparing Strings using arithmetical operations, using OR-combined expression etc.).
 * It signifies that while the expression might be valid in a broader context, it cannot be represented with a
 * secondary index Filter.
 */
class NoApplicableFilterException extends RuntimeException {
    private static final long serialVersionUID = 1L;

	NoApplicableFilterException(String description) {
        super(description);
    }
}
