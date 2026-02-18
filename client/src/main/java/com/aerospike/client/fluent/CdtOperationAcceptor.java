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
package com.aerospike.client.fluent;

/**
 * Interface for accepting CDT operations and returning a builder.
 * This interface enables code sharing between {@link CdtGetOrRemoveBuilder} (read/write)
 * and {@link CdtReadOnlyBuilder} (read-only) by providing a common way to add operations
 * and return to the parent builder.
 *
 * @param <T> the type of the parent builder to return after adding an operation
 */
public interface CdtOperationAcceptor<T> {
    /**
     * Add an operation to the operation list.
     *
     * @param op the operation to add
     */
    void acceptOp(Operation op);

    /**
     * Get the parent builder to return for method chaining.
     *
     * @return the parent builder
     */
    T getParentBuilder();
}
