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
 * Callback for handling per-record errors during execution. Errors dispatched to the handler
 * are excluded from the returned {@link RecordStream}.
 *
 * <p>Pass to {@code executeAsync()} or {@code execute()} as a lambda:</p>
 * <pre>{@code
 * RecordStream rs = session.update(set.id(1)).bin("age").add(1)
 *     .executeAsync((key, index, err) -> log.error("Failed key {} at index {}: {}", key, index, err));
 * }</pre>
 *
 * @see ErrorStrategy
 */
@FunctionalInterface
public interface ErrorHandler {

    /**
     * Handle an error for a specific record.
     *
     * @param key       the key of the record that failed
     * @param index     the operation's position in a batch (0-based), 0 for single-key operations,
     *                  or -1 for index/scan queries
     * @param exception the exception describing the failure
     */
    void handle(Key key, int index, AerospikeException exception);
}
