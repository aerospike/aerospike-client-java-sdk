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

import java.util.List;
import java.util.Optional;

/**
 * Shared helpers for async execution paths that complete a {@link CompletableFuture}
 * without an extra consumer virtual thread.
 */
public final class AsyncExecutionSupport {

    private AsyncExecutionSupport() {}

    /**
     * Creates an {@link AsyncRecordStream} with optional publish-time error routing.
     *
     * @param capacity minimum queue capacity (at least 1)
     * @param errorHandler optional handler; non-OK results are dispatched and not published
     * @return a new async stream
     */
    public static AsyncRecordStream newStream(int capacity, ErrorHandler errorHandler) {
        int cap = Math.max(1, capacity);
        AsyncRecordStream stream = new AsyncRecordStream(cap);
        if (errorHandler != null) {
            stream.withErrorHandler(errorHandler);
        }
        return stream;
    }

    /**
     * Returns zero or one result as an {@link Optional}. An empty list yields {@link Optional#empty()}.
     * More than one result is a programming error and completes the future exceptionally via
     * {@link IllegalStateException}.
     *
     * @param results collected results
     * @return optional containing the sole result, or empty if none
     * @throws IllegalStateException if the list size is greater than 1
     */
    public static Optional<RecordResult> singleAsOptional(List<RecordResult> results) {
        if (results.isEmpty()) {
            return Optional.empty();
        }
        if (results.size() != 1) {
            throw new IllegalStateException(
                "Expected at most one result, but got " + results.size());
        }
        return Optional.of(results.get(0));
    }

    /**
     * Same as {@link #singleAsOptional(List)} for a list of mapped values.
     */
    public static <T> Optional<T> singleMappedAsOptional(List<T> results) {
        if (results.isEmpty()) {
            return Optional.empty();
        }
        if (results.size() != 1) {
            throw new IllegalStateException(
                "Expected at most one result, but got " + results.size());
        }
        return Optional.of(results.get(0));
    }
}
