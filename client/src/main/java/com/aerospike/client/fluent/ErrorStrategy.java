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
 * Strategy for handling per-record errors during asynchronous or explicitly overridden execution.
 *
 * <p>Pass to {@code executeAsync()} or {@code execute()} to control how errors are surfaced:</p>
 * <pre>{@code
 * // Errors embedded in the RecordStream as failed RecordResult entries
 * RecordStream rs = session.update(set.id(1)).bin("age").add(1)
 *     .executeAsync(ErrorStrategy.IN_STREAM);
 * }</pre>
 *
 * @see ErrorHandler
 */
public enum ErrorStrategy {

    /**
     * Embed errors in the {@link RecordStream} as {@link RecordResult} entries with non-OK result codes.
     * The caller is responsible for checking each result via {@link RecordResult#isOk()} or
     * {@link RecordResult#orThrow()}.
     */
    IN_STREAM
}
