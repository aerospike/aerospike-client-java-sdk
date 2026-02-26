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

import java.util.Objects;

/**
 * Internal representation of the resolved error routing decision. Constructed from user-facing
 * {@link ErrorStrategy} or {@link ErrorHandler} parameters combined with execution context
 * (single-key vs batch, sync vs async).
 *
 * <p>Threaded through {@code OperationSpecExecutor} and result-building code to determine
 * what happens when a per-record error is encountered.</p>
 */
public sealed interface ErrorDisposition
        permits ErrorDisposition.Throw, ErrorDisposition.InStream, ErrorDisposition.Handler {

    /** Throw the exception immediately. Default for single-key sync operations. */
    Throw THROW = new Throw();

    /** Embed errors in the RecordStream as RecordResult entries. Default for batch and async operations. */
    InStream IN_STREAM = new InStream();

    /** Create a Handler disposition that dispatches errors to the given callback. */
    static Handler handler(ErrorHandler errorHandler) {
        Objects.requireNonNull(errorHandler, "ErrorHandler must not be null");
        return new Handler(errorHandler);
    }

    /** Resolve an {@link ErrorStrategy} to the corresponding disposition. */
    static ErrorDisposition fromStrategy(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return switch (strategy) {
            case IN_STREAM -> IN_STREAM;
        };
    }

    /** Throw the exception immediately. */
    record Throw() implements ErrorDisposition {}

    /** Embed the error in the RecordStream as a RecordResult with a non-OK result code. */
    record InStream() implements ErrorDisposition {}

    /** Dispatch the error to an {@link ErrorHandler} callback, excluding it from the stream. */
    record Handler(ErrorHandler errorHandler) implements ErrorDisposition {
        public Handler {
            Objects.requireNonNull(errorHandler, "ErrorHandler must not be null");
        }
    }
}
