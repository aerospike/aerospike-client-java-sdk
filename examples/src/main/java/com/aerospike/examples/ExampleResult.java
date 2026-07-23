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
package com.aerospike.examples;

public record ExampleResult(
    String name,
    ExampleStatus status,
    long durationMillis,
    String message,
    Throwable error
) {
    public static ExampleResult passed(String name, long durationMillis) {
        return new ExampleResult(name, ExampleStatus.PASSED, durationMillis, null, null);
    }

    public static ExampleResult skipped(String name, long durationMillis, String message) {
        return new ExampleResult(name, ExampleStatus.SKIPPED, durationMillis, message, null);
    }

    public static ExampleResult failed(String name, long durationMillis, Throwable error) {
        return new ExampleResult(name, ExampleStatus.FAILED, durationMillis, error.getMessage(), error);
    }

    public boolean failed() {
        return status == ExampleStatus.FAILED;
    }
}
