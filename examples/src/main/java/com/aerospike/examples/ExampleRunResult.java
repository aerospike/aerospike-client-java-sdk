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

import java.util.List;

public record ExampleRunResult(List<ExampleResult> results) {
    public ExampleRunResult {
        results = List.copyOf(results);
    }

    public boolean hasFailures() {
        return results.stream().anyMatch(ExampleResult::failed);
    }

    public int exitCode() {
        return hasFailures() ? 1 : 0;
    }

    public long passedCount() {
        return count(ExampleStatus.PASSED);
    }

    public long failedCount() {
        return count(ExampleStatus.FAILED);
    }

    public long skippedCount() {
        return count(ExampleStatus.SKIPPED);
    }

    private long count(ExampleStatus status) {
        return results.stream().filter(result -> result.status() == status).count();
    }
}
