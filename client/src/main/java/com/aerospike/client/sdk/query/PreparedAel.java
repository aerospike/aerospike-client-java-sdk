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
package com.aerospike.client.sdk.query;

/**
 * A reusable AEL template with {@code ?0}, {@code ?1}, ... placeholders (zero-based).
 * Parameter values are substituted client-side into valid AEL literals at execution time.
 */
public class PreparedAel {
    private final String statement;

    public PreparedAel(String statement) {
        this.statement = statement;
    }

    /**
     * Create a prepared AEL template.
     *
     * @param statement AEL template with {@code ?0}, {@code ?1}, ... placeholders
     */
    public static PreparedAel prepare(String statement) {
        return new PreparedAel(statement);
    }

    public String getStatement() {
        return statement;
    }

    /**
     * Substitute bound parameters into the template and return the resulting AEL string.
     *
     * @param params values for {@code ?0}, {@code ?1}, ...
     * @return AEL with placeholders replaced by formatted literals
     */
    public String formValue(Object... params) {
        return AelPlaceholderBinder.bind(statement, params);
    }
}
