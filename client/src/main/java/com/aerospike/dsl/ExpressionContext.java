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
package com.aerospike.dsl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * This class stores input string and optional values for placeholders (if they are used)
 */
@AllArgsConstructor(staticName = "of")
@RequiredArgsConstructor(staticName = "of")
@Getter
public class ExpressionContext {

    /**
     * Input string. If placeholders are used, they should be matched with {@code values}
     */
    private final String expression;
    /**
     * {@link PlaceholderValues} to be matched with placeholders in the {@code input} string.
     * Optional (needed only if there are placeholders)
     */
    private PlaceholderValues values;
}
