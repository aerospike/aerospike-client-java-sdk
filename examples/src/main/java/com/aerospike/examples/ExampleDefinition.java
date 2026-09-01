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

import java.util.Set;

/**
 * Registry metadata for one runnable example.
 */
public record ExampleDefinition(
    String name,
    Class<? extends Example> exampleClass,
    ExampleFixture fixture,
    Set<String> tags
) {
    public ExampleDefinition {
        fixture = fixture == null ? ExampleFixture.NONE : fixture;
        tags = tags == null ? Set.of() : Set.copyOf(tags);
    }

    public boolean hasTag(String tag) {
        return tags.contains(tag);
    }
}
