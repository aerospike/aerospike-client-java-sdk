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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Filter#withOverrides} used by query hint propagation.
 */
public class FilterOverrideTest {

    @Test
    public void overrideIndexName() {
        Filter original = Filter.equal("myBin", 42);
        Filter overridden = Filter.withOverrides(original, null, "explicit_idx");

        assertEquals("myBin", overridden.getName());
        assertEquals("explicit_idx", overridden.getIndexName());
        assertEquals(original.getValType(), overridden.getValType());
        assertEquals(original.getBegin(), overridden.getBegin());
        assertEquals(original.getEnd(), overridden.getEnd());
        assertEquals(original.getCollectionType(), overridden.getCollectionType());
    }

    @Test
    public void overrideBinName() {
        Filter original = Filter.equal("originalBin", "hello");
        Filter overridden = Filter.withOverrides(original, "hintedBin", null);

        assertEquals("hintedBin", overridden.getName());
        assertNull(overridden.getIndexName());
        assertEquals(original.getValType(), overridden.getValType());
        assertEquals(original.getBegin(), overridden.getBegin());
    }

    @Test
    public void overrideBothNullKeepsOriginal() {
        Filter original = Filter.range("rangeBin", 1, 100);
        Filter overridden = Filter.withOverrides(original, null, null);

        assertEquals("rangeBin", overridden.getName());
        assertNull(overridden.getIndexName());
        assertEquals(original.getBegin(), overridden.getBegin());
        assertEquals(original.getEnd(), overridden.getEnd());
    }

    @Test
    public void overridePreservesCollectionType() {
        Filter original = Filter.contains("listBin", IndexCollectionType.LIST, 99);
        Filter overridden = Filter.withOverrides(original, null, "list_idx");

        assertEquals("listBin", overridden.getName());
        assertEquals("list_idx", overridden.getIndexName());
        assertEquals(IndexCollectionType.LIST, overridden.getCollectionType());
    }
}
