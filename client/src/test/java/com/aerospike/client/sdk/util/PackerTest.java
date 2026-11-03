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
package com.aerospike.client.sdk.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.MapOrder;

/**
 * Enum and UUID values are stored as strings, so they must pack exactly like the equivalent
 * String, including the string particle type that precedes the UTF-8 bytes.
 */
public class PackerTest {
    private enum Status {
        ACTIVE
    }

    private static final UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    public void enumAndUuidListElementsPackAsStrings() {
        assertArrayEquals(
            Packer.pack(List.of("ACTIVE", uuid.toString())),
            Packer.pack(List.of(Status.ACTIVE, uuid)));
    }

    @Test
    public void uuidMapKeyPacksAsString() {
        assertArrayEquals(
            Packer.pack(Map.of(uuid.toString(), 10), MapOrder.UNORDERED),
            Packer.pack(Map.of(uuid, 10), MapOrder.UNORDERED));
    }

    @Test
    public void enumMapKeyPacksAsString() {
        assertArrayEquals(
            Packer.pack(Map.of("ACTIVE", 10), MapOrder.UNORDERED),
            Packer.pack(Map.of(Status.ACTIVE, 10), MapOrder.UNORDERED));
    }
}
