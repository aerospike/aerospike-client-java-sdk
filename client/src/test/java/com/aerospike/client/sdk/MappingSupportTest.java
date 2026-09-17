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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MappingSupport} (no cluster required).
 */
public class MappingSupportTest {

    @Test
    public void requireMapper_nullFactory_throws() {
        assertThrows(IllegalStateException.class,
            () -> MappingSupport.requireMapper(null, String.class));
    }

    @Test
    public void requireMapper_missingRegistration_throws() {
        RecordMappingFactory f = new RecordMappingFactory() {
            @Override
            public <T> RecordMapper<T> getMapper(Class<T> clazz) {
                return null;
            }
        };
        assertThrows(IllegalStateException.class,
            () -> MappingSupport.requireMapper(f, String.class));
    }

    @Test
    public void requireMapper_nullClass_throws() {
        assertThrows(NullPointerException.class,
            () -> MappingSupport.requireMapper(null, null));
    }
}
