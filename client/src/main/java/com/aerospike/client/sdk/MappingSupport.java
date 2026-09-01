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

import java.util.Objects;

/**
 * Helpers for resolving {@link RecordMapper} instances from a {@link RecordMappingFactory}.
 */
public final class MappingSupport {

    private MappingSupport() {
    }

    /**
     * Returns the mapper registered for {@code clazz}, or throws {@link IllegalStateException}
     * with a clear message if the factory is null or no mapper is registered.
     *
     * @param factory mapping factory from the cluster (may be {@code null})
     * @param clazz domain type
     * @param <T> domain type
     * @return non-null mapper
     * @throws NullPointerException if {@code clazz} is {@code null}
     * @throws IllegalStateException if {@code factory} is {@code null} or has no mapper for {@code clazz}
     */
    public static <T> RecordMapper<T> requireMapper(RecordMappingFactory factory, Class<T> clazz) {
        Objects.requireNonNull(clazz, "clazz");
        if (factory == null) {
            throw new IllegalStateException(
                "No RecordMappingFactory is configured on the cluster. "
                    + "Use Cluster.setRecordMappingFactory(...) before mapping type "
                    + clazz.getName() + ".");
        }
        RecordMapper<T> mapper = factory.getMapper(clazz);
        if (mapper == null) {
            throw new IllegalStateException(
                "No RecordMapper registered for type " + clazz.getName()
                    + ". Add it to the RecordMappingFactory on the cluster.");
        }
        return mapper;
    }
}
