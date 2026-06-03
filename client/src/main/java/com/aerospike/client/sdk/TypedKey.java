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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An Aerospike {@link Key} paired with the Java entity type used for factory-backed
 * {@link RecordMapper} resolution on reads.
 *
 * @param <T> domain type
 * @see Session#query(TypedKey)
 * @see Session#queryTypedKeys(List)
 */
public final class TypedKey<T> {
    private final Key key;
    private final Class<T> entityClass;

    public TypedKey(Key key, Class<T> entityClass) {
        this.key = Objects.requireNonNull(key, "key");
        this.entityClass = Objects.requireNonNull(entityClass, "entityClass");
    }

    public Key getKey() {
        return key;
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * Converts a list of typed keys to native keys (same order).
     */
    public static List<Key> nativeKeys(List<? extends TypedKey<?>> typedKeys) {
        List<Key> out = new ArrayList<>(typedKeys.size());
        for (TypedKey<?> tk : typedKeys) {
            out.add(tk.getKey());
        }
        return out;
    }
}
