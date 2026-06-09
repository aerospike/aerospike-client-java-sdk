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

    /**
     * Validates that every typed key uses the same entity class (same rule as
     * {@link ChainableQueryBuilder#initQueryTyped(List)} for typed batch reads).
     *
     * @param typedKeys non-empty list
     * @return the shared entity class
     * @throws IllegalArgumentException if the list is empty or entity classes differ
     */
    public static Class<?> requireSharedEntityClass(List<? extends TypedKey<?>> typedKeys) {
        if (typedKeys == null || typedKeys.isEmpty()) {
            throw new IllegalArgumentException("typedKeys must not be empty");
        }
        Class<?> entity = typedKeys.get(0).getEntityClass();
        for (int i = 1; i < typedKeys.size(); i++) {
            if (!typedKeys.get(i).getEntityClass().equals(entity)) {
                throw new IllegalArgumentException(
                    "All TypedKey entries must share the same entity class; found "
                        + entity.getName() + " and " + typedKeys.get(i).getEntityClass().getName());
            }
        }
        return entity;
    }
}
