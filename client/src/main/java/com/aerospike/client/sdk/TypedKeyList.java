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
 * distributed under the License is distributed on an "AS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations
 * under the License.
 */
package com.aerospike.client.sdk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A mutable {@link List} of {@link TypedKey} values for the same domain type {@code T}.
 *
 * <p>Use this type (especially values returned from {@link TypedDataSet#ids(int...)}) so
 * {@link Session} and chainable builders can offer overloads alongside {@code List<Key>} without
 * Java erasure collapsing two {@code List}-typed parameters to the same raw signature.</p>
 *
 * <p><strong>Homogeneity:</strong> All SDK entry points that accept a {@code TypedKeyList} still run
 * {@link TypedKey#requireSharedEntityClass} (or equivalent) at operation boundaries. The list is
 * otherwise fully mutable; callers who add keys with a mismatched entity class accept undefined
 * behavior or runtime failures.</p>
 *
 * @param <T> domain type shared by keys in this list (by convention)
 * @see TypedDataSet#ids(int...)
 * @see Session#query(TypedKeyList)
 * @see Session#upsert(TypedKeyList)
 */
public final class TypedKeyList<T> extends ArrayList<TypedKey<T>> {

    private static final long serialVersionUID = 1L;

    public TypedKeyList() {
        super();
    }

    public TypedKeyList(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Copies keys into a new list and validates a single shared entity class.
     *
     * @param keys non-empty list; all keys must share the same entity class
     * @param <T> domain type (caller may use explicit type arguments when the static type is a wildcard)
     * @return new mutable {@code TypedKeyList}
     * @throws IllegalArgumentException if {@code keys} is empty or entity classes differ
     */
    @SuppressWarnings("unchecked")
    public static <T> TypedKeyList<T> of(List<? extends TypedKey<?>> keys) {
        Objects.requireNonNull(keys, "keys");
        TypedKey.requireSharedEntityClass(keys);
        TypedKeyList<T> out = new TypedKeyList<>(keys.size());
        for (TypedKey<?> k : keys) {
            out.add((TypedKey<T>) k);
        }
        return out;
    }

    /**
     * Varargs factory: copies into a new list and validates a single shared entity class.
     *
     * @param keys non-empty; all keys must share the same entity class
     * @param <T> domain type
     * @return new mutable {@code TypedKeyList}
     */
    @SafeVarargs
    public static <T> TypedKeyList<T> of(TypedKey<T>... keys) {
        Objects.requireNonNull(keys, "keys");
        if (keys.length == 0) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        List<TypedKey<T>> asList = Arrays.asList(keys);
        TypedKey.requireSharedEntityClass(asList);
        TypedKeyList<T> out = new TypedKeyList<>(keys.length);
        out.addAll(asList);
        return out;
    }
}
