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

import java.util.List;
import java.util.Objects;

/**
 * Namespace/set bound to a Java entity type {@code T} for compile-time safety and
 * factory-backed object mapping.
 *
 * <p>{@link #id(String)}, {@link #id(int)}, and other {@code id} overloads return a
 * {@link TypedKey} carrying {@code Class<T>} so read chains can map results without an
 * explicit {@link RecordMapper}. For APIs that require a plain {@link Key} or
 * {@code List<Key>} (for example {@link Session#query(java.util.List)}), use
 * {@link #asKey(int)} / {@link #asKeys(int...)} or {@link #asDataSet()}.</p>
 *
 * <p>Batch-typed key methods {@link #ids(int...)} (and other {@code ids} / {@code idsFromDigests} overloads)
 * return {@link TypedKeyList} so {@link Session} overload resolution can distinguish typed batches from
 * {@code List<Key>} without erasure clashes.</p>
 *
 * @param <T> domain type stored in this set
 * @see Session#query(TypedDataSet)
 * @see RecordMappingFactory
 */
public final class TypedDataSet<T> {

    private final DataSet backing;
    private final Class<T> clazz;

    public TypedDataSet(String namespace, String set, Class<T> clazz) {
        this.backing = DataSet.of(namespace, set);
        this.clazz = Objects.requireNonNull(clazz, "clazz");
    }

    public static <R> TypedDataSet<R> of(String namespace, String set, Class<R> clazz) {
        return new TypedDataSet<>(namespace, set, clazz);
    }

    /**
     * Same namespace and set without the bound Java type (indexes, truncate, background tasks,
     * {@link Session#query(DataSet)}, etc.).
     */
    public DataSet asDataSet() {
        return backing;
    }

    public String getNamespace() {
        return backing.getNamespace();
    }

    public String getSet() {
        return backing.getSet();
    }

    public Class<T> getClazz() {
        return clazz;
    }

    // ---- Typed keys (entity class carried for reads / factory mapping) ----

    public TypedKey<T> id(String id) {
        return new TypedKey<>(backing.id(id), clazz);
    }

    public TypedKey<T> id(byte[] id) {
        return new TypedKey<>(backing.id(id), clazz);
    }

    public TypedKey<T> id(byte[] id, int offset, int length) {
        return new TypedKey<>(backing.id(id, offset, length), clazz);
    }

    public TypedKey<T> id(int id) {
        return new TypedKey<>(backing.id(id), clazz);
    }

    public TypedKey<T> id(long id) {
        return new TypedKey<>(backing.id(id), clazz);
    }

    public TypedKey<T> idForObject(Object object) {
        return new TypedKey<>(backing.idForObject(object), clazz);
    }

    public TypedKeyList<T> ids(List<? extends Object> ids) {
        TypedKeyList<T> out = new TypedKeyList<>();
        for (Object id : ids) {
            out.add(idForObject(id));
        }
        return out;
    }

    public TypedKeyList<T> ids(int... ids) {
        TypedKeyList<T> out = new TypedKeyList<>(ids.length);
        for (int thisId : ids) {
            out.add(id(thisId));
        }
        return out;
    }

    public TypedKeyList<T> ids(long... ids) {
        TypedKeyList<T> out = new TypedKeyList<>(ids.length);
        for (long thisId : ids) {
            out.add(id(thisId));
        }
        return out;
    }

    public TypedKeyList<T> ids(String... ids) {
        TypedKeyList<T> out = new TypedKeyList<>(ids.length);
        for (String thisId : ids) {
            out.add(id(thisId));
        }
        return out;
    }

    public TypedKeyList<T> ids(byte[]... ids) {
        TypedKeyList<T> out = new TypedKeyList<>(ids.length);
        for (byte[] thisId : ids) {
            out.add(id(thisId));
        }
        return out;
    }

    public TypedKey<T> idFromDigest(byte[] digest) {
        return new TypedKey<>(backing.idFromDigest(digest), clazz);
    }

    public TypedKeyList<T> idsFromDigests(byte[]... digests) {
        TypedKeyList<T> out = new TypedKeyList<>(digests.length);
        for (byte[] digest : digests) {
            out.add(idFromDigest(digest));
        }
        return out;
    }

    // ---- Plain Key helpers (no Class<T>; use with untyped Session APIs) ----

    /** Same record identity as {@link #id(String)} but returns a {@link Key}. */
    public Key asKey(String id) {
        return backing.id(id);
    }

    public Key asKey(byte[] id) {
        return backing.id(id);
    }

    public Key asKey(byte[] id, int offset, int length) {
        return backing.id(id, offset, length);
    }

    public Key asKey(int id) {
        return backing.id(id);
    }

    public Key asKey(long id) {
        return backing.id(id);
    }

    public Key asKeyForObject(Object object) {
        return backing.idForObject(object);
    }

    public List<Key> asKeys(List<? extends Object> ids) {
        return backing.ids(ids);
    }

    public List<Key> asKeys(int... ids) {
        return backing.ids(ids);
    }

    public List<Key> asKeys(long... ids) {
        return backing.ids(ids);
    }

    public List<Key> asKeys(String... ids) {
        return backing.ids(ids);
    }

    public List<Key> asKeys(byte[]... ids) {
        return backing.ids(ids);
    }

    public Key asKeyFromDigest(byte[] digest) {
        return backing.idFromDigest(digest);
    }

    public List<Key> asKeysFromDigests(byte[]... digests) {
        return backing.idsFromDigests(digests);
    }

    @Override
    public String toString() {
        return "TypedDataSet [namespace=" + getNamespace() + ", set=" + getSet() + ", clazz=" + clazz.getName() + "]";
    }
}
