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
 * Context for deserializing a record into a Java domain type using a {@link RecordMappingFactory}.
 *
 * <p>Pass {@code RecordReadContext} into the four-argument
 * {@link RecordMapper#fromMap(java.util.Map, Key, int, RecordReadContext)} when mapping needs the
 * {@link Session} or factory (dependent reads, nested objects). Use the three-argument
 * {@link RecordMapper#fromMap(java.util.Map, Key, int)} for pure bin/key/gen mapping (for example
 * explicit {@link RecordMapper} arguments on an untyped {@link RecordStream}).</p>
 *
 * @param <T> entity type
 * @see RecordMapper#fromMap(java.util.Map, Key, int, RecordReadContext)
 */
public final class RecordReadContext<T> {
    private final Session session;
    private final Class<T> entityClass;

    public RecordReadContext(Session session, Class<T> entityClass) {
        this.session = Objects.requireNonNull(session, "session");
        this.entityClass = Objects.requireNonNull(entityClass, "entityClass");
    }

    public Session getSession() {
        return session;
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * Mapping factory configured on the cluster backing this session.
     */
    public RecordMappingFactory getRecordMappingFactory() {
        return session.getRecordMappingFactory();
    }
}
