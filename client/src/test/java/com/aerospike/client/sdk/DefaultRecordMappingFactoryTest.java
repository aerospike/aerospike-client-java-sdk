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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultRecordMappingFactory} (no cluster required).
 */
public class DefaultRecordMappingFactoryTest {

    private static class ParentEntity {
    }

    private static class ChildEntity extends ParentEntity {
    }

    private static final class ParentMapper implements RecordMapper<ParentEntity> {
        @Override
        public ParentEntity fromMap(Map<String, Object> map, Key recordKey, int generation) {
            return new ParentEntity();
        }

        @Override
        public Map<String, Object> toMap(ParentEntity element) {
            return Map.of();
        }

        @Override
        public Object id(ParentEntity element) {
            return "p";
        }
    }

    public static final class T0 {}
    public static final class T1 {}
    public static final class T2 {}
    public static final class T3 {}
    public static final class T4 {}
    public static final class T5 {}

    private static <T> RecordMapper<T> emptyMapper() {
        return new RecordMapper<T>() {
            @Override
            public T fromMap(Map<String, Object> map, Key recordKey, int generation) {
                return null;
            }

            @Override
            public Map<String, Object> toMap(T element) {
                return Map.of();
            }

            @Override
            public Object id(T element) {
                return "x";
            }
        };
    }

    @Test
    public void addChainsAndSupportsMoreThanFourTypes() {
        DefaultRecordMappingFactory factory = new DefaultRecordMappingFactory()
            .add(T0.class, emptyMapper())
            .add(T1.class, emptyMapper())
            .add(T2.class, emptyMapper())
            .add(T3.class, emptyMapper())
            .add(T4.class, emptyMapper())
            .add(T5.class, emptyMapper());
        assertNotNull(factory.getMapper(T0.class));
        assertNotNull(factory.getMapper(T5.class));
    }

    @Test
    public void addThrowsWhenClassAlreadyRegistered() {
        ParentMapper first = new ParentMapper();
        ParentMapper second = new ParentMapper();
        DefaultRecordMappingFactory factory = new DefaultRecordMappingFactory()
            .add(ParentEntity.class, first);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> factory.add(ParentEntity.class, second));
        assertTrue(ex.getMessage().contains(ParentEntity.class.getName()));
        assertSame(first, factory.getMapper(ParentEntity.class));
    }

    @Test
    public void addWithForceReplacesExisting() {
        ParentMapper first = new ParentMapper();
        ParentMapper second = new ParentMapper();
        DefaultRecordMappingFactory factory = new DefaultRecordMappingFactory()
            .add(ParentEntity.class, first)
            .add(ParentEntity.class, second, true);
        assertSame(second, factory.getMapper(ParentEntity.class));
    }

    @Test
    public void getMapperDoesNotReturnParentMapperForSubclass() {
        DefaultRecordMappingFactory factory = new DefaultRecordMappingFactory()
            .add(ParentEntity.class, new ParentMapper());
        assertNotNull(factory.getMapper(ParentEntity.class));
        assertNull(factory.getMapper(ChildEntity.class));
    }

    @Test
    public void removeUnregisters() {
        DefaultRecordMappingFactory factory = DefaultRecordMappingFactory.of(
            ParentEntity.class, new ParentMapper());
        factory.remove(ParentEntity.class);
        assertNull(factory.getMapper(ParentEntity.class));
    }
}
