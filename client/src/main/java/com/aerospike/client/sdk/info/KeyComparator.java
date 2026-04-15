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
package com.aerospike.client.sdk.info;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.sdk.info.annotations.Key;

public class KeyComparator {
    // Cache of class to its key fields
    private static final Map<Class<?>, List<Field>> keyFieldCache = new ConcurrentHashMap<>();

    public static <T> boolean compareKeyFields(T a, T b) {
        if (a == null || b == null) {
            return false;
        }
        if (!a.getClass().equals(b.getClass())) {
            return false;
        }

        Class<?> clazz = a.getClass();
        List<Field> keyFields = keyFieldCache.computeIfAbsent(clazz, KeyComparator::findKeyFields);

        if (keyFields.isEmpty()) {
            return false;
        }

        try {
            for (Field field : keyFields) {
                Object valA = field.get(a);
                Object valB = field.get(b);
                if (!Objects.equals(valA, valB)) {
                    return false;
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to access field", e);
        }

        return true;
    }

    private static List<Field> findKeyFields(Class<?> clazz) {
        List<Field> keyFields = new ArrayList<>();
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Key.class)) {
                field.setAccessible(true);
                keyFields.add(field);
            }
        }
        return keyFields;
    }
}
