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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.sdk.info.annotations.Aggregate;
import com.aerospike.client.sdk.info.annotations.And;
import com.aerospike.client.sdk.info.annotations.Average;
import com.aerospike.client.sdk.info.annotations.FirstOf;
import com.aerospike.client.sdk.info.annotations.Maximum;
import com.aerospike.client.sdk.info.annotations.Minimum;
import com.aerospike.client.sdk.info.annotations.MustMatch;
import com.aerospike.client.sdk.info.annotations.Or;

public class StatMerger {
    // Cache of fields per class
    private static final Map<Class<?>, List<Field>> fieldCache = new ConcurrentHashMap<>();

    /**
     * Merges a list of stats objects into a single aggregated instance.
     */
    public static <T> T merge(List<T> statsList) throws Exception {
        if (statsList == null || statsList.isEmpty()) {
            return null;
        }

        // Find the first non-null element to use as the base class
        T firstNonNull = null;
        for (T item : statsList) {
            if (item != null) {
                firstNonNull = item;
                break;
            }
        }

        if (firstNonNull == null) {
            return null; // all values are null
        }

        Class<?> clazz = firstNonNull.getClass();
        @SuppressWarnings("unchecked")
        T result = (T) clazz.getDeclaredConstructor().newInstance();

        for (Field field : getCachedFields(clazz)) {
            List<Object> values = new ArrayList<>();
            for (T stat : statsList) {
                Object val = field.get(stat);
                if (val != null) {
                    values.add(val);
                }
            }

            Object merged = mergeField(field, values);
            field.set(result, merged);
        }
        return result;
    }

    /**
     * Returns cached list of all declared fields (public, protected, private).
     */
    private static List<Field> getCachedFields(Class<?> clazz) {
        return fieldCache.computeIfAbsent(clazz, c -> {
            List<Field> fields = new ArrayList<>();
            while (c != null && c != Object.class) {
                for (Field field : c.getDeclaredFields()) {
                    // Ignore static fields
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }

                    field.setAccessible(true);
                    fields.add(field);
                }
                c = c.getSuperclass();
            }
            return fields;
        });
    }

    /**
     * Merges a single field based on its annotation.
     */
    @SuppressWarnings("unchecked")
    private static Object mergeField(Field field, List<Object> values) throws Exception {
        if (values.isEmpty()) {
            return null;
        }

        if (field.isAnnotationPresent(Aggregate.class)) {
            return mergeAggregate(values, field.getType());
        }

        if (field.isAnnotationPresent(And.class)) {
            return mergeAnd(values, field.getType());
        }

        if (field.isAnnotationPresent(Average.class)) {
            return mergeAverage(values, field.getType());
        }

        if (field.isAnnotationPresent(FirstOf.class)) {
            return mergeFirstOf(values, field.getType(), field.getAnnotation(FirstOf.class).value());
        }

        if (field.isAnnotationPresent(Minimum.class)) {
            // Enums use most common by default
            return mergeMinimum(values, field.getType());
        }

        if (field.isAnnotationPresent(Maximum.class)) {
            return mergeMaximum(values, field.getType());
        }

        if (field.isAnnotationPresent(MustMatch.class)) {
            Object first = values.get(0);
            for (Object v : values) {
                if (!Objects.equals(first, v)) {
                    throw new IllegalStateException("Mismatch on field '" + field.getName() + "': " + first + " vs " + v);
                }
            }
            return first;
        }

        if (field.isAnnotationPresent(Or.class)) {
            return mergeOr(values, field.getType());
        }

        if (List.class.isAssignableFrom(field.getType())) {
            // Avoid compile error as List<Object> cannot be cast to List<List<Object>>
            List<List<Object>> lists = new ArrayList<>(values.size());
            for (Object value : values) {
                lists.add((List<Object>)value);
            }
            return mergeLists(lists);
        }
        // Recurse into sub-objects if not a simple type
        if (!field.getType().isEnum() && !field.getType().isPrimitive() && !isJavaLangType(field.getType())) {
            return merge(values);
        }

        if (field.getType() == int.class || field.getType() == Integer.class ||
                field.getType() == long.class || field.getType() == Long.class) {
            // Integer types use Aggregation by default
            return mergeAggregate(values, field.getType());
        }

        if (field.getType() == float.class || field.getType() == Float.class ||
                field.getType() == double.class || field.getType() == Double.class) {
            // floating point types use Average by default
            return mergeAverage(values, field.getType());
        }

        if (field.getType() == boolean.class || field.getType() == Boolean.class) {
            // Boolean types use AND by default
            return mergeAnd(values, field.getType());
        }

        if (field.getType().isEnum() || field.getType() == String.class) {
            return mergeMostCommon(values, field.getType());
        }
        // Fallback: just take the first value
        return values.get(0);
    }

    private static <T> List<T> mergeLists(List<List<T>> lists) throws Exception {
        if (lists == null || lists.isEmpty()) {
            return List.of();
        }

        // Find the maximum length of any inner list
        int maxLength = lists.stream()
            .filter(Objects::nonNull)
            .mapToInt(List::size)
            .max()
            .orElse(0);

        List<T> result = new ArrayList<>(maxLength);

        for (int i = 0; i < maxLength; i++) {
            List<T> column = new ArrayList<>();
            for (List<T> row : lists) {
                if (row == null || i >= row.size()) {
                    column.add(null);
                } else {
                    column.add(row.get(i));
                }
            }
            result.add(merge(column));
        }

        return result;
    }

    private static Object mergeAggregate(List<Object> values, Class<?> type) {
        if (type == int.class || type == Integer.class) {
            return values.stream().mapToInt(v -> (int) v).sum();
        }
        if (type == long.class || type == Long.class) {
            return values.stream().mapToLong(v -> (long) v).sum();
        }
        if (type == float.class || type == Float.class) {
            return (float) values.stream().mapToDouble(v -> ((Number) v).doubleValue()).sum();
        }
        if (type == double.class || type == Double.class) {
            return values.stream().mapToDouble(v -> ((Number) v).doubleValue()).sum();
        }
        throw new UnsupportedOperationException("Unsupported type for @Aggregate: " + type);
    }

    private static Object mergeAverage(List<Object> values, Class<?> type) {
        if (type == int.class || type == Integer.class) {
            return (int) values.stream().mapToInt(v -> (int) v).average().orElse(0);
        }
        if (type == long.class || type == Long.class) {
            return (long) values.stream().mapToLong(v -> (long) v).average().orElse(0);
        }
        if (type == float.class || type == Float.class) {
            return (float) values.stream().mapToDouble(v -> ((Number) v).doubleValue()).average().orElse(0.0);
        }
        if (type == double.class || type == Double.class) {
            return values.stream().mapToDouble(v -> ((Number) v).doubleValue()).average().orElse(0.0);
        }
        throw new UnsupportedOperationException("Unsupported type for @Average: " + type);
    }

    private static Object mergeMinimum(List<Object> values, Class<?> type) {
        if (type == int.class || type == Integer.class) {
            return (int) values.stream().mapToInt(v -> (int) v).min().orElse(0);
        }
        if (type == long.class || type == Long.class) {
            return (long) values.stream().mapToLong(v -> (long) v).min().orElse(0);
        }
        if (type == float.class || type == Float.class) {
            return (float) values.stream().mapToDouble(v -> ((Number) v).doubleValue()).min().orElse(0.0);
        }
        if (type == double.class || type == Double.class) {
            return values.stream().mapToDouble(v -> ((Number) v).doubleValue()).min().orElse(0.0);
        }
        throw new UnsupportedOperationException("Unsupported type for @Minimum: " + type);
    }

    private static Object mergeMaximum(List<Object> values, Class<?> type) {
        if (type == int.class || type == Integer.class) {
            return (int) values.stream().mapToInt(v -> (int) v).max().orElse(0);
        }
        if (type == long.class || type == Long.class) {
            return (long) values.stream().mapToLong(v -> (long) v).max().orElse(0);
        }
        if (type == float.class || type == Float.class) {
            return (float) values.stream().mapToDouble(v -> ((Number) v).doubleValue()).max().orElse(0.0);
        }
        if (type == double.class || type == Double.class) {
            return values.stream().mapToDouble(v -> ((Number) v).doubleValue()).max().orElse(0.0);
        }
        throw new UnsupportedOperationException("Unsupported type for @Maximum: " + type);
    }

    private static <T> int indexOf(T item, T[] items) {
        for (int i = 0; i < items.length; i++) {
            if (items[i].equals(item)) {
                return i;
            }
        }
        return -1;
    }

    private static <T> T mergeFirstOf(List<T> values, Class<?> type, String[] preferredOrder) {
        T result = null;
        int lowestIndex = Integer.MAX_VALUE;
        for (T val : values) {
            if (val != null) {
                String thisValue = val.toString();
                int index = indexOf(thisValue, preferredOrder);
                if (index >= 0 && index < lowestIndex) {
                    lowestIndex = index;
                    result = val;
                }
            }
        }
        return result == null? values.get(0) : result;
    }

    private static <T> T mergeMostCommon(List<T> values, Class<?> type) {
        Map<T, Integer> counts = new HashMap<>();
        for (T item : values) {
            if (item != null) {
                counts.put(item, counts.getOrDefault(item, 0) + 1);
            }
        }

        T mostCommon = null;
        int maxCount = -1;

        for (Map.Entry<T, Integer> entry : counts.entrySet()) {
            if (entry.getValue() > maxCount) {
                mostCommon = entry.getKey();
                maxCount = entry.getValue();
            }
        }

        return mostCommon;
    }

    private static Object mergeAnd(List<Object> values, Class<?> type) {
        if (type == boolean.class || type == Boolean.class) {
            for (Object val : values) {
                if (val != null && (Boolean)val == false) {
                    return false;
                }
            }
            return true;
        }
        throw new UnsupportedOperationException("Unsupported type for @And: " + type);
    }

    private static Object mergeOr(List<Object> values, Class<?> type) {
        if (type == boolean.class || type == Boolean.class) {
            for (Object val : values) {
                if (val != null && (Boolean)val == true) {
                    return true;
                }
            }
            return false;
        }
        throw new UnsupportedOperationException("Unsupported type for @Or: " + type);
    }


    private static boolean isJavaLangType(Class<?> clazz) {
        return clazz.isPrimitive() || clazz.getPackageName().startsWith("java.lang");
    }
}