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
package com.aerospike.client.sdk.util;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.RecordMapper;

public class MapUtil {
    /**
     * Safely extract a String value from a map
     * @param map The source map
     * @param key The key to extract
     * @return The string value or null if not found or not a string
     */
    public static String asString(Map<String, Object> map, String key) {
        return (String)map.get(key);
    }

    /**
     * Safely extract a long value from a map
     * @param map The source map
     * @param key The key to extract
     * @return The long value or 0 if not found or not a number
     */
    public static long asLong(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0L;
    }

    public static <T extends Enum<T>> T asEnum(Map<String, Object> map, String key, Class<T> clazz) {
        Object raw = map.get(key);
        if (raw == null)
		 {
			return null; // or throw if you prefer
		}

        // Already the right enum type
        if (clazz.isInstance(raw)) {
            return clazz.cast(raw);
        }

        // Convert from String (supports exact and case-insensitive match)
        if (raw instanceof String) {
            String s = ((String) raw).trim();
            if (s.isEmpty()) {
				return null;
			}

            // Try exact name first (fast path)
            try {
                return Enum.valueOf(clazz, s);
            } catch (IllegalArgumentException ignored) {
                // Fallback: case-insensitive match
                for (T constant : clazz.getEnumConstants()) {
                    if (constant.name().equalsIgnoreCase(s)) {
                        return constant;
                    }
                }
            }
            throw new IllegalArgumentException(
                "Value for key '" + key + "' ('" + s + "') is not a valid " + clazz.getSimpleName());
        }

        throw new IllegalArgumentException(
            "Value for key '" + key + "' is a " + raw.getClass().getSimpleName() + ", expected String or " + clazz.getSimpleName());
    }

    /**
     * Safely extract a List value from a map
     * @param map The source map
     * @param key The key to extract
     * @return The list value or null if not found or not a list
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> asList(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof List ? (List<T>) value : null;
    }

    /**
     * Safely extract a Map value from a map
     * @param map The source map
     * @param key The key to extract
     * @return The map value or null if not found or not a map
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> asMap(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof Map ? (Map<K, V>) value : null;
    }
    public static int asInt(Map<String, Object> map, String key) {
        return (int)(long)map.get(key);
    }
    public static Date asDateFromLong(Map<String, Object> map, String key) {
        if (map.containsKey(key)) {
            long value = asLong(map, key);
            return value > 0 ? new Date(value) : null;
        }
        return null;
    }

    public static LocalDate asLocalDateFromLong(Map<String, Object> map, String key) {
        if (map.containsKey(key)) {
            long value = asLong(map, key);
            return value > 0 ? LocalDate.ofEpochDay(value) : null;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
	public static <T> T asObjectFromMap(Map<String, Object> map, String key, RecordMapper<T> mapper) {
        if (map.containsKey(key)) {
            Object data = map.get(key);
            if (data != null) {
                return mapper.fromMap((Map<String, Object>)data, null, 0);
            }
        }
        return null;
    }

    public static byte[] asBlob(Map<String, Object> map, String key) {
        return (byte[])map.get(key);
    }

    public static MapBuilder buildMap() {
        return new MapBuilder();
    }

    public static class MapBuilder {
        private Map<String, Object> map = new HashMap<>();

        public MapBuilder add(String name, String value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, int value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, long value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, float value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, double value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, boolean value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, byte value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, byte[] value) {
            map.put(name, value);
            return this;
        }

        public MapBuilder add(String name, Enum<?> value) {
            map.put(name, value != null ? value.name() : null);
            return this;
        }

        public MapBuilder addAsLong(String name, Date value) {
            map.put(name, value == null ? 0L : value.getTime());
            return this;
        }

        public MapBuilder addAsLong(String name, LocalDate value) {
            map.put(name, value == null ? 0L : value.toEpochDay());
            return this;
        }

        public <T> MapBuilder add(String name, T obj, RecordMapper<T> mapper) {
            if (obj != null) {
                map.put(name, mapper.toMap(obj));
            }
            return this;
        }

        public <T> MapBuilder add(String name, List<T> objList, RecordMapper<T> mapper) {
            if (objList != null) {
                List<Map<String, Object>> data = new ArrayList<>();
                objList.forEach(item -> data.add(mapper.toMap(item)));
                map.put(name, data);
            }
            return this;
        }

        public MapBuilder add(String name, Map<?, ?> childMap) {
            map.put(name, childMap);
            return this;
        }

        public MapBuilder add(String name, List<?> childObjects) {
            map.put(name, childObjects);
            return this;
        }

        public Map<String, Object> done() {
            return map;
        }
    }
}
