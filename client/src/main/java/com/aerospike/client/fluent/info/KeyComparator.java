package com.aerospike.client.fluent.info;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.fluent.info.annotations.Key;

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
