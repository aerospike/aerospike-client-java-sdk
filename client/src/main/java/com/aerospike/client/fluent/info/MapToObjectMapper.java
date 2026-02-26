package com.aerospike.client.fluent.info;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.info.annotations.Mapping;
import com.aerospike.client.fluent.info.annotations.Mappings;
import com.aerospike.client.fluent.info.annotations.Named;

/**
 * Rather than explicitly mapping a key / value from a map that is returned from an info
 * call to a field in the resulting object, introspection is used to perform this mapping.
 * This means to maintain the relationship from info call to objects, all that is needed
 * is to update the object.
 * <p/>
 * The rules used in the mapping are:
 * <ul>
 * <li>{@code @Named} allows an explicit name for a field to be given when the object doesn't match
 * what is presented. For example, if the name in the {@code info} response is {@code indexname} but
 * it needs to be mapped to a field called {@code indexName}:<p/>
 * <pre>
 *    @Named("indexname")
 *    private String indexName;
 *    </pre>
 * <li>Class level mappings. This are present at the start of a class to be mapped to and allow global,
 * regular expression replacement of the keys present in the info calls. For example, the info call
 * returns {@code storage-engine=device} but then there are other attributes which map to attributes
 * of {@code storage-engine}, so this has to be an object.
 * <p/>
 * Similarly {@code storage-engine.file[0]=<path-to-file>} should be mapped to a {@code storageEngine.files[0].filePath=<path-to-file>}.
 * Both of these mappings can be achieved using the following code
 * <pre>
 * @Mappings({
 *    @Mapping(from = "storage-engine", to = "storage-engine.type"),
 *   @Mapping(from = "storage-engine.file\\[(\\d+)\\]", to = "storage-engine.files[$1].filePath")
 * })
 * </pre>
 * </li>
 * <li>Names which include either _ or - are converted to camelCase. For example, "write-block-size"
 * will map to "writeBlockSize"</li>
 * <li>Names which include "["..."]" will map to an array. "storage-engine.file[0]" assumes there
 * is a {@code List<File> file} off {@code storageEngine}</li>
 * </ul>
 */
public class MapToObjectMapper {
    private static class ClassCacheEntry {
        private Map<String, Field> fieldMapping = new HashMap<>();
        private Map<Pattern, String> keyMapping = new HashMap<>();

        public ClassCacheEntry(Class<?> clazz) {
            // Add aggregated mappings
            Mappings mappings = clazz.getAnnotation(Mappings.class);
            if (mappings != null) {
                for (Mapping thisMapping : mappings.value()) {
                    this.keyMapping.put(Pattern.compile(thisMapping.from()), thisMapping.to());
                }
            }
            // Add individual mapping
            Mapping mapping = clazz.getAnnotation(Mapping.class);
            if (mapping != null) {
                this.keyMapping.put(Pattern.compile(mapping.from()), mapping.to());
            }
            for (Field field : getAllFields(clazz)) {
                field.setAccessible(true);
                String key = field.getName();

                Named named = field.getAnnotation(Named.class);
                if (named != null) {
                    key = named.value();
                    this.fieldMapping.put(key, field);
                }
                else {
                    // If the field is camel-cased store both the
                    // metric and config values
                    String configKey = fieldNameToStat(key, false);
                    this.fieldMapping.put(configKey, field);
                    if (!configKey.equals(key) ) {
                        this.fieldMapping.put(fieldNameToStat(key, true), field);
                    }
                }

            }
        }

        public Map<String, Field> getFieldMapping() {
            return fieldMapping;
        }

        public String translateKey(String input) {
            for (Entry<Pattern, String> entry : keyMapping.entrySet()) {
                Pattern pattern = entry.getKey();
                Matcher matcher = pattern.matcher(input);
                if (matcher.matches()) {
                    return matcher.replaceAll(entry.getValue());
                }
            }
            return input; // return original if no match
        }
    }

    // Field cache per class
    private static final Map<Class<?>, ClassCacheEntry> fieldCache = new ConcurrentHashMap<>();

    private static class PathToken {
        String fieldName;
        Integer index;

        PathToken(String fieldName, Integer index) {
            this.fieldName = fieldName;
            this.index = index;
        }
    }

    private static PathToken parseToken(String token) {
        if (token.matches(".*\\[\\d+\\]")) {
            int bracketStart = token.indexOf('[');
            String fieldName = token.substring(0, bracketStart);
            int index = Integer.parseInt(token.substring(bracketStart + 1, token.length() - 1));
            return new PathToken(fieldName, index);
        }
        return new PathToken(token, null);
    }

    private static Class<?> getGenericListType(Field field) {
        ParameterizedType type = (ParameterizedType) field.getGenericType();
        return (Class<?>) type.getActualTypeArguments()[0];
    }

    /**
     * Converts a string with underscores or hyphens into camelCase.
     * Examples:
     *   "max_bytes" -> "maxBytes"
     *   "min-bytes" -> "minBytes"
     *
     * @param input the input string
     * @return the camelCase version of the string
     */
    public static String toCamelCase(String input) {
        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = false;

        for (char ch : input.toCharArray()) {
            if (ch == '_' || ch == '-') {
                capitalizeNext = true;
            } else if (capitalizeNext) {
                result.append(Character.toUpperCase(ch));
                capitalizeNext = false;
            } else {
                result.append(ch);
            }
        }

        return result.toString();
    }

    /**
     * Converts a camelCase field name into a delimited stat name.
     * Underscores (_) are used if isMetric is true, otherwise dashes (-) are used.
     *
     * Examples:
     *   fieldNameToStat("userLoginCount", true)  -> "user_login_count"
     *   fieldNameToStat("totalErrorsFound", false) -> "total-errors-found"
     *
     * @param fieldName the camelCase field name
     * @param isMetric true to use underscores, false to use dashes
     * @return the converted stat name
     */
    public static String fieldNameToStat(String fieldName, boolean isMetric) {
        if (fieldName == null || fieldName.isEmpty()) {
            return fieldName;
        }

        String delimiter = isMetric ? "_" : "-";

        // Insert delimiter before uppercase letters (except at the start), then convert to lowercase
        String result = fieldName
                .replaceAll("(?<!^)([A-Z])", delimiter + "$1")
                .toLowerCase();

        return result;
    }

    public static <T> T mapToObject(Map<String, String> data, Class<T> clazz) {
        try {
            T instance = clazz.getDeclaredConstructor().newInstance();
            ClassCacheEntry cacheEntry = getFieldMapping(clazz);

            for (Map.Entry<String, String> entry : data.entrySet()) {
                String entryKey = cacheEntry.translateKey(entry.getKey());
                try {
                    setNestedFieldValue(instance, entryKey, entry.getValue());
                } catch (Exception e) {
                    handleUnknownKey(instance, entryKey, entry.getValue());
                }
            }
            return instance;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException e) {

            if (Log.warnEnabled()) {
                Log.warn(String.format("Error mapping data from object to a class of type %s: %s (%s)",
                        clazz.getName(),
                        e.getMessage(),
                        e.getClass().getName()));
                if (Log.debugEnabled()) {
                    e.printStackTrace();
                }
            }
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void setNestedFieldValue(Object root, String path, String stringValue) throws Exception {
        String[] tokens = path.split("\\.");
        Object current = root;
        Class<?> currentClass = root.getClass();

        for (int i = 0; i < tokens.length - 1; i++) {
            ClassCacheEntry cacheEntry = getFieldMapping(currentClass);

            PathToken token = parseToken(tokens[i]);
            // Do not translate the token if we're on the root class, it has already been translated
            String mappedValue = (root.getClass().equals(currentClass)) ? token.fieldName : cacheEntry.translateKey(token.fieldName);

            Field field = cacheEntry.getFieldMapping().get(mappedValue);
            if (field == null) {
                field = currentClass.getDeclaredField(toCamelCase(mappedValue));
            }
            field.setAccessible(true);

            Object next = field.get(current);
            if (next == null) {
                next = field.getType().isAssignableFrom(List.class) ? new ArrayList<>() : field.getType().getDeclaredConstructor().newInstance();
                field.set(current, next);
            }

            if (token.index != null) {
                List<?> list = (List<?>) next;
                while (list.size() <= token.index) {
                    list.add(null);
                }

                Object element = list.get(token.index);
                if (element == null) {
                    Class<?> elementType = getGenericListType(field);
                    element = elementType.getDeclaredConstructor().newInstance();
                    ((List<Object>) list).set(token.index, element);
                }
                current = element;
                currentClass = element.getClass();
            } else {
                current = next;
                currentClass = field.getType();
            }
        }

        // Set the final value
        ClassCacheEntry finalEntry = getFieldMapping(currentClass);

        PathToken finalToken = parseToken(tokens[tokens.length - 1]);
        String fieldName = (root.getClass().equals(currentClass)) ? finalToken.fieldName : finalEntry.translateKey(finalToken.fieldName);

        Field finalField = finalEntry.getFieldMapping().get(fieldName);
        if (finalField == null) {
            finalField = currentClass.getDeclaredField(toCamelCase(fieldName));
        }
        finalField.setAccessible(true);
        Object converted = convertString(stringValue, finalField.getType());
        finalField.set(current, converted);
    }


    private static void handleUnknownKey(Object target, String key, String value) {
        if (Log.infoEnabled()) {
            Log.info(String.format("Unknown key encountered: %s = %s (target class: %s)",
                    key, value, target.getClass().getSimpleName()));
        }
    }

    private static ClassCacheEntry getFieldMapping(Class<?> clazz) {
        return fieldCache.computeIfAbsent(clazz, cls -> {
            return new ClassCacheEntry(clazz);
        });
    }

    private static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            fields.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    private static Object convertString(String value, Class<?> targetType) {
        if (targetType == String.class) {
			return value;
		}
        if (targetType == int.class || targetType == Integer.class) {
			return Integer.parseInt(value);
		}
        if (targetType == long.class || targetType == Long.class) {
			return Long.parseLong(value);
		}
        if (targetType == float.class || targetType == Float.class) {
			return Float.parseFloat(value);
		}
        if (targetType == double.class || targetType == Double.class) {
			return Double.parseDouble(value);
		}
        if (targetType == boolean.class || targetType == Boolean.class) {
			return Boolean.parseBoolean(value);
		}

        if (targetType.isEnum()) {
            @SuppressWarnings("rawtypes")
            Class<? extends Enum> enumClass = targetType.asSubclass(Enum.class);
            return Enum.valueOf(enumClass, value.replaceAll("-", "_").toUpperCase());
        }

        throw new IllegalArgumentException("Unsupported type: " + targetType.getName());
    }
}
