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
package com.aerospike.client.sdk.query;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.sdk.AerospikeComparator;
import com.aerospike.client.sdk.command.Buffer;

/**
 * Client-side substitution of {@code ?0}, {@code ?1}, ... placeholders in AEL templates.
 * Bound values are formatted as valid AEL literals before the expression is sent to the server.
 */
public final class AelPlaceholderBinder {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");
    private static final Pattern REMAINING_PLACEHOLDER = Pattern.compile("\\?\\d+");

    private AelPlaceholderBinder() {
    }

    /**
     * Replace {@code ?N} placeholders (zero-based) with formatted AEL literals.
     *
     * @param template AEL template containing {@code ?0}, {@code ?1}, ...
     * @param params   bound parameter values
     * @return AEL string with all placeholders substituted
     */
    public static String bind(String template, Object... params) {
        if (template == null) {
            throw new IllegalArgumentException("AEL template must not be null");
        }
        Object[] values = params != null ? params : new Object[0];

        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            if (index < 0 || index >= values.length || values[index] == null) {
                throw new IllegalArgumentException("Missing value for placeholder ?" + index);
            }
            String replacement = formatLiteral(values[index]);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(sb);
        String result = sb.toString();

        if (REMAINING_PLACEHOLDER.matcher(result).find()) {
            throw new IllegalArgumentException("Unsubstituted placeholders remain in AEL template");
        }

        return result;
    }

    /**
     * Format a Java value as an AEL literal suitable for substitution into an expression.
     */
    public static String formatLiteral(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot format null as AEL literal");
        }
        if (value instanceof String s) {
            return formatString(s);
        }
        if (value instanceof Boolean b) {
            return b ? "true" : "false";
        }
        if (value instanceof Integer || value instanceof Long) {
            return value.toString();
        }
        if (value instanceof Float || value instanceof Double) {
            return formatFloat(((Number) value).doubleValue());
        }
        if (value instanceof byte[] bytes) {
            return "X'" + Buffer.bytesToHexString(bytes) + "'";
        }
        if (value instanceof List<?> list) {
            return formatList(list);
        }
        if (value instanceof Map<?, ?> map) {
            return formatMap(map);
        }
        throw new UnsupportedOperationException(
                "Cannot format value of type " + value.getClass().getSimpleName() + " as AEL literal");
    }

    private static String formatString(String value) {
        if (!value.contains("'")) {
            return "'" + value + "'";
        }
        if (!value.contains("\"")) {
            return "\"" + value + "\"";
        }
        throw new IllegalArgumentException("String cannot be quoted for AEL: contains both ' and \"");
    }

    private static String formatFloat(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("Cannot format non-finite float value: " + value);
        }

        String plain = BigDecimal.valueOf(value).toPlainString();
        if (plain.indexOf('e') >= 0 || plain.indexOf('E') >= 0) {
            throw new IllegalArgumentException("Float literal must not use exponent notation: " + plain);
        }

        if (!plain.contains(".")) {
            plain = plain + ".0";
        }
        else if (plain.endsWith(".")) {
            plain = plain + "0";
        }

        return plain;
    }

    private static String formatList(List<?> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(formatLiteral(list.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String formatMap(Map<?, ?> map) {
        SortedMap<Object, Object> sorted = toSortedMap(map);
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Object, Object> entry : sorted.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(formatMapKey(entry.getKey())).append(": ").append(formatLiteral(entry.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    private static SortedMap<Object, Object> toSortedMap(Map<?, ?> map) {
        if (map instanceof SortedMap<?, ?> sortedMap) {
            @SuppressWarnings("unchecked")
            SortedMap<Object, Object> objectMap = (SortedMap<Object, Object>) sortedMap;
            return objectMap;
        }
        SortedMap<Object, Object> sorted = new TreeMap<>(new AerospikeComparator());
        @SuppressWarnings("unchecked")
        Map<Object, Object> objectMap = (Map<Object, Object>) map;
        sorted.putAll(objectMap);
        return sorted;
    }

    private static String formatMapKey(Object key) {
        if (key instanceof String || key instanceof Integer || key instanceof Long || key instanceof byte[]) {
            return formatLiteral(key);
        }
        throw new UnsupportedOperationException(
                "Map key type not supported for AEL literal: " + key.getClass().getSimpleName());
    }
}
