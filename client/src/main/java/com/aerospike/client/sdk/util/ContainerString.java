/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk.util;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.sdk.AerospikeList;
import com.aerospike.client.sdk.command.Buffer;

/**
 * Bounded formatting helper for public container {@code toString()} implementations.
 */
public final class ContainerString {
    public static final int DEFAULT_MAX_CHARS = 1000;

    private static final int MAX_BYTES = 64;
    private static final int MAX_ITEMS = 20;
    private static final int MAX_STRING_CHARS = 256;
    private static final String TRUNCATED = "...";

    private ContainerString() {
    }

    public static String format(Object value) {
        return format(value, DEFAULT_MAX_CHARS);
    }

    public static String format(Object value, int maxChars) {
        StringBuilder sb = new StringBuilder(Math.min(Math.max(maxChars, 0), 64));
        append(sb, value, maxChars);
        return sb.toString();
    }

    public static void append(StringBuilder sb, Object value, int maxChars) {
        appendValue(sb, value, new Context(maxChars));
    }

    public static String formatIterable(Iterable<?> values) {
        return formatIterable(values, DEFAULT_MAX_CHARS);
    }

    public static String formatIterable(Iterable<?> values, int maxChars) {
        StringBuilder sb = new StringBuilder(Math.min(Math.max(maxChars, 0), 64));
        Context ctx = new Context(maxChars);
        appendTracked(sb, values, "(this Collection)", ctx, () -> appendIterable(sb, values, ctx));
        return sb.toString();
    }

    public static String formatMap(Map<?,?> values) {
        return formatMap(values, DEFAULT_MAX_CHARS);
    }

    public static String formatMap(Map<?,?> values, int maxChars) {
        StringBuilder sb = new StringBuilder(Math.min(Math.max(maxChars, 0), 64));
        Context ctx = new Context(maxChars);
        appendTracked(sb, values, "(this Map)", ctx, () -> appendMap(sb, values, ctx));
        return sb.toString();
    }

    private static void appendValue(StringBuilder sb, Object value, Context ctx) {
        if (ctx.full(sb)) {
            return;
        }

        if (value == null) {
            appendLimited(sb, "null", ctx);
        }
        else if (value instanceof CharSequence cs) {
            appendLimited(sb, cap(cs.toString()), ctx);
        }
        else if (value instanceof byte[] bytes) {
            appendBytes(sb, bytes, ctx);
        }
        else if (value.getClass().isArray()) {
            appendArray(sb, value, ctx);
        }
        else if (value instanceof AerospikeList<?> list) {
            appendTracked(sb, value, "(this AerospikeList)", ctx, () -> appendAerospikeList(sb, list, ctx));
        }
        else if (value instanceof Map<?,?> map) {
            appendTracked(sb, value, "(this Map)", ctx, () -> appendMap(sb, map, ctx));
        }
        else if (value instanceof Iterable<?> iterable) {
            appendTracked(sb, value, "(this Collection)", ctx, () -> appendIterable(sb, iterable, ctx));
        }
        else {
            appendLimited(sb, cap(String.valueOf(value)), ctx);
        }
    }

    private static void appendAerospikeList(StringBuilder sb, AerospikeList<?> list, Context ctx) {
        appendLimited(sb, "AerospikeList{order=", ctx);
        appendValue(sb, list.getOrder(), ctx);
        appendLimited(sb, ", persistIndex=", ctx);
        appendValue(sb, list.isPersistIndex(), ctx);
        appendLimited(sb, ", values=", ctx);
        appendIterable(sb, list, ctx);
        appendLimited(sb, "}", ctx);
    }

    private static void appendBytes(StringBuilder sb, byte[] bytes, Context ctx) {
        appendLimited(sb, "bytes[len=", ctx);
        appendLimited(sb, Integer.toString(bytes.length), ctx);
        appendLimited(sb, ", hex=", ctx);
        int len = Math.min(bytes.length, MAX_BYTES);
        appendLimited(sb, Buffer.bytesToHexString(bytes, 0, len), ctx);
        if (len < bytes.length) {
            appendLimited(sb, TRUNCATED, ctx);
        }
        appendLimited(sb, "]", ctx);
    }

    private static void appendArray(StringBuilder sb, Object array, Context ctx) {
        appendTracked(sb, array, "(this Array)", ctx, () -> {
            appendLimited(sb, "[", ctx);
            int len = Array.getLength(array);
            int limit = Math.min(len, MAX_ITEMS);
            for (int i = 0; i < limit && !ctx.full(sb); i++) {
                if (i > 0) {
                    appendLimited(sb, ", ", ctx);
                }
                appendValue(sb, Array.get(array, i), ctx);
            }
            if (limit < len) {
                appendLimited(sb, limit > 0 ? ", ..." : "...", ctx);
            }
            appendLimited(sb, "]", ctx);
        });
    }

    private static void appendIterable(StringBuilder sb, Iterable<?> values, Context ctx) {
        appendLimited(sb, "[", ctx);
        Iterator<?> iterator = values.iterator();
        int count = 0;
        while (iterator.hasNext() && count < MAX_ITEMS && !ctx.full(sb)) {
            if (count > 0) {
                appendLimited(sb, ", ", ctx);
            }
            appendValue(sb, iterator.next(), ctx);
            count++;
        }
        if (iterator.hasNext()) {
            appendLimited(sb, count > 0 ? ", ..." : "...", ctx);
        }
        appendLimited(sb, "]", ctx);
    }

    private static void appendMap(StringBuilder sb, Map<?,?> values, Context ctx) {
        appendLimited(sb, "{", ctx);
        Iterator<? extends Map.Entry<?,?>> iterator = values.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext() && count < MAX_ITEMS && !ctx.full(sb)) {
            if (count > 0) {
                appendLimited(sb, ", ", ctx);
            }
            Map.Entry<?,?> entry = iterator.next();
            appendValue(sb, entry.getKey(), ctx);
            appendLimited(sb, "=", ctx);
            appendValue(sb, entry.getValue(), ctx);
            count++;
        }
        if (iterator.hasNext()) {
            appendLimited(sb, count > 0 ? ", ..." : "...", ctx);
        }
        appendLimited(sb, "}", ctx);
    }

    private static void appendTracked(StringBuilder sb, Object value, String cycleText, Context ctx, Runnable append) {
        if (!ctx.enter(value)) {
            appendLimited(sb, cycleText, ctx);
            return;
        }
        try {
            append.run();
        }
        finally {
            ctx.exit(value);
        }
    }

    private static void appendLimited(StringBuilder sb, String value, Context ctx) {
        if (ctx.maxChars <= 0 || sb.length() >= ctx.maxChars) {
            return;
        }
        int remaining = ctx.maxChars - sb.length();
        if (value.length() <= remaining) {
            sb.append(value);
        }
        else if (remaining <= TRUNCATED.length()) {
            sb.append(value, 0, remaining);
        }
        else {
            sb.append(value, 0, remaining - TRUNCATED.length());
            sb.append(TRUNCATED);
        }
    }

    private static String cap(String value) {
        return value.length() <= MAX_STRING_CHARS ? value : value.substring(0, MAX_STRING_CHARS) + TRUNCATED;
    }

    private static final class Context {
        private final int maxChars;
        private final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());

        private Context(int maxChars) {
            this.maxChars = maxChars;
        }

        private boolean full(StringBuilder sb) {
            return maxChars <= 0 || sb.length() >= maxChars;
        }

        private boolean enter(Object value) {
            return seen.add(value);
        }

        private void exit(Object value) {
            seen.remove(value);
        }
    }
}
