/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.client.sdk;

import java.util.List;
import java.util.function.Consumer;

/**
 * String read and modify operations on a nested string leaf.
 * The CDT path may have any number of steps, but the last selector must pick one
 * element ({@code onListIndex}, {@code onMapKey}, and similar), not a range.
 *
 * @param <T> parent builder returned after queuing the operation
 */
public interface StringContextBuilder<T> extends StringReadContextBuilder<T> {
    T insert(int index, String value);
    T insert(int index, String value, Consumer<StringWriteOptions> options);
    T insert(int index, String value, StringWriteOptions options);
    T overwrite(int index, String value);
    T overwrite(int index, String value, Consumer<StringWriteOptions> options);
    T overwrite(int index, String value, StringWriteOptions options);
    T concat(String fragment);
    T concat(String fragment, Consumer<StringWriteOptions> options);
    T concat(String fragment, StringWriteOptions options);
    T concat(List<String> fragments);
    T concat(List<String> fragments, Consumer<StringWriteOptions> options);
    T concat(List<String> fragments, StringWriteOptions options);
    T append(String fragment);
    T append(String fragment, Consumer<StringWriteOptions> options);
    T append(String fragment, StringWriteOptions options);
    T prepend(String fragment);
    T prepend(String fragment, Consumer<StringWriteOptions> options);
    T prepend(String fragment, StringWriteOptions options);
    T snip(int start);
    T snip(int start, int end);
    T snip(int start, int end, Consumer<StringWriteOptions> options);
    T snip(int start, int end, StringWriteOptions options);
    T replace(String needle, String replacement);
    T replace(String needle, String replacement, Consumer<StringWriteOptions> options);
    T replace(String needle, String replacement, StringWriteOptions options);
    T replaceAll(String needle, String replacement);
    T replaceAll(String needle, String replacement, Consumer<StringWriteOptions> options);
    T replaceAll(String needle, String replacement, StringWriteOptions options);
    T upper();
    T upper(Consumer<StringWriteOptions> options);
    T upper(StringWriteOptions options);
    T lower();
    T lower(Consumer<StringWriteOptions> options);
    T lower(StringWriteOptions options);
    T caseFold();
    T caseFold(Consumer<StringWriteOptions> options);
    T caseFold(StringWriteOptions options);
    T normalizeNfc();
    T normalizeNfc(Consumer<StringWriteOptions> options);
    T normalizeNfc(StringWriteOptions options);
    T trimStart();
    T trimStart(Consumer<StringWriteOptions> options);
    T trimStart(StringWriteOptions options);
    T trimEnd();
    T trimEnd(Consumer<StringWriteOptions> options);
    T trimEnd(StringWriteOptions options);
    T trim();
    T trim(Consumer<StringWriteOptions> options);
    T trim(StringWriteOptions options);
    T padStart(int targetLength, String padString);
    T padStart(int targetLength, String padString, Consumer<StringWriteOptions> options);
    T padStart(int targetLength, String padString, StringWriteOptions options);
    T padEnd(int targetLength, String padString);
    T padEnd(int targetLength, String padString, Consumer<StringWriteOptions> options);
    T padEnd(int targetLength, String padString, StringWriteOptions options);
    T repeat(int count);
    T repeat(int count, Consumer<StringWriteOptions> options);
    T repeat(int count, StringWriteOptions options);
    T regexReplace(String pattern, String replacement);
    T regexReplace(String pattern, String replacement, Consumer<StringWriteOptions> options);
    T regexReplace(String pattern, String replacement, StringWriteOptions options);
    T regexReplace(String pattern, String replacement, int regexFlags);
    T regexReplace(String pattern, String replacement, int regexFlags,
        Consumer<StringWriteOptions> options);
    T regexReplace(String pattern, String replacement, int regexFlags, StringWriteOptions options);
}
