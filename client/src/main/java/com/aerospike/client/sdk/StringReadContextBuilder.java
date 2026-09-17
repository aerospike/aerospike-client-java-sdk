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

/**
 * String read operations on a nested string leaf.
 * The CDT path may have any number of steps, but the last selector must pick one
 * element ({@code onListIndex}, {@code onMapKey}, and similar), not a range.
 *
 * @param <T> parent builder returned after queuing the operation
 */
public interface StringReadContextBuilder<T> {
    T strlen();
    T substr(int start);
    T substr(int start, int end);
    T charAt(int index);
    T find(String needle);
    T find(String needle, int occurrence);
    T contains(String needle);
    T startsWith(String prefix);
    T endsWith(String suffix);
    T stringToInteger();
    T stringToDouble();
    T byteLength();
    T isNumeric();
    T isNumeric(int numericType);
    T isUpper();
    T isLower();
    T stringToBlob();
    T split();
    T split(String separator);
    T b64Decode();
    T regexCompare(String pattern);
    T regexCompare(String pattern, int regexFlags);
}
