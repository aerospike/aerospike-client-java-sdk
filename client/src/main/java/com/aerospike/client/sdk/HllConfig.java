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

/**
 * HyperLogLog bin configuration describing the index bit count and optional
 * minhash bit count. Used as input for {@code hllInit}/{@code hllAdd} operations
 * and returned by {@code hllDescribe} via {@link Record#getHllConfig(String)}.
 *
 * @param indexBitCount   number of index bits (4–16 inclusive)
 * @param minHashBitCount number of minhash bits (4–51 inclusive, or -1 for none).
 *                        indexBitCount + minHashBitCount must be ≤ 64.
 */
public record HllConfig(int indexBitCount, int minHashBitCount) {

    /**
     * Create a config with index bits only (no minhash).
     *
     * @param indexBitCount number of index bits (4–16)
     */
    public static HllConfig of(int indexBitCount) {
        return new HllConfig(indexBitCount, -1);
    }

    /**
     * Create a config with index bits and minhash bits.
     *
     * @param indexBitCount   number of index bits (4–16)
     * @param minHashBitCount number of minhash bits (4–51)
     */
    public static HllConfig of(int indexBitCount, int minHashBitCount) {
        return new HllConfig(indexBitCount, minHashBitCount);
    }
}
