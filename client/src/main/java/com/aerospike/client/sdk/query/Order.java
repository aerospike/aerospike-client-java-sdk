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

/**
 * Sort direction for {@link QueryBuilder#orderBy(String, OrderByType, Order)}.
 *
 * <p>A record whose order-key bin is missing or type-mismatched (see {@link OrderByType})
 * ranks last regardless of direction: largest in {@link #ASC}, smallest in {@link #DESC}.</p>
 */
public enum Order {
    /** Ascending: {@link QueryBuilder#topK(int)} keeps the {@code k} smallest values. */
    ASC(0),

    /** Descending: {@link QueryBuilder#topK(int)} keeps the {@code k} largest values. */
    DESC(1);

    private final int wireCode;

    Order(int wireCode) {
        this.wireCode = wireCode;
    }

    /**
     * The single-byte wire code sent in the {@code ORDER_BY} message field's {@code direction} slot.
     */
    public int getWireCode() {
        return wireCode;
    }
}
