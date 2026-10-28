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
 * Bitmask flags for {@link QueryBuilder#orderBy(String, OrderByType, Order, int)}.
 *
 * <p>Room for future per-type modifiers, same shape as {@code ExpReadFlags}/{@code ExpWriteFlags}.</p>
 */
public final class OrderByFlags {
    /** No flags. */
    public static final int NONE = 0;

    /**
     * Fold ASCII case before comparing. Valid only when the order-by bin's declared
     * {@link OrderByType} is {@link OrderByType#STRING}.
     */
    public static final int CASE_INSENSITIVE = 1;

    private OrderByFlags() {
    }
}
