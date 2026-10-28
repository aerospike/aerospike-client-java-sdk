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

import java.util.Objects;

/**
 * Immutable value object describing a {@code ORDER BY <bin> <ASC|DESC> LIMIT k} clause,
 * as set via {@link QueryBuilder#orderBy(String, OrderByType, Order)} and
 * {@link QueryBuilder#topK(int)}.
 *
 * @see QueryBuilder#getOrderBySpec()
 */
public final class OrderBySpec {
    private final String binName;
    private final OrderByType type;
    private final Order direction;
    private final int flags;

    public OrderBySpec(String binName, OrderByType type, Order direction, int flags) {
        this.binName = binName;
        this.type = type;
        this.direction = direction;
        this.flags = flags;
    }

    /**
     * The order-key bin name, as it appears in the returned record (physical or projected).
     */
    public String getBinName() {
        return binName;
    }

    /**
     * The declared scalar type of the order-key bin.
     */
    public OrderByType getType() {
        return type;
    }

    /**
     * The sort direction.
     */
    public Order getDirection() {
        return direction;
    }

    /**
     * Bitmask of {@link OrderByFlags}.
     */
    public int getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        return "OrderBySpec[binName=" + binName + ", type=" + type + ", direction=" + direction +
            ", flags=" + flags + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof OrderBySpec other)) {
            return false;
        }
        return flags == other.flags && Objects.equals(binName, other.binName) &&
            type == other.type && direction == other.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(binName, type, direction, flags);
    }
}
