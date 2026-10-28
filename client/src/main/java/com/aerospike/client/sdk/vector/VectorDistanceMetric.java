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
package com.aerospike.client.sdk.vector;

import com.aerospike.client.sdk.query.Order;

/**
 * Distance metric used to compare two vectors in a vector distance expression.
 * See {@link com.aerospike.client.sdk.exp.VectorExp#distance}.
 * <p>
 * Euclidean distance is returned as squared L2 distance. Dot product and cosine
 * are similarity metrics, while Euclidean is a distance metric.
 */
public enum VectorDistanceMetric {
    /**
     * Squared Euclidean (L2²) distance. Smaller values indicate closer vectors.
     */
    EUCLIDEAN_SQUARED(0, Order.ASC),

    /**
     * Dot product. Larger values indicate more similar vectors.
     */
    DOT_PRODUCT(1, Order.DESC),

    /**
     * Cosine similarity. Larger values indicate closer vectors.
     */
    COSINE_SIMILARITY(2, Order.DESC);

    private final int code;
    private final Order order;

    VectorDistanceMetric(final int code, final Order order) {
        this.code = code;
        this.order = order;
    }

    /**
     * Return the wire protocol code for this metric.
     */
    public int getCode() {
        return code;
    }

    /**
     * Return nearest order for this metric.
     */
    public Order nearestOrder() {
        return order;
    }
}
