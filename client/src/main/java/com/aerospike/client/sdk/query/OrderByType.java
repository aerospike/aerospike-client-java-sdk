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
 * Declared scalar type of a {@link QueryBuilder#orderBy(String, OrderByType, Order)} bin.
 *
 * <p>Aerospike bins have no schema, so the server needs this declared type to pick a
 * comparator. A record whose bin is absent or holds a different type ranks last.</p>
 */
public enum OrderByType {
    INTEGER(1),
    DOUBLE(2),
    STRING(3),
    BYTES(4);

    private final int wireCode;

    OrderByType(int wireCode) {
        this.wireCode = wireCode;
    }

    /** The single-byte wire code sent in the {@code ORDER_BY} field's {@code type} slot. */
    public int getWireCode() {
        return wireCode;
    }
}
