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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations
 * under the License.
 */
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.mapper.Customer;

/**
 * Unit tests for {@link TypedKeyList} factories (no cluster).
 */
public class TypedKeyListTest {

    private static final class OtherEntity {
    }

    @Test
    public void ofVarargs_homogeneous() {
        TypedDataSet<Customer> ds = TypedDataSet.of("ns", "customers", Customer.class);
        TypedKeyList<Customer> keys = TypedKeyList.of(ds.id(1), ds.id(2));
        assertEquals(2, keys.size());
        assertEquals(Customer.class, keys.get(0).getEntityClass());
    }

    @Test
    public void ofVarargs_emptyThrows() {
        assertThrows(IllegalArgumentException.class, () -> TypedKeyList.of());
    }

    @Test
    public void ofList_homogeneous() {
        TypedDataSet<Customer> ds = TypedDataSet.of("ns", "customers", Customer.class);
        TypedKeyList<Customer> keys = TypedKeyList.of(List.of(ds.id(1), ds.id(2)));
        assertEquals(2, keys.size());
    }

    @Test
    public void ofList_heterogeneousEntityClassThrows() {
        TypedDataSet<Customer> customers = TypedDataSet.of("ns", "c", Customer.class);
        TypedDataSet<OtherEntity> other = TypedDataSet.of("ns", "o", OtherEntity.class);
        assertThrows(IllegalArgumentException.class,
            () -> TypedKeyList.of(List.of(customers.id(1), other.id(1))));
    }

    @Test
    public void typedDataSetIdsReturnsTypedKeyList() {
        TypedDataSet<Customer> ds = TypedDataSet.of("ns", "customers", Customer.class);
        TypedKeyList<Customer> keys = ds.ids(1, 2, 3);
        assertEquals(3, keys.size());
    }
}
