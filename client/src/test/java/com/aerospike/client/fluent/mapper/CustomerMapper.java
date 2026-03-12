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
package com.aerospike.client.fluent.mapper;

import java.util.Map;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.MapUtil;

public class CustomerMapper implements RecordMapper<Customer> {
    @Override
    public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
        Customer result = new Customer();
        result.setId(recordKey.userKey.toLong());
        result.setAge(MapUtil.asInt(map, "age"));
        result.setDob(MapUtil.asDateFromLong(map, "dob"));
        result.setName(MapUtil.asString(map, "name"));
        result.setAddress(MapUtil.asObjectFromMap(map, "address", new AddressMapper()));
        return result;
    }

    @Override
    public Map<String, Value> toMap(Customer customer) {
        return MapUtil.buildMap()
            .add("id", customer.getId())
            .add("age", customer.getAge())
            .addAsLong("dob", customer.getDob())
            .add("name", customer.getName())
            .add("address", customer.getAddress(), new AddressMapper())
            .done();
    }

    @Override
    public Object id(Customer customer) {
        return customer.getId();
    }
}