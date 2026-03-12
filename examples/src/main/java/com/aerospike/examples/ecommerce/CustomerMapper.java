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
package com.aerospike.examples.ecommerce;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.MapUtil;

public class CustomerMapper implements RecordMapper<Customer> {

    @Override
    public Customer fromMap(Map<String, Object> map, Key key, int generation) {
        Customer c = new Customer();
        c.setId(key.userKey.toString());
        c.setName(MapUtil.asString(map, "name"));
        c.setEmail(MapUtil.asString(map, "email"));
        c.setCreditLimitCents(MapUtil.asLong(map, "creditLimit"));
        c.setBalanceCents(MapUtil.asLong(map, "balance"));
        return c;
    }

    @Override
    public Map<String, Value> toMap(Customer c) {
        Map<String, Value> map = new HashMap<>();
        map.put("name", Value.get(c.getName()));
        map.put("email", Value.get(c.getEmail()));
        map.put("creditLimit", Value.get(c.getCreditLimitCents()));
        map.put("balance", Value.get(c.getBalanceCents()));
        return map;
    }

    @Override
    public Object id(Customer c) {
        return c.getId();
    }
}
