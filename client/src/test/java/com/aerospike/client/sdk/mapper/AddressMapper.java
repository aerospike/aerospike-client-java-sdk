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
package com.aerospike.client.sdk.mapper;

import java.util.Map;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.util.MapUtil;

public class AddressMapper implements RecordMapper<Address> {
    @Override
    public Address fromMap(Map<String, Object> map, Key recordKey, int generation) {
        Address result = new Address(
            MapUtil.asString(map, "line1"),
            MapUtil.asString(map, "city"),
            MapUtil.asString(map, "state"),
            MapUtil.asString(map, "country"),
            MapUtil.asString(map, "zip"));
        return result;
    }

    @Override
    public Map<String, Object> toMap(Address addr) {
        return MapUtil.buildMap().add("line1", addr.getLine1()).add("city", addr.getCity()).add(
            "state", addr.getState()).add("country", addr.getCountry()).add("zip",
            addr.getZipCode()).done();
    }

    @Override
    public Object id(Address element) {
        return null;
    }
}
