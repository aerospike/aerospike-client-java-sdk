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

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordMapper;
import com.aerospike.client.sdk.util.MapUtil;

public class ProductMapper implements RecordMapper<Product> {

    @Override
    public Product fromMap(Map<String, Object> map, Key key, int generation) {
        Product p = new Product();
        p.setSku(key.userKey.toString());
        p.setName(MapUtil.asString(map, "name"));
        p.setPriceCents(MapUtil.asLong(map, "price"));
        p.setStockQty(MapUtil.asInt(map, "stock"));
        p.setSalePriceCents(MapUtil.asLong(map, "salePrice"));
        return p;
    }

    @Override
    public Map<String, Object> toMap(Product p) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", p.getName());
        map.put("price", p.getPriceCents());
        map.put("stock", p.getStockQty());
        map.put("salePrice", p.getSalePriceCents());
        return map;
    }

    @Override
    public Object id(Product p) {
        return p.getSku();
    }
}
