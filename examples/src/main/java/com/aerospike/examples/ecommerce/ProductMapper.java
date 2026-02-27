package com.aerospike.examples.ecommerce;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.MapUtil;

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
    public Map<String, Value> toMap(Product p) {
        Map<String, Value> map = new HashMap<>();
        map.put("name", Value.get(p.getName()));
        map.put("price", Value.get(p.getPriceCents()));
        map.put("stock", Value.get(p.getStockQty()));
        map.put("salePrice", Value.get(p.getSalePriceCents()));
        return map;
    }

    @Override
    public Object id(Product p) {
        return p.getSku();
    }
}
