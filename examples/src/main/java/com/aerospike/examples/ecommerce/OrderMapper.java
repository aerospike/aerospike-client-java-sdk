package com.aerospike.examples.ecommerce;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.MapUtil;

public class OrderMapper implements RecordMapper<Order> {

    @Override
    public Order fromMap(Map<String, Object> map, Key key, int generation) {
        Order o = new Order();
        o.setOrderId(key.userKey.toString());
        o.setCustomerId(MapUtil.asString(map, "customerId"));
        o.setSku(MapUtil.asString(map, "sku"));
        o.setQty(MapUtil.asInt(map, "qty"));
        o.setTotalCents(MapUtil.asLong(map, "totalCents"));
        o.setStatus(MapUtil.asString(map, "status"));
        o.setTimestamp(MapUtil.asLong(map, "timestamp"));
        return o;
    }

    @Override
    public Map<String, Value> toMap(Order o) {
        Map<String, Value> map = new HashMap<>();
        map.put("customerId", Value.get(o.getCustomerId()));
        map.put("sku", Value.get(o.getSku()));
        map.put("qty", Value.get(o.getQty()));
        map.put("totalCents", Value.get(o.getTotalCents()));
        map.put("status", Value.get(o.getStatus()));
        map.put("timestamp", Value.get(o.getTimestamp()));
        return map;
    }

    @Override
    public Object id(Order o) {
        return o.getOrderId();
    }
}
