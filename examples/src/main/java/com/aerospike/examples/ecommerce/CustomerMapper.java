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
