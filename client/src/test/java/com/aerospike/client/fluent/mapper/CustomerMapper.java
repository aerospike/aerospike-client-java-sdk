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