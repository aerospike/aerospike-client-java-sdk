package com.aerospike.client.fluent.mapper;

import java.util.Map;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.util.MapUtil;

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
    public Map<String, Value> toMap(Address addr) {
        return MapUtil.buildMap()
                .add("line1", addr.getLine1())
                .add("city", addr.getCity())
                .add("state", addr.getState())
                .add("country", addr.getCountry())
                .add("zip", addr.getZipCode())
                .done();
    }

    @Override
    public Object id(Address element) {
        return null;
    }
}
