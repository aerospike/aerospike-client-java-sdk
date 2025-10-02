package com.aerospike.client.fluent.info.classes;

public enum IndexType {
    STRING, NUMERIC, GEOJSON, BLOB, MAPVALUES, MAPKEYS, LISTINDEXES;

    public static IndexType fromString(String value) {
        return IndexType.valueOf(value.trim().toUpperCase());
    }
}