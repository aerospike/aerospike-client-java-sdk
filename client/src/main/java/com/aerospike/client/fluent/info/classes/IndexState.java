package com.aerospike.client.fluent.info.classes;

public enum IndexState {
    WO, RW;

    public static IndexState fromString(String value) {
        return IndexState.valueOf(value.trim().toUpperCase());
    }
}