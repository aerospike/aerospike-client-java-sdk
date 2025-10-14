package com.aerospike.client.fluent.dsl;

public interface DslBin<T> {
    Comparison<T> eq(T value);
    Comparison<T> ne(T value);
    Comparison<T> eq(DslBin<T> other);
    Comparison<T> ne(DslBin<T> other);
}
