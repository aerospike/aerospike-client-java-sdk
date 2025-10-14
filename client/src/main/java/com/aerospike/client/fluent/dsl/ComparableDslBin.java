package com.aerospike.client.fluent.dsl;

public interface ComparableDslBin<T extends Comparable<T>> extends DslBin<T> {
    Comparison<T> gt(T value);
    Comparison<T> lt(T value);
    Comparison<T> gte(T value);
    Comparison<T> lte(T value);

    Comparison<T> gt(DslBin<T> other);
    Comparison<T> lt(DslBin<T> other);
    Comparison<T> gte(DslBin<T> other);
    Comparison<T> lte(DslBin<T> other);

    ArithmeticExpression add(Number value);
    ArithmeticExpression sub(Number value);
    ArithmeticExpression mul(Number value);
    ArithmeticExpression div(Number value);

    ArithmeticExpression add(DslExpression other);
    ArithmeticExpression sub(DslExpression other);
    ArithmeticExpression mul(DslExpression other);
    ArithmeticExpression div(DslExpression other);
}
