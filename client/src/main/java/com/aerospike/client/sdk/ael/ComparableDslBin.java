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
package com.aerospike.client.sdk.ael;

public interface ComparableDslBin<T extends Comparable<T>> extends AelBin<T> {
    Comparison<T> gt(T value);
    Comparison<T> lt(T value);
    Comparison<T> gte(T value);
    Comparison<T> lte(T value);

    Comparison<T> gt(AelBin<T> other);
    Comparison<T> lt(AelBin<T> other);
    Comparison<T> gte(AelBin<T> other);
    Comparison<T> lte(AelBin<T> other);

    ArithmeticExpression add(Number value);
    ArithmeticExpression sub(Number value);
    ArithmeticExpression mul(Number value);
    ArithmeticExpression div(Number value);

    ArithmeticExpression add(AelExpression other);
    ArithmeticExpression sub(AelExpression other);
    ArithmeticExpression mul(AelExpression other);
    ArithmeticExpression div(AelExpression other);
}
