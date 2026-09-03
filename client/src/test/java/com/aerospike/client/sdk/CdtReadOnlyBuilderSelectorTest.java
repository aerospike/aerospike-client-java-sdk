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
package com.aerospike.client.sdk;

import static com.aerospike.client.sdk.CdtOperationCapture.BIN;
import static com.aerospike.client.sdk.CdtOperationCapture.ROOT_CTX;
import static com.aerospike.client.sdk.CdtOperationCapture.assertOperation;
import static com.aerospike.client.sdk.CdtOperationCapture.emit;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;

/**
 * Covers every {@code on*} selector overload on {@link CdtReadOnlyBuilder} — the query-path CDT
 * navigator — by driving the builder with a capturing {@link CdtOperationAcceptor} and comparing the
 * emitted {@link Operation} against the equivalent direct {@code MapOperation} / {@code ListOperation}
 * call. No cluster is required.
 *
 * <p>These selectors are a typed cross-product: {@code onMapValueRange} alone has 22 overloads whose
 * bodies differ only in which {@code Value.get} the compiler selects. Each case here pins the routing
 * (selector to {@link CdtOperation}), the argument conversion, and the argument order. Server-side
 * semantics of each selector belong in the integration suites, not here.</p>
 *
 * <p>Start and end arguments are always distinct so a transposed pair fails.</p>
 */
class CdtReadOnlyBuilderSelectorTest {

    private static final byte[] BYTES_START = { 1, 2 };
    private static final byte[] BYTES_END = { 3, 4 };
    private static final List<?> LIST_START = List.of(1L);
    private static final List<?> LIST_END = List.of(2L);
    private static final Map<?, ?> MAP_START = Map.of("a", 1L);
    private static final Map<?, ?> MAP_END = Map.of("b", 2L);
    private static final SpecialValue SV_START = SpecialValue.NULL;
    private static final SpecialValue SV_END = SpecialValue.INFINITY;

    private static final int RANK = 3;
    private static final int COUNT = 2;

    @ParameterizedTest(name = "{0}")
    @MethodSource({
        "mapValueRangeOverloads",
        "listValueRangeOverloads",
        "mapValueRelativeRankRangeOverloads",
        "listValueRelativeRankRangeOverloads",
        "mapKeyRangeOverloads",
        "mapValueOverloads",
        "listValueOverloads",
        "mapKeyRelativeIndexRangeOverloads",
        "positionalSelectors",
    })
    void selectorEmitsExpectedOperation(String label, Consumer<CdtReadOnlyBuilder<Object>> chain, Operation expected) {
        assertOperation(emit(chain), expected);
    }

    private static Arguments selector(String label, Consumer<CdtReadOnlyBuilder<Object>> chain, Operation expected) {
        return Arguments.of(label, chain, expected);
    }

    // ========================================
    // Expected-operation shorthands
    // ========================================

    private static Operation mapValueRange(Value begin, Value end) {
        return MapOperation.getByValueRange(BIN, begin, end, MapReturnType.VALUE, ROOT_CTX);
    }

    private static Operation listValueRange(Value begin, Value end) {
        return ListOperation.getByValueRange(BIN, begin, end, ListReturnType.VALUE, ROOT_CTX);
    }

    private static Operation mapKeyRange(Value begin, Value end) {
        return MapOperation.getByKeyRange(BIN, begin, end, MapReturnType.VALUE, ROOT_CTX);
    }

    private static Operation mapValueRelRank(Value value) {
        return MapOperation.getByValueRelativeRankRange(BIN, value, RANK, MapReturnType.VALUE, ROOT_CTX);
    }

    private static Operation mapValueRelRankCounted(Value value) {
        return MapOperation.getByValueRelativeRankRange(BIN, value, RANK, COUNT, MapReturnType.VALUE, ROOT_CTX);
    }

    private static Operation listValueRelRank(Value value) {
        return ListOperation.getByValueRelativeRankRange(BIN, value, RANK, ListReturnType.VALUE, ROOT_CTX);
    }

    private static Operation listValueRelRankCounted(Value value) {
        return ListOperation.getByValueRelativeRankRange(BIN, value, RANK, COUNT, ListReturnType.VALUE, ROOT_CTX);
    }

    // ========================================
    // onMapValueRange — 22 overloads
    // ========================================

    private static Stream<Arguments> mapValueRangeOverloads() {
        return Stream.of(
            selector("onMapValueRange(long,long)",
                    b -> b.onMapValueRange(1L, 4L).getValues(),
                    mapValueRange(Value.get(1L), Value.get(4L))),
            selector("onMapValueRange(String,String)",
                    b -> b.onMapValueRange("a", "b").getValues(),
                    mapValueRange(Value.get("a"), Value.get("b"))),
            selector("onMapValueRange(byte[],byte[])",
                    b -> b.onMapValueRange(BYTES_START, BYTES_END).getValues(),
                    mapValueRange(Value.get(BYTES_START), Value.get(BYTES_END))),
            selector("onMapValueRange(double,double)",
                    b -> b.onMapValueRange(1.5d, 2.5d).getValues(),
                    mapValueRange(Value.get(1.5d), Value.get(2.5d))),
            selector("onMapValueRange(boolean,boolean)",
                    b -> b.onMapValueRange(false, true).getValues(),
                    mapValueRange(Value.get(false), Value.get(true))),
            selector("onMapValueRange(List,List)",
                    b -> b.onMapValueRange(LIST_START, LIST_END).getValues(),
                    mapValueRange(Value.get(LIST_START), Value.get(LIST_END))),
            selector("onMapValueRange(Map,Map)",
                    b -> b.onMapValueRange(MAP_START, MAP_END).getValues(),
                    mapValueRange(Value.get(MAP_START), Value.get(MAP_END))),
            selector("onMapValueRange(SpecialValue,SpecialValue)",
                    b -> b.onMapValueRange(SV_START, SV_END).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), SV_END.toAerospikeValue())),
            selector("onMapValueRange(SpecialValue,long)",
                    b -> b.onMapValueRange(SV_START, 4L).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get(4L))),
            selector("onMapValueRange(SpecialValue,String)",
                    b -> b.onMapValueRange(SV_START, "b").getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get("b"))),
            selector("onMapValueRange(SpecialValue,byte[])",
                    b -> b.onMapValueRange(SV_START, BYTES_END).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get(BYTES_END))),
            selector("onMapValueRange(SpecialValue,double)",
                    b -> b.onMapValueRange(SV_START, 2.5d).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get(2.5d))),
            selector("onMapValueRange(SpecialValue,boolean)",
                    b -> b.onMapValueRange(SV_START, true).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get(true))),
            selector("onMapValueRange(SpecialValue,List)",
                    b -> b.onMapValueRange(SV_START, LIST_END).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get(LIST_END))),
            selector("onMapValueRange(SpecialValue,Map)",
                    b -> b.onMapValueRange(SV_START, MAP_END).getValues(),
                    mapValueRange(SV_START.toAerospikeValue(), Value.get(MAP_END))),
            selector("onMapValueRange(long,SpecialValue)",
                    b -> b.onMapValueRange(1L, SV_END).getValues(),
                    mapValueRange(Value.get(1L), SV_END.toAerospikeValue())),
            selector("onMapValueRange(String,SpecialValue)",
                    b -> b.onMapValueRange("a", SV_END).getValues(),
                    mapValueRange(Value.get("a"), SV_END.toAerospikeValue())),
            selector("onMapValueRange(byte[],SpecialValue)",
                    b -> b.onMapValueRange(BYTES_START, SV_END).getValues(),
                    mapValueRange(Value.get(BYTES_START), SV_END.toAerospikeValue())),
            selector("onMapValueRange(double,SpecialValue)",
                    b -> b.onMapValueRange(1.5d, SV_END).getValues(),
                    mapValueRange(Value.get(1.5d), SV_END.toAerospikeValue())),
            selector("onMapValueRange(boolean,SpecialValue)",
                    b -> b.onMapValueRange(false, SV_END).getValues(),
                    mapValueRange(Value.get(false), SV_END.toAerospikeValue())),
            selector("onMapValueRange(List,SpecialValue)",
                    b -> b.onMapValueRange(LIST_START, SV_END).getValues(),
                    mapValueRange(Value.get(LIST_START), SV_END.toAerospikeValue())),
            selector("onMapValueRange(Map,SpecialValue)",
                    b -> b.onMapValueRange(MAP_START, SV_END).getValues(),
                    mapValueRange(Value.get(MAP_START), SV_END.toAerospikeValue())));
    }

    // ========================================
    // onListValueRange — 22 overloads
    // ========================================

    private static Stream<Arguments> listValueRangeOverloads() {
        return Stream.of(
            selector("onListValueRange(long,long)",
                    b -> b.onListValueRange(1L, 4L).getValues(),
                    listValueRange(Value.get(1L), Value.get(4L))),
            selector("onListValueRange(String,String)",
                    b -> b.onListValueRange("a", "b").getValues(),
                    listValueRange(Value.get("a"), Value.get("b"))),
            selector("onListValueRange(byte[],byte[])",
                    b -> b.onListValueRange(BYTES_START, BYTES_END).getValues(),
                    listValueRange(Value.get(BYTES_START), Value.get(BYTES_END))),
            selector("onListValueRange(double,double)",
                    b -> b.onListValueRange(1.5d, 2.5d).getValues(),
                    listValueRange(Value.get(1.5d), Value.get(2.5d))),
            selector("onListValueRange(boolean,boolean)",
                    b -> b.onListValueRange(false, true).getValues(),
                    listValueRange(Value.get(false), Value.get(true))),
            selector("onListValueRange(List,List)",
                    b -> b.onListValueRange(LIST_START, LIST_END).getValues(),
                    listValueRange(Value.get(LIST_START), Value.get(LIST_END))),
            selector("onListValueRange(Map,Map)",
                    b -> b.onListValueRange(MAP_START, MAP_END).getValues(),
                    listValueRange(Value.get(MAP_START), Value.get(MAP_END))),
            selector("onListValueRange(SpecialValue,SpecialValue)",
                    b -> b.onListValueRange(SV_START, SV_END).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), SV_END.toAerospikeValue())),
            selector("onListValueRange(SpecialValue,long)",
                    b -> b.onListValueRange(SV_START, 4L).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get(4L))),
            selector("onListValueRange(SpecialValue,String)",
                    b -> b.onListValueRange(SV_START, "b").getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get("b"))),
            selector("onListValueRange(SpecialValue,byte[])",
                    b -> b.onListValueRange(SV_START, BYTES_END).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get(BYTES_END))),
            selector("onListValueRange(SpecialValue,double)",
                    b -> b.onListValueRange(SV_START, 2.5d).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get(2.5d))),
            selector("onListValueRange(SpecialValue,boolean)",
                    b -> b.onListValueRange(SV_START, true).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get(true))),
            selector("onListValueRange(SpecialValue,List)",
                    b -> b.onListValueRange(SV_START, LIST_END).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get(LIST_END))),
            selector("onListValueRange(SpecialValue,Map)",
                    b -> b.onListValueRange(SV_START, MAP_END).getValues(),
                    listValueRange(SV_START.toAerospikeValue(), Value.get(MAP_END))),
            selector("onListValueRange(long,SpecialValue)",
                    b -> b.onListValueRange(1L, SV_END).getValues(),
                    listValueRange(Value.get(1L), SV_END.toAerospikeValue())),
            selector("onListValueRange(String,SpecialValue)",
                    b -> b.onListValueRange("a", SV_END).getValues(),
                    listValueRange(Value.get("a"), SV_END.toAerospikeValue())),
            selector("onListValueRange(byte[],SpecialValue)",
                    b -> b.onListValueRange(BYTES_START, SV_END).getValues(),
                    listValueRange(Value.get(BYTES_START), SV_END.toAerospikeValue())),
            selector("onListValueRange(double,SpecialValue)",
                    b -> b.onListValueRange(1.5d, SV_END).getValues(),
                    listValueRange(Value.get(1.5d), SV_END.toAerospikeValue())),
            selector("onListValueRange(boolean,SpecialValue)",
                    b -> b.onListValueRange(false, SV_END).getValues(),
                    listValueRange(Value.get(false), SV_END.toAerospikeValue())),
            selector("onListValueRange(List,SpecialValue)",
                    b -> b.onListValueRange(LIST_START, SV_END).getValues(),
                    listValueRange(Value.get(LIST_START), SV_END.toAerospikeValue())),
            selector("onListValueRange(Map,SpecialValue)",
                    b -> b.onListValueRange(MAP_START, SV_END).getValues(),
                    listValueRange(Value.get(MAP_START), SV_END.toAerospikeValue())));
    }

    // ========================================
    // onMapValueRelativeRankRange — 16 overloads
    // ========================================

    private static Stream<Arguments> mapValueRelativeRankRangeOverloads() {
        return Stream.of(
            selector("onMapValueRelativeRankRange(long,int)",
                    b -> b.onMapValueRelativeRankRange(1L, RANK).getValues(),
                    mapValueRelRank(Value.get(1L))),
            selector("onMapValueRelativeRankRange(String,int)",
                    b -> b.onMapValueRelativeRankRange("a", RANK).getValues(),
                    mapValueRelRank(Value.get("a"))),
            selector("onMapValueRelativeRankRange(byte[],int)",
                    b -> b.onMapValueRelativeRankRange(BYTES_START, RANK).getValues(),
                    mapValueRelRank(Value.get(BYTES_START))),
            selector("onMapValueRelativeRankRange(double,int)",
                    b -> b.onMapValueRelativeRankRange(1.5d, RANK).getValues(),
                    mapValueRelRank(Value.get(1.5d))),
            selector("onMapValueRelativeRankRange(boolean,int)",
                    b -> b.onMapValueRelativeRankRange(true, RANK).getValues(),
                    mapValueRelRank(Value.get(true))),
            selector("onMapValueRelativeRankRange(List,int)",
                    b -> b.onMapValueRelativeRankRange(LIST_START, RANK).getValues(),
                    mapValueRelRank(Value.get(LIST_START))),
            selector("onMapValueRelativeRankRange(Map,int)",
                    b -> b.onMapValueRelativeRankRange(MAP_START, RANK).getValues(),
                    mapValueRelRank(Value.get(MAP_START))),
            selector("onMapValueRelativeRankRange(SpecialValue,int)",
                    b -> b.onMapValueRelativeRankRange(SV_START, RANK).getValues(),
                    mapValueRelRank(SV_START.toAerospikeValue())),
            selector("onMapValueRelativeRankRange(long,int,int)",
                    b -> b.onMapValueRelativeRankRange(1L, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get(1L))),
            selector("onMapValueRelativeRankRange(String,int,int)",
                    b -> b.onMapValueRelativeRankRange("a", RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get("a"))),
            selector("onMapValueRelativeRankRange(byte[],int,int)",
                    b -> b.onMapValueRelativeRankRange(BYTES_START, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get(BYTES_START))),
            selector("onMapValueRelativeRankRange(double,int,int)",
                    b -> b.onMapValueRelativeRankRange(1.5d, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get(1.5d))),
            selector("onMapValueRelativeRankRange(boolean,int,int)",
                    b -> b.onMapValueRelativeRankRange(true, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get(true))),
            selector("onMapValueRelativeRankRange(List,int,int)",
                    b -> b.onMapValueRelativeRankRange(LIST_START, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get(LIST_START))),
            selector("onMapValueRelativeRankRange(Map,int,int)",
                    b -> b.onMapValueRelativeRankRange(MAP_START, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(Value.get(MAP_START))),
            selector("onMapValueRelativeRankRange(SpecialValue,int,int)",
                    b -> b.onMapValueRelativeRankRange(SV_START, RANK, COUNT).getValues(),
                    mapValueRelRankCounted(SV_START.toAerospikeValue())));
    }

    // ========================================
    // onListValueRelativeRankRange — 16 overloads
    // ========================================

    private static Stream<Arguments> listValueRelativeRankRangeOverloads() {
        return Stream.of(
            selector("onListValueRelativeRankRange(long,int)",
                    b -> b.onListValueRelativeRankRange(1L, RANK).getValues(),
                    listValueRelRank(Value.get(1L))),
            selector("onListValueRelativeRankRange(String,int)",
                    b -> b.onListValueRelativeRankRange("a", RANK).getValues(),
                    listValueRelRank(Value.get("a"))),
            selector("onListValueRelativeRankRange(byte[],int)",
                    b -> b.onListValueRelativeRankRange(BYTES_START, RANK).getValues(),
                    listValueRelRank(Value.get(BYTES_START))),
            selector("onListValueRelativeRankRange(double,int)",
                    b -> b.onListValueRelativeRankRange(1.5d, RANK).getValues(),
                    listValueRelRank(Value.get(1.5d))),
            selector("onListValueRelativeRankRange(boolean,int)",
                    b -> b.onListValueRelativeRankRange(true, RANK).getValues(),
                    listValueRelRank(Value.get(true))),
            selector("onListValueRelativeRankRange(List,int)",
                    b -> b.onListValueRelativeRankRange(LIST_START, RANK).getValues(),
                    listValueRelRank(Value.get(LIST_START))),
            selector("onListValueRelativeRankRange(Map,int)",
                    b -> b.onListValueRelativeRankRange(MAP_START, RANK).getValues(),
                    listValueRelRank(Value.get(MAP_START))),
            selector("onListValueRelativeRankRange(SpecialValue,int)",
                    b -> b.onListValueRelativeRankRange(SV_START, RANK).getValues(),
                    listValueRelRank(SV_START.toAerospikeValue())),
            selector("onListValueRelativeRankRange(long,int,int)",
                    b -> b.onListValueRelativeRankRange(1L, RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get(1L))),
            selector("onListValueRelativeRankRange(String,int,int)",
                    b -> b.onListValueRelativeRankRange("a", RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get("a"))),
            selector("onListValueRelativeRankRange(byte[],int,int)",
                    b -> b.onListValueRelativeRankRange(BYTES_START, RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get(BYTES_START))),
            selector("onListValueRelativeRankRange(double,int,int)",
                    b -> b.onListValueRelativeRankRange(1.5d, RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get(1.5d))),
            selector("onListValueRelativeRankRange(boolean,int,int)",
                    b -> b.onListValueRelativeRankRange(true, RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get(true))),
            selector("onListValueRelativeRankRange(List,int,int)",
                    b -> b.onListValueRelativeRankRange(LIST_START, RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get(LIST_START))),
            selector("onListValueRelativeRankRange(Map,int,int)",
                    b -> b.onListValueRelativeRankRange(MAP_START, RANK, COUNT).getValues(),
                    listValueRelRankCounted(Value.get(MAP_START))),
            selector("onListValueRelativeRankRange(SpecialValue,int,int)",
                    b -> b.onListValueRelativeRankRange(SV_START, RANK, COUNT).getValues(),
                    listValueRelRankCounted(SV_START.toAerospikeValue())));
    }

    // ========================================
    // onMapKeyRange — 13 overloads
    // ========================================

    private static Stream<Arguments> mapKeyRangeOverloads() {
        return Stream.of(
            selector("onMapKeyRange(long,long)",
                    b -> b.onMapKeyRange(1L, 4L).getValues(),
                    mapKeyRange(Value.get(1L), Value.get(4L))),
            selector("onMapKeyRange(String,String)",
                    b -> b.onMapKeyRange("a", "b").getValues(),
                    mapKeyRange(Value.get("a"), Value.get("b"))),
            selector("onMapKeyRange(byte[],byte[])",
                    b -> b.onMapKeyRange(BYTES_START, BYTES_END).getValues(),
                    mapKeyRange(Value.get(BYTES_START), Value.get(BYTES_END))),
            selector("onMapKeyRange(double,double)",
                    b -> b.onMapKeyRange(1.5d, 2.5d).getValues(),
                    mapKeyRange(Value.get(1.5d), Value.get(2.5d))),
            selector("onMapKeyRange(SpecialValue,SpecialValue)",
                    b -> b.onMapKeyRange(SV_START, SV_END).getValues(),
                    mapKeyRange(SV_START.toAerospikeValue(), SV_END.toAerospikeValue())),
            selector("onMapKeyRange(SpecialValue,long)",
                    b -> b.onMapKeyRange(SV_START, 4L).getValues(),
                    mapKeyRange(SV_START.toAerospikeValue(), Value.get(4L))),
            selector("onMapKeyRange(SpecialValue,String)",
                    b -> b.onMapKeyRange(SV_START, "b").getValues(),
                    mapKeyRange(SV_START.toAerospikeValue(), Value.get("b"))),
            selector("onMapKeyRange(SpecialValue,byte[])",
                    b -> b.onMapKeyRange(SV_START, BYTES_END).getValues(),
                    mapKeyRange(SV_START.toAerospikeValue(), Value.get(BYTES_END))),
            selector("onMapKeyRange(SpecialValue,double)",
                    b -> b.onMapKeyRange(SV_START, 2.5d).getValues(),
                    mapKeyRange(SV_START.toAerospikeValue(), Value.get(2.5d))),
            selector("onMapKeyRange(long,SpecialValue)",
                    b -> b.onMapKeyRange(1L, SV_END).getValues(),
                    mapKeyRange(Value.get(1L), SV_END.toAerospikeValue())),
            selector("onMapKeyRange(String,SpecialValue)",
                    b -> b.onMapKeyRange("a", SV_END).getValues(),
                    mapKeyRange(Value.get("a"), SV_END.toAerospikeValue())),
            selector("onMapKeyRange(byte[],SpecialValue)",
                    b -> b.onMapKeyRange(BYTES_START, SV_END).getValues(),
                    mapKeyRange(Value.get(BYTES_START), SV_END.toAerospikeValue())),
            selector("onMapKeyRange(double,SpecialValue)",
                    b -> b.onMapKeyRange(1.5d, SV_END).getValues(),
                    mapKeyRange(Value.get(1.5d), SV_END.toAerospikeValue())));
    }

    // ========================================
    // onMapValue / onListValue — 8 overloads each
    // ========================================

    private static Stream<Arguments> mapValueOverloads() {
        return Stream.of(
            selector("onMapValue(long)", b -> b.onMapValue(1L).getValues(), mapValue(Value.get(1L))),
            selector("onMapValue(String)", b -> b.onMapValue("a").getValues(), mapValue(Value.get("a"))),
            selector("onMapValue(byte[])", b -> b.onMapValue(BYTES_START).getValues(), mapValue(Value.get(BYTES_START))),
            selector("onMapValue(double)", b -> b.onMapValue(1.5d).getValues(), mapValue(Value.get(1.5d))),
            selector("onMapValue(boolean)", b -> b.onMapValue(true).getValues(), mapValue(Value.get(true))),
            selector("onMapValue(List)", b -> b.onMapValue(LIST_START).getValues(), mapValue(Value.get(LIST_START))),
            selector("onMapValue(Map)", b -> b.onMapValue(MAP_START).getValues(), mapValue(Value.get(MAP_START))),
            selector("onMapValue(SpecialValue)",
                    b -> b.onMapValue(SV_START).getValues(),
                    mapValue(SV_START.toAerospikeValue())));
    }

    private static Stream<Arguments> listValueOverloads() {
        return Stream.of(
            selector("onListValue(long)", b -> b.onListValue(1L).getValues(), listValue(Value.get(1L))),
            selector("onListValue(String)", b -> b.onListValue("a").getValues(), listValue(Value.get("a"))),
            selector("onListValue(byte[])", b -> b.onListValue(BYTES_START).getValues(), listValue(Value.get(BYTES_START))),
            selector("onListValue(double)", b -> b.onListValue(1.5d).getValues(), listValue(Value.get(1.5d))),
            selector("onListValue(boolean)", b -> b.onListValue(true).getValues(), listValue(Value.get(true))),
            selector("onListValue(List)", b -> b.onListValue(LIST_START).getValues(), listValue(Value.get(LIST_START))),
            selector("onListValue(Map)", b -> b.onListValue(MAP_START).getValues(), listValue(Value.get(MAP_START))),
            selector("onListValue(SpecialValue)",
                    b -> b.onListValue(SV_START).getValues(),
                    listValue(SV_START.toAerospikeValue())));
    }

    private static Operation mapValue(Value value) {
        return MapOperation.getByValue(BIN, value, MapReturnType.VALUE, ROOT_CTX);
    }

    private static Operation listValue(Value value) {
        return ListOperation.getByValue(BIN, value, ListReturnType.VALUE, ROOT_CTX);
    }

    // ========================================
    // onMapKeyRelativeIndexRange — 6 overloads
    // ========================================

    private static Stream<Arguments> mapKeyRelativeIndexRangeOverloads() {
        return Stream.of(
            selector("onMapKeyRelativeIndexRange(long,int)",
                    b -> b.onMapKeyRelativeIndexRange(1L, RANK).getValues(),
                    mapKeyRelIndex(Value.get(1L))),
            selector("onMapKeyRelativeIndexRange(String,int)",
                    b -> b.onMapKeyRelativeIndexRange("a", RANK).getValues(),
                    mapKeyRelIndex(Value.get("a"))),
            selector("onMapKeyRelativeIndexRange(byte[],int)",
                    b -> b.onMapKeyRelativeIndexRange(BYTES_START, RANK).getValues(),
                    mapKeyRelIndex(Value.get(BYTES_START))),
            selector("onMapKeyRelativeIndexRange(long,int,int)",
                    b -> b.onMapKeyRelativeIndexRange(1L, RANK, COUNT).getValues(),
                    mapKeyRelIndexCounted(Value.get(1L))),
            selector("onMapKeyRelativeIndexRange(String,int,int)",
                    b -> b.onMapKeyRelativeIndexRange("a", RANK, COUNT).getValues(),
                    mapKeyRelIndexCounted(Value.get("a"))),
            selector("onMapKeyRelativeIndexRange(byte[],int,int)",
                    b -> b.onMapKeyRelativeIndexRange(BYTES_START, RANK, COUNT).getValues(),
                    mapKeyRelIndexCounted(Value.get(BYTES_START))));
    }

    private static Operation mapKeyRelIndex(Value key) {
        return MapOperation.getByKeyRelativeIndexRange(BIN, key, RANK, MapReturnType.VALUE, ROOT_CTX);
    }

    private static Operation mapKeyRelIndexCounted(Value key) {
        return MapOperation.getByKeyRelativeIndexRange(BIN, key, RANK, COUNT, MapReturnType.VALUE, ROOT_CTX);
    }

    // ========================================
    // Index, rank, key and multi-value selectors
    // ========================================

    private static Stream<Arguments> positionalSelectors() {
        List<Value> keyValues = List.of(Value.get("k1"), Value.get("k2"));
        List<Value> valueValues = List.of(Value.get(1L), Value.get(2L));
        return Stream.of(
            selector("onMapIndex(int)",
                    b -> b.onMapIndex(3).getValues(),
                    MapOperation.getByIndex(BIN, 3, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapIndexRange(int,int)",
                    b -> b.onMapIndexRange(1, COUNT).getValues(),
                    MapOperation.getByIndexRange(BIN, 1, COUNT, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapIndexRange(int)",
                    b -> b.onMapIndexRange(1).getValues(),
                    MapOperation.getByIndexRange(BIN, 1, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapKey(long)",
                    b -> b.onMapKey(7L).getValues(),
                    MapOperation.getByKey(BIN, Value.get(7L), MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapKey(String)",
                    b -> b.onMapKey("k").getValues(),
                    MapOperation.getByKey(BIN, Value.get("k"), MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapKey(byte[])",
                    b -> b.onMapKey(BYTES_START).getValues(),
                    MapOperation.getByKey(BIN, Value.get(BYTES_START), MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapRank(int)",
                    b -> b.onMapRank(2).getValues(),
                    MapOperation.getByRank(BIN, 2, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapRankRange(int,int)",
                    b -> b.onMapRankRange(1, COUNT).getValues(),
                    MapOperation.getByRankRange(BIN, 1, COUNT, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapRankRange(int) routes to the counted form with count 0",
                    b -> b.onMapRankRange(1).getValues(),
                    MapOperation.getByRankRange(BIN, 1, 0, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapKeyList(List)",
                    b -> b.onMapKeyList(List.of("k1", "k2")).getValues(),
                    MapOperation.getByKeyList(BIN, keyValues, MapReturnType.VALUE, ROOT_CTX)),
            selector("onMapValueList(List)",
                    b -> b.onMapValueList(List.of(1L, 2L)).getValues(),
                    MapOperation.getByValueList(BIN, valueValues, MapReturnType.VALUE, ROOT_CTX)),
            selector("onListIndex(int)",
                    b -> b.onListIndex(0).getValues(),
                    ListOperation.getByIndex(BIN, 0, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListIndex(int,ListOrder,boolean)",
                    b -> b.onListIndex(0, ListOrder.ORDERED, true).getValues(),
                    ListOperation.getByIndex(BIN, 0, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListRank(int)",
                    b -> b.onListRank(2).getValues(),
                    ListOperation.getByRank(BIN, 2, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListIndexRange(int,int)",
                    b -> b.onListIndexRange(1, COUNT).getValues(),
                    ListOperation.getByIndexRange(BIN, 1, COUNT, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListIndexRange(int)",
                    b -> b.onListIndexRange(1).getValues(),
                    ListOperation.getByIndexRange(BIN, 1, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListRankRange(int,int)",
                    b -> b.onListRankRange(1, COUNT).getValues(),
                    ListOperation.getByRankRange(BIN, 1, COUNT, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListRankRange(int)",
                    b -> b.onListRankRange(1).getValues(),
                    ListOperation.getByRankRange(BIN, 1, ListReturnType.VALUE, ROOT_CTX)),
            selector("onListValueList(List)",
                    b -> b.onListValueList(List.of(1L, 2L)).getValues(),
                    ListOperation.getByValueList(BIN, valueValues, ListReturnType.VALUE, ROOT_CTX)));
    }
}
