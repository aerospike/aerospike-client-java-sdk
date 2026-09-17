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
import static com.aerospike.client.sdk.CdtOperationCapture.assertOperation;
import static com.aerospike.client.sdk.CdtOperationCapture.emitBin;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtOperationCapture.CapturingOperationBuilder;
import com.aerospike.client.sdk.cdt.MapOrder;

/**
 * Covers the whole-bin writes and reads on {@link BinBuilder}.
 *
 * <p>Each of these overloads exists so a caller can hand the builder a Java type directly, and each one
 * does nothing but wrap that value in a {@link Bin}. What can go wrong is the wrapping: an overload that
 * picks the wrong {@code Bin} factory, or a write that is queued as an add. The table pins the pairing of
 * each overload to the operation it produces.</p>
 */
class BinBuilderValueTest {

    private static final byte[] BYTES = { 1, 2, 3 };
    private static final List<?> LIST = List.of(1L, 2L);
    private static final Map<?, ?> MAP = Map.of("a", 1L);
    private static final SortedMap<String, Long> SORTED_MAP = new TreeMap<>(Map.of("a", 1L));
    private static final AerospikeList<Long> AEROSPIKE_LIST = new AerospikeList<>(2);
    private static final AerospikeMap<String, Long> AEROSPIKE_MAP =
        AerospikeMap.of(MapOrder.KEY_ORDERED, Map.of("a", 1L));
    private static final Value.HLLValue HLL = new Value.HLLValue(BYTES);
    private static final Value.GeoJSONValue GEO =
        new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[0,0]}");

    private static Arguments binOp(String label, Consumer<BinBuilder<CapturingOperationBuilder>> invoke,
                                   Operation expected) {
        return Arguments.of(label, invoke, expected);
    }

    private static Stream<Arguments> binOperations() {
        return Stream.of(
            binOp("setTo(String)", b -> b.setTo("s"), Operation.put(new Bin(BIN, "s"))),
            binOp("setTo(int)", b -> b.setTo(1), Operation.put(new Bin(BIN, 1))),
            binOp("setTo(long)", b -> b.setTo(1L), Operation.put(new Bin(BIN, 1L))),
            binOp("setTo(float)", b -> b.setTo(1.5f), Operation.put(new Bin(BIN, 1.5f))),
            binOp("setTo(double)", b -> b.setTo(1.5d), Operation.put(new Bin(BIN, 1.5d))),
            binOp("setTo(boolean)", b -> b.setTo(true), Operation.put(new Bin(BIN, true))),
            binOp("setTo(byte[])", b -> b.setTo(BYTES), Operation.put(new Bin(BIN, BYTES))),
            binOp("setTo(List)", b -> b.setTo(LIST), Operation.put(new Bin(BIN, LIST))),
            binOp("setTo(Map)", b -> b.setTo(MAP), Operation.put(new Bin(BIN, MAP))),
            binOp("setTo(SortedMap)", b -> b.setTo(SORTED_MAP), Operation.put(new Bin(BIN, SORTED_MAP))),
            binOp("setTo(AerospikeList)", b -> b.setTo(AEROSPIKE_LIST),
                    Operation.put(new Bin(BIN, AEROSPIKE_LIST))),
            binOp("setTo(AerospikeMap)", b -> b.setTo(AEROSPIKE_MAP), Operation.put(new Bin(BIN, AEROSPIKE_MAP))),
            binOp("setTo(HLLValue)", b -> b.setTo(HLL), Operation.put(new Bin(BIN, HLL))),
            binOp("setTo(GeoJSONValue)", b -> b.setTo(GEO), Operation.put(new Bin(BIN, GEO))),
            binOp("setToGeoJson(String)", b -> b.setToGeoJson(GEO.toString()),
                    Operation.put(Bin.asGeoJSON(BIN, GEO.toString()))),

            binOp("add(int)", b -> b.add(1), Operation.add(new Bin(BIN, 1))),
            binOp("add(long)", b -> b.add(1L), Operation.add(new Bin(BIN, 1L))),
            binOp("add(float)", b -> b.add(1.5f), Operation.add(new Bin(BIN, 1.5f))),
            binOp("add(double)", b -> b.add(1.5d), Operation.add(new Bin(BIN, 1.5d))),

            binOp("remove()", b -> b.remove(), Operation.put(Bin.asNull(BIN))),
            binOp("get()", b -> b.get(), Operation.get(BIN)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("binOperations")
    void binOperationWrapsTheValueItWasGiven(String label,
                                             Consumer<BinBuilder<CapturingOperationBuilder>> invoke,
                                             Operation expected) {
        assertOperation(emitBin(invoke), expected);
    }
}
