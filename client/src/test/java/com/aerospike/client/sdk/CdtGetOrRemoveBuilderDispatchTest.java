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
import static com.aerospike.client.sdk.CdtOperationCapture.emitOperate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtOperationCapture.CapturingOperationBuilder;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;

/**
 * Covers the two dispatch switches at the heart of {@link CdtGetOrRemoveBuilder}.
 *
 * <p>Unlike {@link CdtReadOnlyBuilder}, which repeats a switch inside every terminal, the operate-path
 * builder funnels all reads through {@code dispatchGet} and all removals through {@code dispatchRemove}.
 * That means the selector dimension and the terminal dimension are independent, so they are tested as
 * two separate tables instead of a cross-product:</p>
 * <ul>
 *   <li>every selector shape is driven once through each dispatch switch, pinning the low-level
 *       operation and its arguments;</li>
 *   <li>every terminal is driven once, pinning only the return type it feeds into dispatch.</li>
 * </ul>
 */
// getAsMap and getAsOrderedMap are deprecated but still shipped, so they are still covered here.
@SuppressWarnings("deprecation")
class CdtGetOrRemoveBuilderDispatchTest {

    private static final Value KEY_A = Value.get("a");
    private static final Value VAL_1 = Value.get(1L);
    private static final Value VAL_4 = Value.get(4L);
    private static final List<Value> KEY_LIST = List.of(Value.get("k1"), Value.get("k2"));
    private static final List<Value> VALUE_LIST = List.of(Value.get(1L), Value.get(2L));

    private static final int INDEX = 1;
    private static final int RANK = 3;
    private static final int COUNT = 2;

    private static Arguments selector(String label, boolean list,
                                      Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                      IntFunction<Operation> get, IntFunction<Operation> remove) {
        return Arguments.of(label, list, apply, get, remove);
    }

    /**
     * One entry per {@code case} label in the dispatch switches, plus one per optional-count branch.
     * The {@code get} and {@code remove} functions are the direct CDT call the fluent chain should
     * compile down to, parameterised only by the return type the terminal chose.
     */
    private static Stream<Arguments> selectors() {
        return Stream.of(
            selector("mapIndex", false,
                b -> b.onMapIndex(INDEX),
                rt -> MapOperation.getByIndex(BIN, INDEX, rt, ROOT_CTX),
                rt -> MapOperation.removeByIndex(BIN, INDEX, rt, ROOT_CTX)),
            selector("mapIndexRange(count)", false,
                b -> b.onMapIndexRange(INDEX, COUNT),
                rt -> MapOperation.getByIndexRange(BIN, INDEX, COUNT, rt, ROOT_CTX),
                rt -> MapOperation.removeByIndexRange(BIN, INDEX, COUNT, rt, ROOT_CTX)),
            selector("mapIndexRange(open)", false,
                b -> b.onMapIndexRange(INDEX),
                rt -> MapOperation.getByIndexRange(BIN, INDEX, rt, ROOT_CTX),
                rt -> MapOperation.removeByIndexRange(BIN, INDEX, rt, ROOT_CTX)),
            selector("mapKey", false,
                b -> b.onMapKey("a"),
                rt -> MapOperation.getByKey(BIN, KEY_A, rt, ROOT_CTX),
                rt -> MapOperation.removeByKey(BIN, KEY_A, rt, ROOT_CTX)),
            selector("mapKeyList", false,
                b -> b.onMapKeyList(List.of("k1", "k2")),
                rt -> MapOperation.getByKeyList(BIN, KEY_LIST, rt, ROOT_CTX),
                rt -> MapOperation.removeByKeyList(BIN, KEY_LIST, rt, ROOT_CTX)),
            selector("mapKeyRange", false,
                b -> b.onMapKeyRange("a", "b"),
                rt -> MapOperation.getByKeyRange(BIN, KEY_A, Value.get("b"), rt, ROOT_CTX),
                rt -> MapOperation.removeByKeyRange(BIN, KEY_A, Value.get("b"), rt, ROOT_CTX)),
            selector("mapRank", false,
                b -> b.onMapRank(RANK),
                rt -> MapOperation.getByRank(BIN, RANK, rt, ROOT_CTX),
                rt -> MapOperation.removeByRank(BIN, RANK, rt, ROOT_CTX)),
            selector("mapRankRange(count)", false,
                b -> b.onMapRankRange(RANK, COUNT),
                rt -> MapOperation.getByRankRange(BIN, RANK, COUNT, rt, ROOT_CTX),
                rt -> MapOperation.removeByRankRange(BIN, RANK, COUNT, rt, ROOT_CTX)),
            selector("mapValue", false,
                b -> b.onMapValue(1L),
                rt -> MapOperation.getByValue(BIN, VAL_1, rt, ROOT_CTX),
                rt -> MapOperation.removeByValue(BIN, VAL_1, rt, ROOT_CTX)),
            selector("mapValueList", false,
                b -> b.onMapValueList(List.of(1L, 2L)),
                rt -> MapOperation.getByValueList(BIN, VALUE_LIST, rt, ROOT_CTX),
                rt -> MapOperation.removeByValueList(BIN, VALUE_LIST, rt, ROOT_CTX)),
            selector("mapValueRange", false,
                b -> b.onMapValueRange(1L, 4L),
                rt -> MapOperation.getByValueRange(BIN, VAL_1, VAL_4, rt, ROOT_CTX),
                rt -> MapOperation.removeByValueRange(BIN, VAL_1, VAL_4, rt, ROOT_CTX)),
            selector("mapKeyRelativeIndexRange(count)", false,
                b -> b.onMapKeyRelativeIndexRange("a", INDEX, COUNT),
                rt -> MapOperation.getByKeyRelativeIndexRange(BIN, KEY_A, INDEX, COUNT, rt, ROOT_CTX),
                rt -> MapOperation.removeByKeyRelativeIndexRange(BIN, KEY_A, INDEX, COUNT, rt, ROOT_CTX)),
            selector("mapKeyRelativeIndexRange(open)", false,
                b -> b.onMapKeyRelativeIndexRange("a", INDEX),
                rt -> MapOperation.getByKeyRelativeIndexRange(BIN, KEY_A, INDEX, rt, ROOT_CTX),
                rt -> MapOperation.removeByKeyRelativeIndexRange(BIN, KEY_A, INDEX, rt, ROOT_CTX)),
            selector("mapValueRelativeRankRange(count)", false,
                b -> b.onMapValueRelativeRankRange(1L, RANK, COUNT),
                rt -> MapOperation.getByValueRelativeRankRange(BIN, VAL_1, RANK, COUNT, rt, ROOT_CTX),
                rt -> MapOperation.removeByValueRelativeRankRange(BIN, VAL_1, RANK, COUNT, rt, ROOT_CTX)),
            selector("mapValueRelativeRankRange(open)", false,
                b -> b.onMapValueRelativeRankRange(1L, RANK),
                rt -> MapOperation.getByValueRelativeRankRange(BIN, VAL_1, RANK, rt, ROOT_CTX),
                rt -> MapOperation.removeByValueRelativeRankRange(BIN, VAL_1, RANK, rt, ROOT_CTX)),

            selector("listIndex", true,
                b -> b.onListIndex(INDEX),
                rt -> ListOperation.getByIndex(BIN, INDEX, rt, ROOT_CTX),
                rt -> ListOperation.removeByIndex(BIN, INDEX, rt, ROOT_CTX)),
            selector("listIndexRange(count)", true,
                b -> b.onListIndexRange(INDEX, COUNT),
                rt -> ListOperation.getByIndexRange(BIN, INDEX, COUNT, rt, ROOT_CTX),
                rt -> ListOperation.removeByIndexRange(BIN, INDEX, COUNT, rt, ROOT_CTX)),
            selector("listIndexRange(open)", true,
                b -> b.onListIndexRange(INDEX),
                rt -> ListOperation.getByIndexRange(BIN, INDEX, rt, ROOT_CTX),
                rt -> ListOperation.removeByIndexRange(BIN, INDEX, rt, ROOT_CTX)),
            selector("listRank", true,
                b -> b.onListRank(RANK),
                rt -> ListOperation.getByRank(BIN, RANK, rt, ROOT_CTX),
                rt -> ListOperation.removeByRank(BIN, RANK, rt, ROOT_CTX)),
            selector("listRankRange(count)", true,
                b -> b.onListRankRange(RANK, COUNT),
                rt -> ListOperation.getByRankRange(BIN, RANK, COUNT, rt, ROOT_CTX),
                rt -> ListOperation.removeByRankRange(BIN, RANK, COUNT, rt, ROOT_CTX)),
            selector("listRankRange(open)", true,
                b -> b.onListRankRange(RANK),
                rt -> ListOperation.getByRankRange(BIN, RANK, rt, ROOT_CTX),
                rt -> ListOperation.removeByRankRange(BIN, RANK, rt, ROOT_CTX)),
            selector("listValue", true,
                b -> b.onListValue(1L),
                rt -> ListOperation.getByValue(BIN, VAL_1, rt, ROOT_CTX),
                rt -> ListOperation.removeByValue(BIN, VAL_1, rt, ROOT_CTX)),
            selector("listValueList", true,
                b -> b.onListValueList(List.of(1L, 2L)),
                rt -> ListOperation.getByValueList(BIN, VALUE_LIST, rt, ROOT_CTX),
                rt -> ListOperation.removeByValueList(BIN, VALUE_LIST, rt, ROOT_CTX)),
            selector("listValueRange", true,
                b -> b.onListValueRange(1L, 4L),
                rt -> ListOperation.getByValueRange(BIN, VAL_1, VAL_4, rt, ROOT_CTX),
                rt -> ListOperation.removeByValueRange(BIN, VAL_1, VAL_4, rt, ROOT_CTX)),
            selector("listValueRelativeRankRange(count)", true,
                b -> b.onListValueRelativeRankRange(1L, RANK, COUNT),
                rt -> ListOperation.getByValueRelativeRankRange(BIN, VAL_1, RANK, COUNT, rt, ROOT_CTX),
                rt -> ListOperation.removeByValueRelativeRankRange(BIN, VAL_1, RANK, COUNT, rt, ROOT_CTX)),
            selector("listValueRelativeRankRange(open)", true,
                b -> b.onListValueRelativeRankRange(1L, RANK),
                rt -> ListOperation.getByValueRelativeRankRange(BIN, VAL_1, RANK, rt, ROOT_CTX),
                rt -> ListOperation.removeByValueRelativeRankRange(BIN, VAL_1, RANK, rt, ROOT_CTX))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("selectors")
    void getValuesCompilesToDirectCdtRead(String label, boolean list,
                                          Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                          IntFunction<Operation> get, IntFunction<Operation> remove) {
        assertOperation(emitOperate(apply, CdtGetOrRemoveBuilder::getValues),
                get.apply(list ? ListReturnType.VALUE : MapReturnType.VALUE));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("selectors")
    void removeCompilesToDirectCdtRemove(String label, boolean list,
                                         Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                         IntFunction<Operation> get, IntFunction<Operation> remove) {
        assertOperation(emitOperate(apply, CdtGetOrRemoveBuilder::remove),
                remove.apply(list ? ListReturnType.COUNT : MapReturnType.NONE));
    }

    // ========================================
    // Terminals: return type only
    // ========================================

    private static Arguments terminal(String label,
                                      Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                      int mapReturnType) {
        return Arguments.of(label, apply, mapReturnType);
    }

    /** Map-capable read terminals, checked against a map range selector so inverted variants are legal. */
    private static Stream<Arguments> mapReadTerminals() {
        return Stream.of(
            terminal("getValues", CdtGetOrRemoveBuilder::getValues, MapReturnType.VALUE),
            terminal("getKeys", CdtGetOrRemoveBuilder::getKeys, MapReturnType.KEY),
            terminal("count", CdtGetOrRemoveBuilder::count, MapReturnType.COUNT),
            terminal("exists", CdtGetOrRemoveBuilder::exists, MapReturnType.EXISTS),
            terminal("getIndexes", CdtGetOrRemoveBuilder::getIndexes, MapReturnType.INDEX),
            terminal("getReverseIndexes", CdtGetOrRemoveBuilder::getReverseIndexes, MapReturnType.REVERSE_INDEX),
            terminal("getRanks", CdtGetOrRemoveBuilder::getRanks, MapReturnType.RANK),
            terminal("getReverseRanks", CdtGetOrRemoveBuilder::getReverseRanks, MapReturnType.REVERSE_RANK),
            terminal("getKeysAndValues", CdtGetOrRemoveBuilder::getKeysAndValues, MapReturnType.KEY_VALUE),
            terminal("getAsMap", CdtGetOrRemoveBuilder::getAsMap, MapReturnType.UNORDERED_MAP),
            terminal("getAsOrderedMap", CdtGetOrRemoveBuilder::getAsOrderedMap, MapReturnType.ORDERED_MAP),
            terminal("countAllOthers", CdtGetOrRemoveBuilder::countAllOthers,
                MapReturnType.COUNT | MapReturnType.INVERTED),
            terminal("getAllOtherValues", CdtGetOrRemoveBuilder::getAllOtherValues,
                MapReturnType.VALUE | MapReturnType.INVERTED),
            terminal("getAllOtherKeys", CdtGetOrRemoveBuilder::getAllOtherKeys,
                MapReturnType.KEY | MapReturnType.INVERTED),
            terminal("getAllOtherIndexes", CdtGetOrRemoveBuilder::getAllOtherIndexes,
                MapReturnType.INDEX | MapReturnType.INVERTED),
            terminal("getAllOtherReverseIndexes", CdtGetOrRemoveBuilder::getAllOtherReverseIndexes,
                MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED),
            terminal("getAllOtherRanks", CdtGetOrRemoveBuilder::getAllOtherRanks,
                MapReturnType.RANK | MapReturnType.INVERTED),
            terminal("getAllOtherReverseRanks", CdtGetOrRemoveBuilder::getAllOtherReverseRanks,
                MapReturnType.REVERSE_RANK | MapReturnType.INVERTED),
            terminal("getAllOtherKeysAndValues", CdtGetOrRemoveBuilder::getAllOtherKeysAndValues,
                MapReturnType.KEY_VALUE | MapReturnType.INVERTED)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mapReadTerminals")
    void mapReadTerminalSelectsItsReturnType(String label,
                                             Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                             int mapReturnType) {
        assertOperation(emitOperate(b -> b.onMapValueRange(1L, 4L), apply),
                MapOperation.getByValueRange(BIN, VAL_1, VAL_4, mapReturnType, ROOT_CTX));
    }

    private static Stream<Arguments> listReadTerminals() {
        return Stream.of(
            terminal("getValues", CdtGetOrRemoveBuilder::getValues, ListReturnType.VALUE),
            terminal("count", CdtGetOrRemoveBuilder::count, ListReturnType.COUNT),
            terminal("exists", CdtGetOrRemoveBuilder::exists, ListReturnType.EXISTS),
            terminal("getIndexes", CdtGetOrRemoveBuilder::getIndexes, ListReturnType.INDEX),
            terminal("getReverseIndexes", CdtGetOrRemoveBuilder::getReverseIndexes, ListReturnType.REVERSE_INDEX),
            terminal("getRanks", CdtGetOrRemoveBuilder::getRanks, ListReturnType.RANK),
            terminal("getReverseRanks", CdtGetOrRemoveBuilder::getReverseRanks, ListReturnType.REVERSE_RANK),
            terminal("countAllOthers", CdtGetOrRemoveBuilder::countAllOthers,
                ListReturnType.COUNT | ListReturnType.INVERTED),
            terminal("getAllOtherValues", CdtGetOrRemoveBuilder::getAllOtherValues,
                ListReturnType.VALUE | ListReturnType.INVERTED),
            terminal("getAllOtherIndexes", CdtGetOrRemoveBuilder::getAllOtherIndexes,
                ListReturnType.INDEX | ListReturnType.INVERTED),
            terminal("getAllOtherReverseIndexes", CdtGetOrRemoveBuilder::getAllOtherReverseIndexes,
                ListReturnType.REVERSE_INDEX | ListReturnType.INVERTED),
            terminal("getAllOtherRanks", CdtGetOrRemoveBuilder::getAllOtherRanks,
                ListReturnType.RANK | ListReturnType.INVERTED),
            terminal("getAllOtherReverseRanks", CdtGetOrRemoveBuilder::getAllOtherReverseRanks,
                ListReturnType.REVERSE_RANK | ListReturnType.INVERTED)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("listReadTerminals")
    void listReadTerminalSelectsItsReturnType(String label,
                                              Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                              int listReturnType) {
        assertOperation(emitOperate(b -> b.onListValueRange(1L, 4L), apply),
                ListOperation.getByValueRange(BIN, VAL_1, VAL_4, listReturnType, ROOT_CTX));
    }

    // ========================================
    // removeAnd() / removeAllOthersAnd()
    // ========================================

    private static Stream<Arguments> removeResultTerminals() {
        return Stream.of(
            Arguments.of("getValues", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getValues, MapReturnType.VALUE),
            Arguments.of("getKeys", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getKeys, MapReturnType.KEY),
            Arguments.of("count", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::count, MapReturnType.COUNT),
            Arguments.of("getIndexes", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getIndexes, MapReturnType.INDEX),
            Arguments.of("getReverseIndexes", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getReverseIndexes, MapReturnType.REVERSE_INDEX),
            Arguments.of("getRanks", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getRanks, MapReturnType.RANK),
            Arguments.of("getReverseRanks", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getReverseRanks, MapReturnType.REVERSE_RANK),
            Arguments.of("getKeysAndValues", (Consumer<RemoveResultBuilder<CapturingOperationBuilder>>)
                RemoveResultBuilder::getKeysAndValues, MapReturnType.KEY_VALUE)
        );
    }

    @ParameterizedTest(name = "removeAnd().{0}")
    @MethodSource("removeResultTerminals")
    void removeAndReturnsRequestedData(String label,
                                       Consumer<RemoveResultBuilder<CapturingOperationBuilder>> apply,
                                       int mapReturnType) {
        assertOperation(emitOperate(b -> b.onMapValueRange(1L, 4L), b -> apply.accept(b.removeAnd())),
                MapOperation.removeByValueRange(BIN, VAL_1, VAL_4, mapReturnType, ROOT_CTX));
    }

    @ParameterizedTest(name = "removeAllOthersAnd().{0}")
    @MethodSource("removeResultTerminals")
    void removeAllOthersAndInvertsTheSelection(String label,
                                               Consumer<RemoveResultBuilder<CapturingOperationBuilder>> apply,
                                               int mapReturnType) {
        assertOperation(emitOperate(b -> b.onMapValueRange(1L, 4L), b -> apply.accept(b.removeAllOthersAnd())),
                MapOperation.removeByValueRange(BIN, VAL_1, VAL_4, mapReturnType | MapReturnType.INVERTED, ROOT_CTX));
    }

    @Test
    void removeAllOthersInvertsWithoutReturningData() {
        assertOperation(emitOperate(b -> b.onMapValueRange(1L, 4L), CdtGetOrRemoveBuilder::removeAllOthers),
                MapOperation.removeByValueRange(BIN, VAL_1, VAL_4, MapReturnType.INVERTED, ROOT_CTX));
    }

    /**
     * {@link RemoveResultBuilder} calls the single-argument {@code dispatchRemove}, which passes a
     * {@link MapReturnType} constant through as the list return type. That is only sound because the
     * two constant sets agree; this pins the assumption so a future divergence fails here rather than
     * silently mis-encoding list removals.
     */
    @Test
    void mapAndListReturnTypeConstantsAgree() {
        assertEquals(MapReturnType.NONE, ListReturnType.NONE, "NONE");
        assertEquals(MapReturnType.INDEX, ListReturnType.INDEX, "INDEX");
        assertEquals(MapReturnType.REVERSE_INDEX, ListReturnType.REVERSE_INDEX, "REVERSE_INDEX");
        assertEquals(MapReturnType.RANK, ListReturnType.RANK, "RANK");
        assertEquals(MapReturnType.REVERSE_RANK, ListReturnType.REVERSE_RANK, "REVERSE_RANK");
        assertEquals(MapReturnType.COUNT, ListReturnType.COUNT, "COUNT");
        assertEquals(MapReturnType.VALUE, ListReturnType.VALUE, "VALUE");
        assertEquals(MapReturnType.EXISTS, ListReturnType.EXISTS, "EXISTS");
        assertEquals(MapReturnType.INVERTED, ListReturnType.INVERTED, "INVERTED");
    }

    // ========================================
    // Rejections
    // ========================================

    private static Stream<Arguments> mapOnlyTerminals() {
        return Stream.of(
            terminal("getKeys", CdtGetOrRemoveBuilder::getKeys, 0),
            terminal("getKeysAndValues", CdtGetOrRemoveBuilder::getKeysAndValues, 0),
            terminal("getAsMap", CdtGetOrRemoveBuilder::getAsMap, 0),
            terminal("getAsOrderedMap", CdtGetOrRemoveBuilder::getAsOrderedMap, 0),
            terminal("getAllOtherKeys", CdtGetOrRemoveBuilder::getAllOtherKeys, 0),
            terminal("getAllOtherKeysAndValues", CdtGetOrRemoveBuilder::getAllOtherKeysAndValues, 0),
            terminal("collectKeys", CdtGetOrRemoveBuilder::collectKeys, 0),
            terminal("collectKeyValues", CdtGetOrRemoveBuilder::collectKeyValues, 0)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mapOnlyTerminals")
    void mapOnlyTerminalRejectsListSelection(String label,
                                             Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                             int unused) {
        assertThrows(IllegalArgumentException.class,
                () -> emitOperate(b -> b.onListValueRange(1L, 4L), apply));
    }

    @Test
    void removeAndKeyTerminalsRejectListSelection() {
        assertThrows(IllegalArgumentException.class,
                () -> emitOperate(b -> b.onListValueRange(1L, 4L), b -> b.removeAnd().getKeys()));
        assertThrows(IllegalArgumentException.class,
                () -> emitOperate(b -> b.onListValueRange(1L, 4L), b -> b.removeAnd().getKeysAndValues()));
    }

    private static Stream<Arguments> invertedTerminals() {
        return Stream.of(
            terminal("countAllOthers", CdtGetOrRemoveBuilder::countAllOthers, 0),
            terminal("removeAllOthers", CdtGetOrRemoveBuilder::removeAllOthers, 0),
            terminal("removeAllOthersAnd", CdtGetOrRemoveBuilder::removeAllOthersAnd, 0),
            terminal("getAllOtherValues", CdtGetOrRemoveBuilder::getAllOtherValues, 0),
            terminal("getAllOtherKeys", CdtGetOrRemoveBuilder::getAllOtherKeys, 0),
            terminal("getAllOtherIndexes", CdtGetOrRemoveBuilder::getAllOtherIndexes, 0),
            terminal("getAllOtherReverseIndexes", CdtGetOrRemoveBuilder::getAllOtherReverseIndexes, 0),
            terminal("getAllOtherRanks", CdtGetOrRemoveBuilder::getAllOtherRanks, 0),
            terminal("getAllOtherReverseRanks", CdtGetOrRemoveBuilder::getAllOtherReverseRanks, 0),
            terminal("getAllOtherKeysAndValues", CdtGetOrRemoveBuilder::getAllOtherKeysAndValues, 0)
        );
    }

    /** The server cannot invert a selection that already names exactly one element. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("invertedTerminals")
    void invertedTerminalRejectsSingleElementSelection(String label,
                                                       Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> apply,
                                                       int unused) {
        assertThrows(IllegalArgumentException.class, () -> emitOperate(b -> b.onMapKey("a"), apply));
    }

    // ========================================
    // Optional-count encoding
    // ========================================

    /**
     * {@code CdtOperationParams.hasInt2()} tests {@code int2 != 0}, so an explicitly requested count of
     * zero is indistinguishable from "no count supplied" and takes the open-ended branch. The chain
     * below asks for zero elements but encodes "from index 1 to the end of the map".
     *
     * <p>This pins current behaviour; it is a defect, not the intended contract.</p>
     */
    @Test
    @Tag(KnownDefect.TAG)
    void explicitZeroCountIsEncodedAsOpenEndedRange() {
        KnownDefect.pinned(
            "onMapIndexRange(index, 0) asks for no elements but encodes an open-ended range, selecting "
                + "everything from index onwards; hasInt2() cannot tell a count of zero from an absent count",
            () -> assertOperation(emitOperate(b -> b.onMapIndexRange(INDEX, 0), CdtGetOrRemoveBuilder::getValues),
                    MapOperation.getByIndexRange(BIN, INDEX, MapReturnType.VALUE, ROOT_CTX)));
    }

    /**
     * {@code MAP_BY_RANK_RANGE} is the one selector whose dispatch has no open-ended branch, so
     * {@code onMapRankRange(rank)} reads the unset {@code int2} and encodes a count of zero, selecting
     * nothing instead of everything from {@code rank} onwards.
     *
     * <p>This pins current behaviour; it is a defect, not the intended contract.</p>
     */
    @Test
    @Tag(KnownDefect.TAG)
    void openEndedMapRankRangeIsEncodedAsZeroCount() {
        KnownDefect.pinned(
            "onMapRankRange(rank) should select every entry from rank onwards, but MAP_BY_RANK_RANGE has no "
                + "open-ended branch in either dispatch switch, so it encodes a count of zero and selects nothing",
            () -> {
                assertOperation(emitOperate(b -> b.onMapRankRange(RANK), CdtGetOrRemoveBuilder::getValues),
                        MapOperation.getByRankRange(BIN, RANK, 0, MapReturnType.VALUE, ROOT_CTX));
                assertOperation(emitOperate(b -> b.onMapRankRange(RANK), CdtGetOrRemoveBuilder::remove),
                        MapOperation.removeByRankRange(BIN, RANK, 0, MapReturnType.NONE, ROOT_CTX));
            });
    }
}
