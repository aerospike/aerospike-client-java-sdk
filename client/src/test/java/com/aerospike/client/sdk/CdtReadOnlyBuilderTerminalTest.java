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
import static com.aerospike.client.sdk.CdtOperationCapture.ROOT_KEY;
import static com.aerospike.client.sdk.CdtOperationCapture.assertOperation;
import static com.aerospike.client.sdk.CdtOperationCapture.emit;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;

/**
 * Covers the terminal read methods on {@link CdtReadOnlyBuilder} across every selector they accept.
 *
 * <p>Each terminal is an independent {@code switch} over the current selector that differs from its
 * siblings only in the return-type constant it passes to {@code MapOperation} / {@code ListOperation}.
 * Rather than hand-writing the combinations, the cases are generated from a list of selectors and a
 * list of terminals, so every {@code case} label in every terminal switch is reached exactly once.</p>
 *
 * <p>{@code getValues()} is deliberately absent: {@link CdtReadOnlyBuilderSelectorTest} already drives
 * it across the full selector surface. Server-side behaviour belongs in the integration suites.</p>
 */
@SuppressWarnings("deprecation")
class CdtReadOnlyBuilderTerminalTest {

    /** Marks a terminal that has no list form, so a list selector must be rejected. */
    private static final int MAP_ONLY = -1;

    private static final List<Value> KEY_VALUES = List.of(Value.get("k1"), Value.get("k2"));
    private static final List<Value> NUM_VALUES = List.of(Value.get(1L), Value.get(2L));

    /** Context after a selector has been pushed by the size / get / join terminals. */
    private static final CTX[] NESTED_CTX = { CTX.mapKey(ROOT_KEY), CTX.mapKey(Value.get("k")) };

    private record Selector(String label,
                            Consumer<CdtReadOnlyBuilder<Object>> apply,
                            IntFunction<Operation> expected,
                            boolean mapSelector,
                            boolean invertible) {
    }

    private record Terminal(String label,
                            Consumer<CdtReadOnlyBuilder<Object>> invoke,
                            int mapReturnType,
                            int listReturnType) {
    }

    // ========================================
    // Selectors
    // ========================================

    private static final List<Selector> MAP_SELECTORS = List.of(
        new Selector("onMapIndex(3)", b -> b.onMapIndex(3),
                rt -> MapOperation.getByIndex(BIN, 3, rt, ROOT_CTX), true, false),
        new Selector("onMapKey(k)", b -> b.onMapKey("k"),
                rt -> MapOperation.getByKey(BIN, Value.get("k"), rt, ROOT_CTX), true, false),
        new Selector("onMapRank(2)", b -> b.onMapRank(2),
                rt -> MapOperation.getByRank(BIN, 2, rt, ROOT_CTX), true, false),
        new Selector("onMapIndexRange(1,2)", b -> b.onMapIndexRange(1, 2),
                rt -> MapOperation.getByIndexRange(BIN, 1, 2, rt, ROOT_CTX), true, true),
        new Selector("onMapIndexRange(1)", b -> b.onMapIndexRange(1),
                rt -> MapOperation.getByIndexRange(BIN, 1, rt, ROOT_CTX), true, true),
        new Selector("onMapKeyList([k1,k2])", b -> b.onMapKeyList(List.of("k1", "k2")),
                rt -> MapOperation.getByKeyList(BIN, KEY_VALUES, rt, ROOT_CTX), true, true),
        new Selector("onMapKeyRange(a,b)", b -> b.onMapKeyRange("a", "b"),
                rt -> MapOperation.getByKeyRange(BIN, Value.get("a"), Value.get("b"), rt, ROOT_CTX), true, true),
        new Selector("onMapRankRange(1,2)", b -> b.onMapRankRange(1, 2),
                rt -> MapOperation.getByRankRange(BIN, 1, 2, rt, ROOT_CTX), true, true),
        new Selector("onMapValue(1)", b -> b.onMapValue(1L),
                rt -> MapOperation.getByValue(BIN, Value.get(1L), rt, ROOT_CTX), true, true),
        new Selector("onMapValueList([1,2])", b -> b.onMapValueList(List.of(1L, 2L)),
                rt -> MapOperation.getByValueList(BIN, NUM_VALUES, rt, ROOT_CTX), true, true),
        new Selector("onMapValueRange(1,4)", b -> b.onMapValueRange(1L, 4L),
                rt -> MapOperation.getByValueRange(BIN, Value.get(1L), Value.get(4L), rt, ROOT_CTX), true, true),
        new Selector("onMapKeyRelativeIndexRange(a,3)", b -> b.onMapKeyRelativeIndexRange("a", 3),
                rt -> MapOperation.getByKeyRelativeIndexRange(BIN, Value.get("a"), 3, rt, ROOT_CTX), true, true),
        new Selector("onMapKeyRelativeIndexRange(a,3,2)", b -> b.onMapKeyRelativeIndexRange("a", 3, 2),
                rt -> MapOperation.getByKeyRelativeIndexRange(BIN, Value.get("a"), 3, 2, rt, ROOT_CTX), true, true),
        new Selector("onMapValueRelativeRankRange(1,3)", b -> b.onMapValueRelativeRankRange(1L, 3),
                rt -> MapOperation.getByValueRelativeRankRange(BIN, Value.get(1L), 3, rt, ROOT_CTX), true, true),
        new Selector("onMapValueRelativeRankRange(1,3,2)", b -> b.onMapValueRelativeRankRange(1L, 3, 2),
                rt -> MapOperation.getByValueRelativeRankRange(BIN, Value.get(1L), 3, 2, rt, ROOT_CTX), true, true));

    private static final List<Selector> LIST_SELECTORS = List.of(
        new Selector("onListIndex(0)", b -> b.onListIndex(0),
                rt -> ListOperation.getByIndex(BIN, 0, rt, ROOT_CTX), false, false),
        new Selector("onListRank(2)", b -> b.onListRank(2),
                rt -> ListOperation.getByRank(BIN, 2, rt, ROOT_CTX), false, false),
        new Selector("onListIndexRange(1,2)", b -> b.onListIndexRange(1, 2),
                rt -> ListOperation.getByIndexRange(BIN, 1, 2, rt, ROOT_CTX), false, true),
        new Selector("onListIndexRange(1)", b -> b.onListIndexRange(1),
                rt -> ListOperation.getByIndexRange(BIN, 1, rt, ROOT_CTX), false, true),
        new Selector("onListRankRange(1,2)", b -> b.onListRankRange(1, 2),
                rt -> ListOperation.getByRankRange(BIN, 1, 2, rt, ROOT_CTX), false, true),
        new Selector("onListRankRange(1)", b -> b.onListRankRange(1),
                rt -> ListOperation.getByRankRange(BIN, 1, rt, ROOT_CTX), false, true),
        new Selector("onListValue(1)", b -> b.onListValue(1L),
                rt -> ListOperation.getByValue(BIN, Value.get(1L), rt, ROOT_CTX), false, true),
        new Selector("onListValueList([1,2])", b -> b.onListValueList(List.of(1L, 2L)),
                rt -> ListOperation.getByValueList(BIN, NUM_VALUES, rt, ROOT_CTX), false, true),
        new Selector("onListValueRange(1,4)", b -> b.onListValueRange(1L, 4L),
                rt -> ListOperation.getByValueRange(BIN, Value.get(1L), Value.get(4L), rt, ROOT_CTX), false, true),
        new Selector("onListValueRelativeRankRange(1,3)", b -> b.onListValueRelativeRankRange(1L, 3),
                rt -> ListOperation.getByValueRelativeRankRange(BIN, Value.get(1L), 3, rt, ROOT_CTX), false, true),
        new Selector("onListValueRelativeRankRange(1,3,2)", b -> b.onListValueRelativeRankRange(1L, 3, 2),
                rt -> ListOperation.getByValueRelativeRankRange(BIN, Value.get(1L), 3, 2, rt, ROOT_CTX), false, true));

    private static Stream<Selector> allSelectors() {
        return Stream.concat(MAP_SELECTORS.stream(), LIST_SELECTORS.stream());
    }

    // ========================================
    // Terminals
    // ========================================

    private static final List<Terminal> TERMINALS = List.of(
        new Terminal("count()", b -> b.count(), MapReturnType.COUNT, ListReturnType.COUNT),
        new Terminal("exists()", b -> b.exists(), MapReturnType.EXISTS, ListReturnType.EXISTS),
        new Terminal("getKeys()", b -> b.getKeys(), MapReturnType.KEY, MAP_ONLY),
        new Terminal("getIndexes()", b -> b.getIndexes(), MapReturnType.INDEX, MAP_ONLY),
        new Terminal("getReverseIndexes()", b -> b.getReverseIndexes(), MapReturnType.REVERSE_INDEX, MAP_ONLY),
        new Terminal("getRanks()", b -> b.getRanks(), MapReturnType.RANK, MAP_ONLY),
        new Terminal("getReverseRanks()", b -> b.getReverseRanks(), MapReturnType.REVERSE_RANK, MAP_ONLY),
        new Terminal("getKeysAndValues()", b -> b.getKeysAndValues(), MapReturnType.KEY_VALUE, MAP_ONLY),
        new Terminal("getAsMap()", b -> b.getAsMap(), MapReturnType.UNORDERED_MAP, MAP_ONLY),
        new Terminal("getAsOrderedMap()", b -> b.getAsOrderedMap(), MapReturnType.ORDERED_MAP, MAP_ONLY));

    private static final List<Terminal> INVERTED_TERMINALS = List.of(
        new Terminal("getAllOtherValues()", b -> b.getAllOtherValues(),
                MapReturnType.VALUE | MapReturnType.INVERTED, ListReturnType.VALUE | ListReturnType.INVERTED),
        new Terminal("countAllOthers()", b -> b.countAllOthers(),
                MapReturnType.COUNT | MapReturnType.INVERTED, ListReturnType.COUNT | ListReturnType.INVERTED),
        new Terminal("getAllOtherKeys()", b -> b.getAllOtherKeys(),
                MapReturnType.KEY | MapReturnType.INVERTED, MAP_ONLY),
        new Terminal("getAllOtherIndexes()", b -> b.getAllOtherIndexes(),
                MapReturnType.INDEX | MapReturnType.INVERTED, MAP_ONLY),
        new Terminal("getAllOtherReverseIndexes()", b -> b.getAllOtherReverseIndexes(),
                MapReturnType.REVERSE_INDEX | MapReturnType.INVERTED, MAP_ONLY),
        new Terminal("getAllOtherRanks()", b -> b.getAllOtherRanks(),
                MapReturnType.RANK | MapReturnType.INVERTED, MAP_ONLY),
        new Terminal("getAllOtherReverseRanks()", b -> b.getAllOtherReverseRanks(),
                MapReturnType.REVERSE_RANK | MapReturnType.INVERTED, MAP_ONLY),
        new Terminal("getAllOtherKeysAndValues()", b -> b.getAllOtherKeysAndValues(),
                MapReturnType.KEY_VALUE | MapReturnType.INVERTED, MAP_ONLY));

    // ========================================
    // Tests
    // ========================================

    @ParameterizedTest(name = "{0}")
    @MethodSource({ "supportedTerminals", "supportedInvertedTerminals" })
    void terminalEmitsExpectedOperation(String label, Consumer<CdtReadOnlyBuilder<Object>> chain, Operation expected) {
        assertOperation(emit(chain), expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource({ "mapOnlyTerminalsRejectListSelectors", "invertedTerminalsRejectSingleElementSelectors",
        "invertedMapOnlyTerminalsRejectListSelectors" })
    void unsupportedCombinationIsRejected(String label, Consumer<CdtReadOnlyBuilder<Object>> chain) {
        assertThrows(IllegalArgumentException.class, () -> emit(chain));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sizeAndDirectAccessTerminals")
    void nestedTerminalEmitsExpectedOperation(String label, Consumer<CdtReadOnlyBuilder<Object>> chain,
            Operation expected) {
        assertOperation(emit(chain), expected);
    }

    // ========================================
    // Case generation
    // ========================================

    private static Stream<Arguments> supportedTerminals() {
        return combine(allSelectors(), TERMINALS, false);
    }

    private static Stream<Arguments> supportedInvertedTerminals() {
        return combine(allSelectors().filter(Selector::invertible), INVERTED_TERMINALS, true);
    }

    private static Stream<Arguments> combine(Stream<Selector> selectors, List<Terminal> terminals, boolean inverted) {
        String suffix = inverted ? " [inverted]" : "";
        return selectors.flatMap(s -> terminals.stream()
                .filter(t -> returnTypeFor(s, t) != MAP_ONLY)
                .map(t -> Arguments.of(s.label() + " + " + t.label() + suffix,
                        chain(s, t),
                        s.expected().apply(returnTypeFor(s, t)))));
    }

    /** One list selector per map-only terminal: the list {@code case} labels share a single throw site. */
    private static Stream<Arguments> mapOnlyTerminalsRejectListSelectors() {
        List<Terminal> mapOnly = TERMINALS.stream().filter(t -> t.listReturnType() == MAP_ONLY).toList();
        return rotate(mapOnly, LIST_SELECTORS, "");
    }

    private static Stream<Arguments> invertedTerminalsRejectSingleElementSelectors() {
        List<Selector> singleElement = allSelectors().filter(s -> !s.invertible()).toList();
        Terminal terminal = INVERTED_TERMINALS.get(0);
        return singleElement.stream()
                .map(s -> Arguments.of(s.label() + " + " + terminal.label(), chain(s, terminal)));
    }

    /** The "not supported for list operations" guard is repeated per list case, so each needs its own selector. */
    private static Stream<Arguments> invertedMapOnlyTerminalsRejectListSelectors() {
        List<Selector> invertibleLists = LIST_SELECTORS.stream().filter(Selector::invertible).toList();
        Terminal terminal = INVERTED_TERMINALS.get(2);
        return invertibleLists.stream()
                .map(s -> Arguments.of(s.label() + " + " + terminal.label(), chain(s, terminal)));
    }

    private static Stream<Arguments> rotate(List<Terminal> terminals, List<Selector> selectors, String suffix) {
        return Stream.iterate(0, i -> i + 1).limit(terminals.size()).map(i -> {
            Terminal t = terminals.get(i);
            Selector s = selectors.get(i % selectors.size());
            return Arguments.of(s.label() + " + " + t.label() + suffix, chain(s, t));
        });
    }

    private static Stream<Arguments> sizeAndDirectAccessTerminals() {
        return Stream.of(
            Arguments.of("onMapKey(k) + mapSize()",
                    nested(b -> b.mapSize()), MapOperation.size(BIN, NESTED_CTX)),
            Arguments.of("onMapKey(k) + listSize()",
                    nested(b -> b.listSize()), ListOperation.size(BIN, NESTED_CTX)),
            Arguments.of("onMapKey(k) + listGet(1)",
                    nested(b -> b.listGet(1)), ListOperation.get(BIN, 1, NESTED_CTX)),
            Arguments.of("onMapKey(k) + listGetRange(1)",
                    nested(b -> b.listGetRange(1)), ListOperation.getRange(BIN, 1, NESTED_CTX)),
            Arguments.of("onMapKey(k) + listGetRange(1,2)",
                    nested(b -> b.listGetRange(1, 2)), ListOperation.getRange(BIN, 1, 2, NESTED_CTX)),
            Arguments.of("onMapKey(k) + listJoin()",
                    nested(b -> b.listJoin()), ListOperation.join(BIN, NESTED_CTX)),
            Arguments.of("onMapKey(k) + listJoin(sep)",
                    nested(b -> b.listJoin("-")), ListOperation.join(BIN, "-", NESTED_CTX)));
    }

    private static int returnTypeFor(Selector s, Terminal t) {
        return s.mapSelector() ? t.mapReturnType() : t.listReturnType();
    }

    private static Consumer<CdtReadOnlyBuilder<Object>> chain(Selector s, Terminal t) {
        return b -> {
            s.apply().accept(b);
            t.invoke().accept(b);
        };
    }

    private static Consumer<CdtReadOnlyBuilder<Object>> nested(Consumer<CdtReadOnlyBuilder<Object>> terminal) {
        return b -> {
            b.onMapKey("k");
            terminal.accept(b);
        };
    }
}
