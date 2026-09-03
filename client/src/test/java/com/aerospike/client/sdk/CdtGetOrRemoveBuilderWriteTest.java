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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtOperationCapture.CapturingOperationBuilder;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapPolicy;
import com.aerospike.client.sdk.cdt.MapWriteFlags;
import com.aerospike.client.sdk.mapper.Address;
import com.aerospike.client.sdk.mapper.AddressMapper;

/**
 * Covers the 86 write terminals on {@link CdtGetOrRemoveBuilder}.
 *
 * <p>The write surface is a cross-product of three independent dimensions: the verb
 * ({@code setTo}/{@code insert}/{@code update}/{@code upsert}/{@code add}), the Java type of the value,
 * and how write options are supplied (omitted, as a lambda, or as a prepared object). Every overload
 * body is the same three lines, so the table below records only which overload to call, and the
 * expected operation is derived from the three dimensions rather than restated per case.</p>
 *
 * <p>Each case is then driven twice, once against a map key and once against a list index, because the
 * verb means something different on each: {@code setTo} becomes a list {@code set} or a map {@code put},
 * while {@code update} and {@code upsert} are map-only and must be rejected on a list.</p>
 */
class CdtGetOrRemoveBuilderWriteTest {

    private enum Verb { SET_TO, INSERT, UPDATE, UPSERT, ADD }

    /** How write options reach the overload. Each form exercises a different branch of the policy builder. */
    private enum Opts { NONE, CONSUMER, EXPLICIT }

    private static final Value MAP_KEY = Value.get("k");
    private static final int LIST_INDEX = 2;

    private static final byte[] BYTES = { 1, 2, 3 };
    private static final List<?> LIST_VALUE = List.of(1L, 2L);
    private static final Map<?, ?> MAP_VALUE = Map.of("a", 1L);
    private static final Address ADDRESS = new Address("1 Main St", "Springfield", "IL", "US", "62701");
    private static final AddressMapper MAPPER = new AddressMapper();

    /** Non-default so the assertions fail if the options are dropped on the way to the map policy. */
    private static final Consumer<MapEntryWriteOptions> CONSUMER_OPTS =
        o -> o.allowFailures().mapOrder(MapOrder.UNORDERED);
    private static final MapEntryWriteOptions EXPLICIT_OPTS =
        new MapEntryWriteOptions().persistIndex().mapOrder(MapOrder.KEY_VALUE_ORDERED);

    private static Arguments write(Verb verb, Opts opts, String valueLabel, Value expected,
                                   Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> invoke) {
        String label = verb.name().toLowerCase() + "(" + valueLabel + ")"
            + switch (opts) {
                case NONE -> "";
                case CONSUMER -> " + options lambda";
                case EXPLICIT -> " + options object";
            };
        return Arguments.of(label, verb, opts, expected, invoke);
    }

    private static Stream<Arguments> writes() {
        return Stream.of(
            // setTo has no options overloads: it always writes with default map flags.
            write(Verb.SET_TO, Opts.NONE, "long", Value.get(7L), b -> b.setTo(7L)),
            write(Verb.SET_TO, Opts.NONE, "String", Value.get("s"), b -> b.setTo("s")),
            write(Verb.SET_TO, Opts.NONE, "byte[]", Value.get(BYTES), b -> b.setTo(BYTES)),
            write(Verb.SET_TO, Opts.NONE, "boolean", Value.get(true), b -> b.setTo(true)),
            write(Verb.SET_TO, Opts.NONE, "double", Value.get(1.5d), b -> b.setTo(1.5d)),
            write(Verb.SET_TO, Opts.NONE, "List", Value.get(LIST_VALUE), b -> b.setTo(LIST_VALUE)),
            write(Verb.SET_TO, Opts.NONE, "Map", Value.get(MAP_VALUE), b -> b.setTo(MAP_VALUE)),
            write(Verb.SET_TO, Opts.NONE, "mapped", mappedValue(), b -> b.setTo(ADDRESS, MAPPER)),

            write(Verb.INSERT, Opts.NONE, "long", Value.get(7L), b -> b.insert(7L)),
            write(Verb.INSERT, Opts.NONE, "String", Value.get("s"), b -> b.insert("s")),
            write(Verb.INSERT, Opts.NONE, "byte[]", Value.get(BYTES), b -> b.insert(BYTES)),
            write(Verb.INSERT, Opts.NONE, "boolean", Value.get(true), b -> b.insert(true)),
            write(Verb.INSERT, Opts.NONE, "double", Value.get(1.5d), b -> b.insert(1.5d)),
            write(Verb.INSERT, Opts.NONE, "List", Value.get(LIST_VALUE), b -> b.insert(LIST_VALUE)),
            write(Verb.INSERT, Opts.NONE, "Map", Value.get(MAP_VALUE), b -> b.insert(MAP_VALUE)),
            write(Verb.INSERT, Opts.NONE, "mapped", mappedValue(), b -> b.insert(ADDRESS, MAPPER)),
            write(Verb.INSERT, Opts.CONSUMER, "long", Value.get(7L), b -> b.insert(7L, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "String", Value.get("s"), b -> b.insert("s", CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "byte[]", Value.get(BYTES), b -> b.insert(BYTES, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "boolean", Value.get(true), b -> b.insert(true, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "double", Value.get(1.5d), b -> b.insert(1.5d, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "List", Value.get(LIST_VALUE), b -> b.insert(LIST_VALUE, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "Map", Value.get(MAP_VALUE), b -> b.insert(MAP_VALUE, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.CONSUMER, "mapped", mappedValue(), b -> b.insert(ADDRESS, MAPPER, CONSUMER_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "long", Value.get(7L), b -> b.insert(7L, EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "String", Value.get("s"), b -> b.insert("s", EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "byte[]", Value.get(BYTES), b -> b.insert(BYTES, EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "boolean", Value.get(true), b -> b.insert(true, EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "double", Value.get(1.5d), b -> b.insert(1.5d, EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "List", Value.get(LIST_VALUE), b -> b.insert(LIST_VALUE, EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "Map", Value.get(MAP_VALUE), b -> b.insert(MAP_VALUE, EXPLICIT_OPTS)),
            write(Verb.INSERT, Opts.EXPLICIT, "mapped", mappedValue(), b -> b.insert(ADDRESS, MAPPER, EXPLICIT_OPTS)),

            write(Verb.UPDATE, Opts.NONE, "long", Value.get(7L), b -> b.update(7L)),
            write(Verb.UPDATE, Opts.NONE, "String", Value.get("s"), b -> b.update("s")),
            write(Verb.UPDATE, Opts.NONE, "byte[]", Value.get(BYTES), b -> b.update(BYTES)),
            write(Verb.UPDATE, Opts.NONE, "boolean", Value.get(true), b -> b.update(true)),
            write(Verb.UPDATE, Opts.NONE, "double", Value.get(1.5d), b -> b.update(1.5d)),
            write(Verb.UPDATE, Opts.NONE, "List", Value.get(LIST_VALUE), b -> b.update(LIST_VALUE)),
            write(Verb.UPDATE, Opts.NONE, "Map", Value.get(MAP_VALUE), b -> b.update(MAP_VALUE)),
            write(Verb.UPDATE, Opts.NONE, "mapped", mappedValue(), b -> b.update(ADDRESS, MAPPER)),
            write(Verb.UPDATE, Opts.CONSUMER, "long", Value.get(7L), b -> b.update(7L, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "String", Value.get("s"), b -> b.update("s", CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "byte[]", Value.get(BYTES), b -> b.update(BYTES, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "boolean", Value.get(true), b -> b.update(true, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "double", Value.get(1.5d), b -> b.update(1.5d, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "List", Value.get(LIST_VALUE), b -> b.update(LIST_VALUE, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "Map", Value.get(MAP_VALUE), b -> b.update(MAP_VALUE, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.CONSUMER, "mapped", mappedValue(), b -> b.update(ADDRESS, MAPPER, CONSUMER_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "long", Value.get(7L), b -> b.update(7L, EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "String", Value.get("s"), b -> b.update("s", EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "byte[]", Value.get(BYTES), b -> b.update(BYTES, EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "boolean", Value.get(true), b -> b.update(true, EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "double", Value.get(1.5d), b -> b.update(1.5d, EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "List", Value.get(LIST_VALUE), b -> b.update(LIST_VALUE, EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "Map", Value.get(MAP_VALUE), b -> b.update(MAP_VALUE, EXPLICIT_OPTS)),
            write(Verb.UPDATE, Opts.EXPLICIT, "mapped", mappedValue(), b -> b.update(ADDRESS, MAPPER, EXPLICIT_OPTS)),

            write(Verb.UPSERT, Opts.NONE, "long", Value.get(7L), b -> b.upsert(7L)),
            write(Verb.UPSERT, Opts.NONE, "String", Value.get("s"), b -> b.upsert("s")),
            write(Verb.UPSERT, Opts.NONE, "byte[]", Value.get(BYTES), b -> b.upsert(BYTES)),
            write(Verb.UPSERT, Opts.NONE, "boolean", Value.get(true), b -> b.upsert(true)),
            write(Verb.UPSERT, Opts.NONE, "double", Value.get(1.5d), b -> b.upsert(1.5d)),
            write(Verb.UPSERT, Opts.NONE, "List", Value.get(LIST_VALUE), b -> b.upsert(LIST_VALUE)),
            write(Verb.UPSERT, Opts.NONE, "Map", Value.get(MAP_VALUE), b -> b.upsert(MAP_VALUE)),
            write(Verb.UPSERT, Opts.NONE, "mapped", mappedValue(), b -> b.upsert(ADDRESS, MAPPER)),
            write(Verb.UPSERT, Opts.CONSUMER, "long", Value.get(7L), b -> b.upsert(7L, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "String", Value.get("s"), b -> b.upsert("s", CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "byte[]", Value.get(BYTES), b -> b.upsert(BYTES, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "boolean", Value.get(true), b -> b.upsert(true, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "double", Value.get(1.5d), b -> b.upsert(1.5d, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "List", Value.get(LIST_VALUE), b -> b.upsert(LIST_VALUE, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "Map", Value.get(MAP_VALUE), b -> b.upsert(MAP_VALUE, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.CONSUMER, "mapped", mappedValue(), b -> b.upsert(ADDRESS, MAPPER, CONSUMER_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "long", Value.get(7L), b -> b.upsert(7L, EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "String", Value.get("s"), b -> b.upsert("s", EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "byte[]", Value.get(BYTES), b -> b.upsert(BYTES, EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "boolean", Value.get(true), b -> b.upsert(true, EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "double", Value.get(1.5d), b -> b.upsert(1.5d, EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "List", Value.get(LIST_VALUE), b -> b.upsert(LIST_VALUE, EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "Map", Value.get(MAP_VALUE), b -> b.upsert(MAP_VALUE, EXPLICIT_OPTS)),
            write(Verb.UPSERT, Opts.EXPLICIT, "mapped", mappedValue(), b -> b.upsert(ADDRESS, MAPPER, EXPLICIT_OPTS)),

            // add is numeric only.
            write(Verb.ADD, Opts.NONE, "long", Value.get(7L), b -> b.add(7L)),
            write(Verb.ADD, Opts.NONE, "double", Value.get(1.5d), b -> b.add(1.5d)),
            write(Verb.ADD, Opts.CONSUMER, "long", Value.get(7L), b -> b.add(7L, CONSUMER_OPTS)),
            write(Verb.ADD, Opts.CONSUMER, "double", Value.get(1.5d), b -> b.add(1.5d, CONSUMER_OPTS)),
            write(Verb.ADD, Opts.EXPLICIT, "long", Value.get(7L), b -> b.add(7L, EXPLICIT_OPTS)),
            write(Verb.ADD, Opts.EXPLICIT, "double", Value.get(1.5d), b -> b.add(1.5d, EXPLICIT_OPTS))
        );
    }

    private static Value mappedValue() {
        return Value.get(MAPPER.toMap(ADDRESS));
    }

    /**
     * Mirrors {@code CdtGetOrRemoveBuilder.resolveMapPolicy}: the verb picks the base write flag and the
     * options contribute the map order, the no-fail flag, and the persist-index flag.
     */
    private static MapPolicy expectedPolicy(Verb verb, Opts opts) {
        int flags = switch (verb) {
            case INSERT -> MapWriteFlags.CREATE_ONLY;
            case UPDATE -> MapWriteFlags.UPDATE_ONLY;
            default -> MapWriteFlags.DEFAULT;
        };
        return switch (opts) {
            case NONE -> AbstractCdtBuilder.cachedMapPolicy(MapOrder.KEY_ORDERED, flags, false);
            case CONSUMER -> AbstractCdtBuilder.cachedMapPolicy(
                MapOrder.UNORDERED, flags | MapWriteFlags.NO_FAIL, false);
            case EXPLICIT -> AbstractCdtBuilder.cachedMapPolicy(MapOrder.KEY_VALUE_ORDERED, flags, true);
        };
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writes")
    void writeAtMapKeyCompilesToMapPutOrIncrement(String label, Verb verb, Opts opts, Value value,
                                                  Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> invoke) {
        MapPolicy policy = expectedPolicy(verb, opts);
        Operation expected = verb == Verb.ADD
            ? MapOperation.increment(policy, BIN, MAP_KEY, value, ROOT_CTX)
            : MapOperation.put(policy, BIN, MAP_KEY, value, ROOT_CTX);

        assertOperation(emitOperate(b -> b.onMapKey("k"), invoke), expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writes")
    void writeAtListIndexCompilesToListSetInsertOrIncrement(String label, Verb verb, Opts opts, Value value,
                                                            Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> invoke) {
        if (verb == Verb.UPDATE || verb == Verb.UPSERT) {
            assertThrows(IllegalArgumentException.class,
                    () -> emitOperate(b -> b.onListIndex(LIST_INDEX), invoke),
                    "update/upsert have no list equivalent");
            return;
        }

        Operation expected = switch (verb) {
            case SET_TO -> ListOperation.set(BIN, LIST_INDEX, value, ROOT_CTX);
            case INSERT -> ListOperation.insert(BIN, LIST_INDEX, value, ROOT_CTX);
            case ADD -> ListOperation.increment(BIN, LIST_INDEX, value, ROOT_CTX);
            default -> throw new IllegalStateException(verb.name());
        };

        assertOperation(emitOperate(b -> b.onListIndex(LIST_INDEX), invoke), expected);
    }

    /**
     * The list branch of every write terminal tests for {@code LIST_BY_INDEX} specifically, so any other
     * list selector falls through to the map branch and emits a map {@code put} keyed by the value that
     * was being selected. The chain below compiles and produces a well-formed but meaningless operation.
     *
     * <p>This pins current behaviour; it is a defect, not the intended contract.</p>
     */
    @Test
    @Tag(KnownDefect.TAG)
    void writeAfterNonIndexListSelectorSilentlyBecomesMapPut() {
        MapPolicy policy = expectedPolicy(Verb.SET_TO, Opts.NONE);

        KnownDefect.pinned(
            "onListValue(1L).setTo(7L) should either write the matching list element or be rejected, but the "
                + "write terminals only recognise LIST_BY_INDEX, so it emits a map put keyed by the value that "
                + "was being selected",
            () -> assertOperation(emitOperate(b -> b.onListValue(1L), b -> b.setTo(7L)),
                    MapOperation.put(policy, BIN, Value.get(1L), Value.get(7L), ROOT_CTX)));
    }
}
