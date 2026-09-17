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
import static com.aerospike.client.sdk.CdtOperationCapture.emitOperate;
import static com.aerospike.client.sdk.CdtOperationCapture.newOperateBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.CdtOperationCapture.CapturingOperationBuilder;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.path.CdtCollectOptions;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapWriteFlags;
import com.aerospike.client.sdk.exp.Exp;

/**
 * Asserts that the query path and the operate path select the same data.
 *
 * <p>{@link CdtReadOnlyBuilder} and {@link CdtGetOrRemoveBuilder} declare 134 selector overloads with
 * byte-identical signatures, and each maintains its own copy of the navigation logic. Restating the
 * per-overload expectations here would duplicate {@code CdtReadOnlyBuilderSelectorTest} rather than test
 * anything new, so this test pairs the two implementations off against each other: for every shared
 * signature, both builders are driven with the same arguments and must emit the same operation.</p>
 *
 * <p>That covers the operate-path selector surface and, unlike a second copy of the tables, it fails when
 * the two implementations drift apart. If the duplication is ever collapsed into one implementation, this
 * test becomes redundant and should be deleted.</p>
 */
class CdtSelectorParityTest {

    /**
     * Lower bound on the shared surface, so a reflection or filtering mistake that silently discovers
     * nothing fails loudly instead of reporting a green run over zero methods.
     */
    private static final int EXPECTED_SHARED_SELECTORS = 130;

    private static Stream<Arguments> sharedSelectors() {
        Map<String, Method> readOnly = selectorMethods(CdtReadOnlyBuilder.class);
        return selectorMethods(CdtGetOrRemoveBuilder.class).entrySet().stream()
            .filter(entry -> readOnly.containsKey(entry.getKey()))
            .map(entry -> Arguments.of(entry.getKey(), readOnly.get(entry.getKey()), entry.getValue()));
    }

    private static Map<String, Method> selectorMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> !m.isSynthetic() && !m.isBridge())
            .filter(m -> m.getName().startsWith("onMap") || m.getName().startsWith("onList"))
            .filter(m -> Arrays.stream(m.getParameterTypes()).allMatch(CdtSelectorParityTest::canSynthesize))
            .collect(Collectors.toMap(CdtSelectorParityTest::signature, m -> m, (a, b) -> a, LinkedHashMap::new));
    }

    private static String signature(Method method) {
        return method.getName() + Arrays.stream(method.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(",", "(", ")"));
    }

    private static boolean canSynthesize(Class<?> type) {
        return type == long.class || type == int.class || type == double.class || type == boolean.class
            || type == String.class || type == byte[].class || type == List.class || type == Map.class
            || type == SpecialValue.class || type == MapOrder.class
            || type.getSimpleName().equals("ListOrder");
    }

    /**
     * Values are chosen only to be legal for every overload; the assertion is that both builders do the
     * same thing with them, so the specific values do not matter as long as both sides get the same ones.
     */
    private static Object[] argumentsFor(Method method) {
        return Arrays.stream(method.getParameterTypes()).map(type -> {
            if (type == long.class) {
                return 1L;
            }
            if (type == int.class) {
                return 2;
            }
            if (type == double.class) {
                return 1.5d;
            }
            if (type == boolean.class) {
                return Boolean.TRUE;
            }
            if (type == String.class) {
                return "a";
            }
            if (type == byte[].class) {
                return new byte[] { 1, 2 };
            }
            if (type == List.class) {
                return List.of(1L, 2L);
            }
            if (type == Map.class) {
                return Map.of("a", 1L);
            }
            if (type == SpecialValue.class) {
                return SpecialValue.NULL;
            }
            if (type == MapOrder.class) {
                return MapOrder.UNORDERED;
            }
            return type.getEnumConstants()[0];
        }).toArray();
    }

    private static void invoke(Method method, Object target, Object[] args) {
        try {
            method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw new IllegalStateException(signature(method) + " threw", e.getCause());
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(signature(method) + " is not accessible", e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sharedSelectors")
    void queryAndOperatePathsSelectTheSameData(String signature, Method readOnly, Method operate) {
        Object[] args = argumentsFor(operate);

        Operation fromQueryPath = emit(builder -> {
            invoke(readOnly, builder, args);
            builder.getValues();
        });
        Operation fromOperatePath = emitOperate(
            builder -> invoke(operate, builder, args),
            CdtGetOrRemoveBuilder::getValues);

        assertOperation(fromOperatePath, fromQueryPath);
    }

    // ========================================
    // Bin-level entry points
    // ========================================

    private static Stream<Arguments> binEntrySelectors() {
        Map<String, Method> operate = selectorMethods(CdtGetOrRemoveBuilder.class);
        return selectorMethods(BinBuilder.class).entrySet().stream()
            .filter(entry -> operate.containsKey(entry.getKey()))
            .map(entry -> Arguments.of(entry.getKey(), entry.getValue(), operate.get(entry.getKey())));
    }

    /**
     * {@link BinBuilder} declares a third copy of the same selector surface, this time as the entry point:
     * instead of navigating an existing path it constructs a fresh {@link CdtGetOrRemoveBuilder} with the
     * equivalent {@link CdtOperationParams}.
     *
     * <p>The two cannot be compared by emitted operation, because the entry point starts at the bin root
     * with no context while the navigator has already pushed a path segment. What must agree is the
     * selection itself, so that is what is compared.</p>
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("binEntrySelectors")
    void binEntryPointSelectsTheSameThingAsTheNestedNavigator(String signature, Method binEntry, Method operate) {
        Object[] args = argumentsFor(binEntry);

        BinBuilder<CapturingOperationBuilder> bin = new BinBuilder<>(new CapturingOperationBuilder(), BIN);
        AbstractCdtBuilder<?> fromEntryPoint = (AbstractCdtBuilder<?>) invokeReturning(binEntry, bin, args);

        // Navigating from two unrelated roots reveals which fields the selector actually sets: anything
        // that still differs afterwards was left behind by the root rather than chosen by the selector.
        CdtOperationParams fromKeyRoot = navigate(operate, args, new CdtOperationParams(CdtOperation.MAP_BY_KEY,
                Value.get("root")));
        CdtOperationParams fromIndexRoot = navigate(operate, args, new CdtOperationParams(CdtOperation.LIST_BY_INDEX,
                9));

        assertEquals(fromKeyRoot.getOperation(), fromEntryPoint.params.getOperation(), "operation");
        assertSelectorSetFields(fromEntryPoint.params, fromKeyRoot, fromIndexRoot);
    }

    private static CdtOperationParams navigate(Method operate, Object[] args, CdtOperationParams root) {
        CdtGetOrRemoveBuilder<CapturingOperationBuilder> nested =
            new CdtGetOrRemoveBuilder<>(BIN, new CapturingOperationBuilder(), root);
        invoke(operate, nested, args);
        return nested.params;
    }

    /**
     * Asserts the entry point matches the navigator on every field the selector determines. Fields the
     * navigator inherited from its root are skipped: see
     * {@link #navigatorKeepsValuesTheNewSelectionDoesNotUse()}.
     */
    private static void assertSelectorSetFields(CdtOperationParams actual,
                                                CdtOperationParams a, CdtOperationParams b) {
        assertFieldIfDetermined("first value", actual.getVal1(), a.getVal1(), b.getVal1());
        assertFieldIfDetermined("second value", actual.getVal2(), a.getVal2(), b.getVal2());
        assertFieldIfDetermined("first int", actual.getInt1(), a.getInt1(), b.getInt1());
        assertFieldIfDetermined("second int", actual.getInt2(), a.getInt2(), b.getInt2());
        assertFieldIfDetermined("value list", actual.getValues(), a.getValues(), b.getValues());
        assertFieldIfDetermined("map create order", actual.getMapCreateType(), a.getMapCreateType(),
                b.getMapCreateType());
        assertFieldIfDetermined("list create order", actual.getListCreateType(), a.getListCreateType(),
                b.getListCreateType());
        assertFieldIfDetermined("pad", actual.isPad(), a.isPad(), b.isPad());
    }

    private static void assertFieldIfDetermined(String field, Object actual, Object fromOneRoot,
                                                Object fromAnotherRoot) {
        if (Objects.equals(fromOneRoot, fromAnotherRoot)) {
            assertEquals(fromOneRoot, actual, field);
        }
    }

    /**
     * {@code pushCurrentToContextAndReplaceWith} overwrites only the fields the incoming selection needs,
     * so a positional selector leaves the previous selection's key sitting in {@code val1}. Nothing in the
     * read or remove path reads that field for a positional selection, but the write terminals do, which
     * is how {@code onListValue(...).setTo(...)} ends up writing to a map key.
     *
     * <p>This pins current behaviour; it is a defect, not the intended contract.</p>
     */
    @Test
    @Tag(KnownDefect.TAG)
    void navigatorKeepsValuesTheNewSelectionDoesNotUse() {
        CdtGetOrRemoveBuilder<CapturingOperationBuilder> nested =
            newOperateBuilder(new CapturingOperationBuilder());

        nested.onListIndex(3);

        KnownDefect.pinned(
            "pushCurrentToContextAndReplaceWith overwrites only the fields the incoming selection needs, so "
                + "the previous selection's key survives in val1; it should be cleared. This is the root cause "
                + "of the silent map puts pinned in CdtGetOrRemoveBuilderWriteTest and "
                + "CdtGetOrRemoveBuilderPathTest",
            () -> {
                assertEquals(CdtOperation.LIST_BY_INDEX, nested.params.getOperation());
                assertEquals(3, nested.params.getInt1());
                assertEquals(CdtOperationCapture.ROOT_KEY, nested.params.getVal1(),
                        "the key from the previous selection is still there");
            });
    }

    private static Object invokeReturning(Method method, Object target, Object[] args) {
        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw new IllegalStateException(signature(method) + " threw", e.getCause());
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(signature(method) + " is not accessible", e);
        }
    }

    // ========================================
    // Shared path-expression terminals
    // ========================================

    private static Stream<Arguments> sharedCollectTerminals() {
        Map<String, Method> readOnly = collectMethods(CdtReadOnlyBuilder.class);
        return collectMethods(CdtGetOrRemoveBuilder.class).entrySet().stream()
            .filter(entry -> readOnly.containsKey(entry.getKey()))
            .map(entry -> Arguments.of(entry.getKey(), readOnly.get(entry.getKey()), entry.getValue()));
    }

    private static Map<String, Method> collectMethods(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> !m.isSynthetic() && !m.isBridge())
            .filter(m -> m.getName().startsWith("collect"))
            .filter(m -> Arrays.stream(m.getParameterTypes()).allMatch(CdtSelectorParityTest::canSynthesizeCollectArg))
            .collect(Collectors.toMap(CdtSelectorParityTest::signature, m -> m, (a, b) -> a, LinkedHashMap::new));
    }

    private static boolean canSynthesizeCollectArg(Class<?> type) {
        return type == CdtCollectOptions.class || type == ExpressionReadOptions.class
            || type == Consumer.class || type == Exp.Type.class || type == int.class;
    }

    private static Object[] collectArgumentsFor(Method method) {
        Class<?>[] types = method.getParameterTypes();
        Type[] genericTypes = method.getGenericParameterTypes();
        Object[] args = new Object[types.length];

        for (int i = 0; i < types.length; i++) {
            if (types[i] == CdtCollectOptions.class) {
                args[i] = new CdtCollectOptions().noFail(true);
            }
            else if (types[i] == ExpressionReadOptions.class) {
                args[i] = new ExpressionReadOptions();
            }
            else if (types[i] == Consumer.class) {
                args[i] = optionsConsumerFor(genericTypes[i]);
            }
            else if (types[i] == int.class) {
                // The flag arguments are positional; the default of both flag sets is zero.
                args[i] = 0;
            }
            else {
                args[i] = Exp.Type.MAP;
            }
        }
        return args;
    }

    /** The {@code collect*} overloads take consumers of two different option types. */
    private static Consumer<?> optionsConsumerFor(Type genericType) {
        Type optionType = ((ParameterizedType) genericType).getActualTypeArguments()[0];
        if (optionType == ExpressionReadOptions.class) {
            return (Consumer<ExpressionReadOptions>) ExpressionReadOptions::ignoreEvalFailure;
        }
        return (Consumer<CdtCollectOptions>) o -> o.noFail(true);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sharedCollectTerminals")
    void queryAndOperatePathsCollectTheSameData(String signature, Method readOnly, Method operate) {
        Operation fromQueryPath = emit(builder -> {
            builder.onEachChild();
            invoke(readOnly, builder, collectArgumentsFor(readOnly));
        });
        Operation fromOperatePath = emitOperate(
            CdtGetOrRemoveBuilder::onEachChild,
            builder -> invoke(operate, builder, collectArgumentsFor(operate)));

        assertOperation(fromOperatePath, fromQueryPath);
    }

    @Test
    void bothBuildersDeclareTheWholeSharedSelectorSurface() {
        long shared = sharedSelectors().count();
        assertTrue(shared >= EXPECTED_SHARED_SELECTORS,
                "expected at least " + EXPECTED_SHARED_SELECTORS + " shared selectors, found " + shared);
        assertEquals(selectorMethods(CdtReadOnlyBuilder.class).size(), shared,
                "every query-path selector should have an operate-path counterpart");
    }

    /**
     * The three selectors the operate path adds. The map order they carry is not part of the selection,
     * so it cannot be checked by parity: it only surfaces later, as the order of the map a write creates.
     */
    @Test
    void mapKeyWithOrderSetsTheOrderUsedWhenAWriteCreatesTheMap() {
        Operation expected = MapOperation.put(
                AbstractCdtBuilder.cachedMapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT, false),
                BIN, Value.get("k"), Value.get(7L), ROOT_CTX);

        assertOperation(emitOperate(b -> b.onMapKey("k", MapOrder.UNORDERED), b -> b.setTo(7L)), expected);
        assertOperation(emitOperate(b -> b.onMapKey(1L, MapOrder.UNORDERED), b -> b.setTo(7L)),
                MapOperation.put(AbstractCdtBuilder.cachedMapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT, false),
                        BIN, Value.get(1L), Value.get(7L), ROOT_CTX));
        assertOperation(emitOperate(b -> b.onMapKey(new byte[] { 9 }, MapOrder.UNORDERED), b -> b.setTo(7L)),
                MapOperation.put(AbstractCdtBuilder.cachedMapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT, false),
                        BIN, Value.get(new byte[] { 9 }), Value.get(7L), ROOT_CTX));
    }
}
