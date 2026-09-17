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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtOperationCapture.CapturingOperationBuilder;

/**
 * Covers the {@code Consumer<Options>} half of every {@link BinBuilder} operation that takes write options.
 *
 * <p>Fifty-odd operations on {@code BinBuilder} come in three forms: no options, a prepared options object,
 * and a lambda that configures one. The lambda form is always the same three lines — build a fresh options
 * object, hand it to the caller, delegate to the object form — so what needs checking is that the lambda's
 * configuration actually survives the delegation, for every operation, rather than what any individual
 * operation encodes.</p>
 *
 * <p>Pairing the two forms off against each other states exactly that, and does it without a table that
 * would have to be extended by hand every time an operation is added.</p>
 */
class BinBuilderOptionsTest {

    /** Lower bound on the pairs found, so a filtering mistake cannot quietly pass over an empty set. */
    private static final int EXPECTED_PAIRS = 30;

    private static Stream<Arguments> optionsPairs() {
        List<Method> all = Arrays.stream(BinBuilder.class.getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> !m.isSynthetic() && !m.isBridge())
            .collect(Collectors.toList());

        List<Arguments> pairs = new ArrayList<>();
        for (Method lambdaForm : all) {
            int consumerAt = indexOfConsumer(lambdaForm);
            if (consumerAt < 0 || !canSynthesizeAll(lambdaForm, consumerAt)) {
                continue;
            }
            Class<?> optionType = OptionKind.of(lambdaForm).type;
            findObjectForm(all, lambdaForm, consumerAt, optionType)
                .ifPresent(objectForm -> pairs.add(
                    Arguments.of(describe(lambdaForm), lambdaForm, objectForm, consumerAt)));
        }
        return pairs.stream();
    }

    /**
     * Every option type is configured through a no-argument flag setter, so one flag is enough to tell a
     * lambda that was applied from one that was dropped.
     */
    private enum OptionKind {
        STRING(StringWriteOptions.class, o -> ((StringWriteOptions) o).noFail()),
        BIT(BitWriteOptions.class, o -> ((BitWriteOptions) o).noFail()),
        HLL(HllWriteOptions.class, o -> ((HllWriteOptions) o).noFail());

        private final Class<?> type;
        private final Consumer<Object> configure;

        OptionKind(Class<?> type, Consumer<Object> configure) {
            this.type = type;
            this.configure = configure;
        }

        static OptionKind of(Method lambdaForm) {
            String signature = lambdaForm.toGenericString();
            for (OptionKind kind : values()) {
                if (signature.contains("Consumer<" + kind.type.getName() + ">")) {
                    return kind;
                }
            }
            return null;
        }

        Object prepared() {
            try {
                Object options = type.getDeclaredConstructor().newInstance();
                configure.accept(options);
                return options;
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException("cannot instantiate " + type, e);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("optionsPairs")
    void lambdaFormConfiguresTheSameOptionsAsTheObjectForm(String label, Method lambdaForm, Method objectForm,
                                                           int consumerAt) {
        OptionKind kind = OptionKind.of(lambdaForm);

        Object[] lambdaArgs = arguments(lambdaForm);
        lambdaArgs[consumerAt] = (Consumer<Object>) kind.configure::accept;

        Object[] objectArgs = arguments(objectForm);
        objectArgs[consumerAt] = kind.prepared();

        assertOperation(emit(lambdaForm, lambdaArgs), emit(objectForm, objectArgs));
    }

    @Test
    void everyOptionsOperationIsPaired() {
        long pairs = optionsPairs().count();
        assertTrue(pairs >= EXPECTED_PAIRS, "expected at least " + EXPECTED_PAIRS + " pairs, found " + pairs);
    }

    /**
     * The lambda form must not be distinguishable from the object form by the flags alone: an unconfigured
     * lambda has to produce the same operation as a default options object, or the delegation is dropping
     * the caller's object rather than their configuration.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("optionsPairs")
    void emptyLambdaMatchesDefaultOptions(String label, Method lambdaForm, Method objectForm, int consumerAt) {
        OptionKind kind = OptionKind.of(lambdaForm);

        Object[] lambdaArgs = arguments(lambdaForm);
        lambdaArgs[consumerAt] = (Consumer<Object>) options -> { };

        Object[] objectArgs = arguments(objectForm);
        try {
            objectArgs[consumerAt] = kind.type.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }

        assertOperation(emit(lambdaForm, lambdaArgs), emit(objectForm, objectArgs));
    }

    // ========================================
    // Helpers
    // ========================================

    private static Operation emit(Method method, Object[] args) {
        CapturingOperationBuilder parent = new CapturingOperationBuilder();
        BinBuilder<CapturingOperationBuilder> bin = new BinBuilder<>(parent, BIN);
        try {
            method.invoke(bin, args);
        }
        catch (InvocationTargetException e) {
            throw new IllegalStateException(describe(method) + " threw", e.getCause());
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(describe(method) + " is not accessible", e);
        }
        assertEquals(1, parent.captured().size(), describe(method) + " should emit one operation");
        return parent.captured().get(0);
    }

    private static int indexOfConsumer(Method method) {
        Class<?>[] types = method.getParameterTypes();
        for (int i = 0; i < types.length; i++) {
            if (types[i] == Consumer.class) {
                return i;
            }
        }
        return -1;
    }

    /** Finds the sibling that takes the options object where this one takes a lambda. */
    private static java.util.Optional<Method> findObjectForm(List<Method> candidates, Method lambdaForm,
                                                             int consumerAt, Class<?> optionType) {
        Class<?>[] wanted = lambdaForm.getParameterTypes().clone();
        wanted[consumerAt] = optionType;
        return candidates.stream()
            .filter(m -> m.getName().equals(lambdaForm.getName()))
            .filter(m -> Arrays.equals(m.getParameterTypes(), wanted))
            .findFirst();
    }

    private static boolean canSynthesizeAll(Method method, int consumerAt) {
        if (OptionKind.of(method) == null || isExpressionOperation(method)) {
            return false;
        }
        Class<?>[] types = method.getParameterTypes();
        for (int i = 0; i < types.length; i++) {
            if (i != consumerAt && !canSynthesize(types[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@code selectFrom} and friends compile an expression against the cluster's minimum server version,
     * so they cannot be driven without one. Their options plumbing is covered by integration tests.
     */
    private static boolean isExpressionOperation(Method method) {
        return method.getName().endsWith("From");
    }

    private static boolean canSynthesize(Class<?> type) {
        return type == int.class || type == long.class || type == boolean.class || type == byte[].class
            || type == String.class || type == List.class || type == HllConfig.class;
    }

    private static Object[] arguments(Method method) {
        return Arrays.stream(method.getParameterTypes()).map(type -> {
            if (type == int.class) {
                return 1;
            }
            if (type == long.class) {
                return 1L;
            }
            if (type == boolean.class) {
                return Boolean.TRUE;
            }
            if (type == byte[].class) {
                return new byte[] { 1, 2 };
            }
            if (type == String.class) {
                return "a";
            }
            if (type == List.class) {
                return List.of("a");
            }
            if (type == HllConfig.class) {
                return HllConfig.of(8);
            }
            return null;
        }).toArray();
    }

    private static String describe(Method method) {
        return method.getName() + Arrays.stream(method.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(",", "(", ")"));
    }
}
