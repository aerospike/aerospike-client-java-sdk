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
import static com.aerospike.client.sdk.CdtOperationCapture.ROOT_KEY;
import static com.aerospike.client.sdk.CdtOperationCapture.assertOperation;
import static com.aerospike.client.sdk.CdtOperationCapture.emit;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.cdt.path.CdtCollectOptions;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpReadFlags;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Covers the {@code onEachChild} path surface of {@link CdtReadOnlyBuilder}: the {@code collect*}
 * terminals, the expression-read terminals, and the combinations the builder refuses.
 *
 * <p>The rejection cases are the interesting half. {@code onEachChild()} sets
 * {@code ALL_CHILDREN}, which no classic read terminal handles, so
 * {@code onEachChild().getValues()} compiles but throws — see {@code selectorAfterEachChildIsRejected}.</p>
 */
class CdtReadOnlyBuilderPathTest {

    /** Context after {@code onEachChild()} on a builder rooted at one map key. */
    private static final CTX[] EACH_CHILD_CTX = { CTX.mapKey(ROOT_KEY), CTX.allChildren() };

    @ParameterizedTest(name = "{0}")
    @MethodSource("collectTerminals")
    void collectEmitsSelectByPath(String label, Consumer<CdtReadOnlyBuilder<Object>> terminal, Operation expected) {
        assertOperation(emit(afterEachChild(terminal)), expected);
    }

    private static Stream<Arguments> collectTerminals() {
        return Stream.of(
            Arguments.of("collectValues()", terminal(b -> b.collectValues()), selectByPath(SelectFlags.VALUE)),
            Arguments.of("collectValues(Consumer)", terminal(b -> b.collectValues(o -> o.noFail(true))),
                    selectByPath(SelectFlags.VALUE | SelectFlags.NO_FAIL)),
            Arguments.of("collectValues(options)", terminal(b -> b.collectValues(noFail())),
                    selectByPath(SelectFlags.VALUE | SelectFlags.NO_FAIL)),
            Arguments.of("collectKeys()", terminal(b -> b.collectKeys()), selectByPath(SelectFlags.MAP_KEY)),
            Arguments.of("collectKeys(Consumer)", terminal(b -> b.collectKeys(o -> o.noFail(true))),
                    selectByPath(SelectFlags.MAP_KEY | SelectFlags.NO_FAIL)),
            Arguments.of("collectKeys(options)", terminal(b -> b.collectKeys(noFail())),
                    selectByPath(SelectFlags.MAP_KEY | SelectFlags.NO_FAIL)),
            Arguments.of("collectKeyValues()", terminal(b -> b.collectKeyValues()),
                    selectByPath(SelectFlags.MAP_KEY_VALUE)),
            Arguments.of("collectKeyValues(Consumer)", terminal(b -> b.collectKeyValues(o -> o.noFail(true))),
                    selectByPath(SelectFlags.MAP_KEY_VALUE | SelectFlags.NO_FAIL)),
            Arguments.of("collectKeyValues(options)", terminal(b -> b.collectKeyValues(noFail())),
                    selectByPath(SelectFlags.MAP_KEY_VALUE | SelectFlags.NO_FAIL)),
            Arguments.of("collectTree()", terminal(b -> b.collectTree()), selectByPath(SelectFlags.MATCHING_TREE)),
            Arguments.of("collectTree(Consumer)", terminal(b -> b.collectTree(o -> o.noFail(true))),
                    selectByPath(SelectFlags.MATCHING_TREE | SelectFlags.NO_FAIL)),
            Arguments.of("collectTree(options)", terminal(b -> b.collectTree(noFail())),
                    selectByPath(SelectFlags.MATCHING_TREE | SelectFlags.NO_FAIL)));
    }

    @Test
    void expressionReadOverloadsAgreeWithTheExplicitForm() {
        Operation explicitDefaults = emit(afterEachChild(b -> b.collectValuesAsExpressionRead(
                Exp.Type.MAP, Exp.Type.LIST, SelectFlags.VALUE, ExpReadFlags.DEFAULT)));

        assertOperation(emit(afterEachChild(b -> b.collectValuesAsExpressionRead(Exp.Type.MAP, Exp.Type.LIST))),
                explicitDefaults);
        assertOperation(emit(afterEachChild(b -> b.collectValuesAsExpressionRead(
                Exp.Type.MAP, Exp.Type.LIST, new ExpressionReadOptions()))), explicitDefaults);

        Operation explicitNoFail = emit(afterEachChild(b -> b.collectValuesAsExpressionRead(
                Exp.Type.MAP, Exp.Type.LIST, SelectFlags.VALUE, ExpReadFlags.EVAL_NO_FAIL)));
        assertOperation(emit(afterEachChild(b -> b.collectValuesAsExpressionRead(
                Exp.Type.MAP, Exp.Type.LIST, o -> o.ignoreEvalFailure()))), explicitNoFail);
    }

    @Test
    void expressionReadEncodesTheDeclaredBinType() {
        Operation asMap = expressionRead(Exp.Type.MAP);
        Operation asList = expressionRead(Exp.Type.LIST);
        Operation asInt = expressionRead(Exp.Type.INT);

        assertNotEquals(asMap.value, asList.value);
        assertNotEquals(asMap.value, asInt.value);
        assertNotEquals(asList.value, asInt.value);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedAfterEachChild")
    void selectorAfterEachChildIsRejected(String label, Consumer<CdtReadOnlyBuilder<Object>> terminal) {
        assertThrows(IllegalArgumentException.class, () -> emit(afterEachChild(terminal)));
    }

    private static Stream<Arguments> rejectedAfterEachChild() {
        return Stream.of(
            Arguments.of("getValues()", terminal(b -> b.getValues())),
            Arguments.of("count()", terminal(b -> b.count())),
            Arguments.of("exists()", terminal(b -> b.exists())),
            Arguments.of("getAllOtherValues()", terminal(b -> b.getAllOtherValues())));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mapOnlyCollectTerminals")
    void mapOnlyCollectRejectsListSelector(String label, Consumer<CdtReadOnlyBuilder<Object>> terminal) {
        assertThrows(IllegalArgumentException.class, () -> emit(b -> {
            b.onEachChild();
            b.onListIndex(0);
            terminal.accept(b);
        }));
    }

    private static Stream<Arguments> mapOnlyCollectTerminals() {
        return Stream.of(
            Arguments.of("collectKeys()", terminal(b -> b.collectKeys())),
            Arguments.of("collectKeys(options)", terminal(b -> b.collectKeys(noFail()))),
            Arguments.of("collectKeyValues()", terminal(b -> b.collectKeyValues())),
            Arguments.of("collectKeyValues(options)", terminal(b -> b.collectKeyValues(noFail()))));
    }

    @Test
    void collectRequiresAtLeastOneEachChildSegment() {
        assertThrows(IllegalStateException.class, () -> emit(b -> {
            b.onMapKey("k");
            b.collectTree();
        }));
    }

    @Test
    void aelPathOverloadsAreNotSupported() {
        assertThrows(UnsupportedOperationException.class, () -> emit(b -> b.onEachChild("$.x > 1")));
        assertThrows(UnsupportedOperationException.class,
                () -> emit(b -> b.onEachChild(new PreparedAel("x == ?"), 1)));
    }

    // ========================================
    // Helpers
    // ========================================

    private static Operation selectByPath(int flags) {
        return com.aerospike.client.sdk.cdt.CdtOperation.selectByPath(BIN, flags, EACH_CHILD_CTX);
    }

    private static Operation expressionRead(Exp.Type binValueType) {
        return emit(afterEachChild(b -> b.collectValuesAsExpressionRead(binValueType, Exp.Type.LIST)));
    }

    private static CdtCollectOptions noFail() {
        return new CdtCollectOptions().noFail(true);
    }

    private static Consumer<CdtReadOnlyBuilder<Object>> terminal(Consumer<CdtReadOnlyBuilder<Object>> terminal) {
        return terminal;
    }

    private static Consumer<CdtReadOnlyBuilder<Object>> afterEachChild(Consumer<CdtReadOnlyBuilder<Object>> terminal) {
        return b -> {
            b.onEachChild();
            terminal.accept(b);
        };
    }
}
