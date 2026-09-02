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
import static com.aerospike.client.sdk.CdtOperationCapture.emitOperate;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.CdtOperationCapture.CapturingOperationBuilder;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapWriteFlags;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.path.CdtModifyOptions;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Covers the write half of the {@code onEachChild} path surface, which only
 * {@link CdtGetOrRemoveBuilder} has: {@code modifyBy} and {@code removeMatches}.
 *
 * <p>The {@code collect*} half is shared with {@link CdtReadOnlyBuilder} and is covered by
 * {@code CdtSelectorParityTest} instead, so it is deliberately not repeated here.</p>
 */
class CdtGetOrRemoveBuilderPathTest {

    /** Context after {@code onEachChild()} on a builder rooted at one map key. */
    private static final CTX[] EACH_CHILD_CTX = { CTX.mapKey(ROOT_KEY), CTX.allChildren() };

    private static final Exp MODIFY_EXP = Exp.val(1L);

    @ParameterizedTest(name = "{0}")
    @MethodSource("modifyTerminals")
    void modifyEmitsModifyByPath(String label,
                                 Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> terminal,
                                 Exp expected, int expectedFlags) {
        assertOperation(emitOperate(CdtGetOrRemoveBuilder::onEachChild, terminal),
                com.aerospike.client.sdk.cdt.CdtOperation.modifyByPath(
                        BIN, expectedFlags, Exp.build(expected), EACH_CHILD_CTX));
    }

    private static Stream<Arguments> modifyTerminals() {
        return Stream.of(
            Arguments.of("modifyBy(Exp)", terminal(b -> b.modifyBy(MODIFY_EXP)),
                    MODIFY_EXP, ModifyFlags.DEFAULT),
            Arguments.of("modifyBy(Exp, Consumer)", terminal(b -> b.modifyBy(MODIFY_EXP, o -> o.noFail(true))),
                    MODIFY_EXP, ModifyFlags.NO_FAIL),
            Arguments.of("modifyBy(Exp, options)", terminal(b -> b.modifyBy(MODIFY_EXP, noFail())),
                    MODIFY_EXP, ModifyFlags.NO_FAIL),
            Arguments.of("removeMatches()", terminal(b -> b.removeMatches()),
                    Exp.removeResult(), ModifyFlags.DEFAULT),
            Arguments.of("removeMatches(Consumer)", terminal(b -> b.removeMatches(o -> o.noFail(true))),
                    Exp.removeResult(), ModifyFlags.NO_FAIL),
            Arguments.of("removeMatches(options)", terminal(b -> b.removeMatches(noFail())),
                    Exp.removeResult(), ModifyFlags.NO_FAIL));
    }

    /**
     * The AEL overloads are declared so the fluent surface is complete, but the client cannot yet send
     * AEL to the server. Every one of them throws before doing any work.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("aelTerminals")
    void aelPathOverloadsAreNotSupported(String label,
                                         Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> terminal) {
        assertThrows(UnsupportedOperationException.class,
                () -> emitOperate(CdtGetOrRemoveBuilder::onEachChild, terminal));
    }

    private static Stream<Arguments> aelTerminals() {
        PreparedAel prepared = new PreparedAel("x == ?");
        return Stream.of(
            Arguments.of("onEachChild(String)", terminal(b -> b.onEachChild("$.x > 1"))),
            Arguments.of("onEachChild(PreparedAel)", terminal(b -> b.onEachChild(prepared, 1))),
            Arguments.of("modifyBy(String)", terminal(b -> b.modifyBy("$.x = 1"))),
            Arguments.of("modifyBy(String, Consumer)", terminal(b -> b.modifyBy("$.x = 1", o -> o.noFail(true)))),
            Arguments.of("modifyBy(String, options)", terminal(b -> b.modifyBy("$.x = 1", noFail()))),
            Arguments.of("modifyBy(PreparedAel)", terminal(b -> b.modifyBy(prepared, 1))),
            Arguments.of("modifyBy(PreparedAel, Consumer)",
                    terminal(b -> b.modifyBy(prepared, o -> o.noFail(true), 1))),
            Arguments.of("modifyBy(PreparedAel, options)", terminal(b -> b.modifyBy(prepared, noFail(), 1))));
    }

    /**
     * {@code collectValuesAsExpressionRead} reads the bin through a typed expression, so the declared bin
     * type has to reach the wire. Maps, lists, and scalars each take a different branch.
     */
    @Test
    void expressionReadEncodesTheDeclaredBinType() {
        Operation asMap = expressionRead(Exp.Type.MAP);
        Operation asList = expressionRead(Exp.Type.LIST);
        Operation asInt = expressionRead(Exp.Type.INT);

        assertNotEquals(asMap.value, asList.value);
        assertNotEquals(asMap.value, asInt.value);
        assertNotEquals(asList.value, asInt.value);
    }

    @Test
    void modifyRequiresAtLeastOneEachChildSegment() {
        assertThrows(IllegalStateException.class,
                () -> emitOperate(b -> b.onMapKey("k"), b -> b.modifyBy(MODIFY_EXP)));
        assertThrows(IllegalStateException.class,
                () -> emitOperate(b -> b.onMapKey("k"), b -> b.removeMatches()));
    }

    /**
     * {@code onEachChild()} selects a whole subtree, which none of the classic single-selection terminals
     * knows how to encode. The chains below compile and fail at runtime.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedAfterEachChild")
    void classicTerminalAfterEachChildIsRejected(String label,
                                                 Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> terminal) {
        assertThrows(IllegalArgumentException.class,
                () -> emitOperate(CdtGetOrRemoveBuilder::onEachChild, terminal));
    }

    private static Stream<Arguments> rejectedAfterEachChild() {
        return Stream.of(
            Arguments.of("getValues()", terminal(b -> b.getValues())),
            Arguments.of("count()", terminal(b -> b.count())),
            Arguments.of("remove()", terminal(b -> b.remove())),
            Arguments.of("removeAnd().getValues()", terminal(b -> b.removeAnd().getValues())));
    }

    /**
     * The write terminals branch on {@code LIST_BY_INDEX} and treat everything else as a map key, so they
     * never notice that {@code onEachChild()} selected a subtree rather than a single entry. Instead of
     * writing to every child, the chain below writes one entry at the key the path started from, and the
     * {@code onEachChild()} segment is dropped entirely.
     *
     * <p>This pins current behaviour; it is a defect, not the intended contract.</p>
     */
    @Test
    @Tag(KnownDefect.TAG)
    void writeAfterEachChildSilentlyTargetsTheParentKey() {
        KnownDefect.pinned(
            "onEachChild().setTo(1L) should either write every child or be rejected, but the write terminals "
                + "do not recognise ALL_CHILDREN, so the onEachChild() segment is dropped and one entry is "
                + "written at the key the path started from",
            () -> assertOperation(emitOperate(CdtGetOrRemoveBuilder::onEachChild, b -> b.setTo(1L)),
                    com.aerospike.client.sdk.cdt.MapOperation.put(
                            AbstractCdtBuilder.cachedMapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT, false),
                            BIN, ROOT_KEY, Value.get(1L), CdtOperationCapture.ROOT_CTX)));
    }

    // ========================================
    // Helpers
    // ========================================

    private static Operation expressionRead(Exp.Type binValueType) {
        return emitOperate(CdtGetOrRemoveBuilder::onEachChild,
                b -> b.collectValuesAsExpressionRead(binValueType, Exp.Type.LIST));
    }

    private static CdtModifyOptions noFail() {
        return new CdtModifyOptions().noFail(true);
    }

    private static Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> terminal(
            Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> terminal) {
        return terminal;
    }
}
