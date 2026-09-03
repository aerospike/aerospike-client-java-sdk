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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.cdt.CTX;

/**
 * Stands in for the parent operation builder so CDT builders can be driven without a cluster.
 *
 * <p>{@link CdtReadOnlyBuilder} takes a {@link CdtOperationAcceptor} rather than a concrete builder,
 * so the query-path CDT surface can be exercised entirely offline: build a chain, capture the
 * {@link Operation} it emits, and compare it against the equivalent direct {@code MapOperation} or
 * {@code ListOperation} call.</p>
 */
final class CdtOperationCapture implements CdtOperationAcceptor<Object> {

    static final String BIN = "cdtBin";
    static final Value ROOT_KEY = Value.get("root");

    /** Every chain starts nested under one map key, so emitted operations carry a non-empty context. */
    static final CTX[] ROOT_CTX = { CTX.mapKey(ROOT_KEY) };

    private final List<Operation> ops = new ArrayList<>();
    private final Object parent = new Object();

    @Override
    public void acceptOp(Operation op) {
        ops.add(op);
    }

    @Override
    public Object getParentBuilder() {
        return parent;
    }

    static CdtReadOnlyBuilder<Object> newReadOnlyBuilder(CdtOperationCapture capture) {
        return new CdtReadOnlyBuilder<>(BIN, capture, new CdtOperationParams(CdtOperation.MAP_BY_KEY, ROOT_KEY));
    }

    /** Runs {@code chain} against a fresh builder and returns the one operation it emitted. */
    static Operation emit(Consumer<CdtReadOnlyBuilder<Object>> chain) {
        CdtOperationCapture capture = new CdtOperationCapture();
        chain.accept(newReadOnlyBuilder(capture));
        assertEquals(1, capture.ops.size(), "expected exactly one emitted operation");
        return capture.ops.get(0);
    }

    static void assertOperation(Operation actual, Operation expected) {
        assertEquals(expected.type, actual.type, "operation type");
        assertEquals(expected.binName, actual.binName, "bin name");
        assertEquals(expected.value, actual.value, "packed operation payload");
    }

    // ========================================
    // Operate path (CdtGetOrRemoveBuilder, BinBuilder)
    // ========================================

    /**
     * A {@link Session} with no cluster behind it. The constructor only assigns two fields and
     * {@code getCurrentTransaction()} is unconditionally {@code null} on the base class, so builders
     * can be constructed and driven as long as nothing calls {@code execute()}.
     */
    private static final class OfflineSession extends Session {
        OfflineSession() {
            super(null, null);
        }
    }

    /**
     * Minimal concrete {@link AbstractOperationBuilder} that exposes the operations its CDT and bin
     * builders queue up. The real chainable builders keep their operation list private.
     */
    static final class CapturingOperationBuilder extends AbstractOperationBuilder<CapturingOperationBuilder> {
        CapturingOperationBuilder() {
            super(new OfflineSession(), OpType.UPSERT);
        }

        List<Operation> captured() {
            return ops;
        }
    }

    static CdtGetOrRemoveBuilder<CapturingOperationBuilder> newOperateBuilder(CapturingOperationBuilder parent) {
        return new CdtGetOrRemoveBuilder<>(BIN, parent, new CdtOperationParams(CdtOperation.MAP_BY_KEY, ROOT_KEY));
    }

    /** Operate-path equivalent of {@link #emit}: drives {@link CdtGetOrRemoveBuilder} nested under one map key. */
    static Operation emitOperate(Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> chain) {
        return single(emitAllOperate(chain));
    }

    static List<Operation> emitAllOperate(Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> chain) {
        CapturingOperationBuilder parent = new CapturingOperationBuilder();
        chain.accept(newOperateBuilder(parent));
        return parent.captured();
    }

    /**
     * Applies {@code selector} and then {@code terminal} to the same builder instance.
     *
     * <p>{@link CdtGetOrRemoveBuilder} funnels every read through one dispatch switch and every removal
     * through another, so the selector and terminal dimensions are independent. Selector methods mutate
     * the shared {@link CdtOperationParams} and return {@code this}, which lets the two dimensions be
     * combined here rather than needing a lambda per pair.</p>
     */
    static Operation emitOperate(Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> selector,
                                 Consumer<CdtGetOrRemoveBuilder<CapturingOperationBuilder>> terminal) {
        CapturingOperationBuilder parent = new CapturingOperationBuilder();
        CdtGetOrRemoveBuilder<CapturingOperationBuilder> builder = newOperateBuilder(parent);
        selector.accept(builder);
        terminal.accept(builder);
        return single(parent.captured());
    }

    /** Drives a top-level {@link BinBuilder}, i.e. the entry point before any selector is applied. */
    static Operation emitBin(Consumer<BinBuilder<CapturingOperationBuilder>> chain) {
        CapturingOperationBuilder parent = new CapturingOperationBuilder();
        chain.accept(new BinBuilder<>(parent, BIN));
        return single(parent.captured());
    }

    private static Operation single(List<Operation> ops) {
        assertEquals(1, ops.size(), "expected exactly one emitted operation");
        return ops.get(0);
    }
}
