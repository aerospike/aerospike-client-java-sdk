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
import static com.aerospike.client.sdk.CdtOperationCapture.emitBin;
import static com.aerospike.client.sdk.CdtOperationCapture.emitOperate;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;

/**
 * Covers the server 8.1.2 path steps: {@code onMapKeyList} as a {@link CTX#mapKeysIn(Value...)} path segment,
 * and {@code andFilter} as a {@link CTX#andFilter(Exp)} segment. Offline; no server.
 */
class CdtPathKeyListAndFilterTest {

    private static final Exp FILTER = Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10));
    private static final Exp CHILD_FILTER = Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(100));
    private static final CTX KEYS_AB = CTX.mapKeysIn(Value.get("a"), Value.get("b"));

    // ========================================
    // Read path (CdtReadOnlyBuilder)
    // ========================================

    @Test
    void keyListThenCollectEmitsMapKeysIn() {
        assertOperation(emit(b -> b.onMapKeyList(List.of("a", "b")).collectValues()),
                CdtOperation.selectByPath(BIN, SelectFlags.VALUE, CTX.mapKey(ROOT_KEY), KEYS_AB));
    }

    @Test
    void keyListAndFilterThenCollectEmitsBothContexts() {
        assertOperation(emit(b -> b.onMapKeyList(List.of("a", "b")).andFilter(FILTER).collectKeyValues()),
                CdtOperation.selectByPath(BIN, SelectFlags.MAP_KEY_VALUE,
                        CTX.mapKey(ROOT_KEY), KEYS_AB, CTX.andFilter(FILTER)));
    }

    /** The booking example from the docs, without falling back to {@code appendOperations}. */
    @Test
    void keyListAndFilterThenEachChildMatchesTheDocsExample() {
        assertOperation(emit(b -> b.onMapKeyList(List.of(10001L, 10003L))
                        .andFilter(FILTER)
                        .onEachChild()
                        .onEachChild(CHILD_FILTER)
                        .collectTree(o -> o.noFail(true))),
                CdtOperation.selectByPath(BIN, SelectFlags.MATCHING_TREE | SelectFlags.NO_FAIL,
                        CTX.mapKey(ROOT_KEY),
                        CTX.mapKeysIn(Value.get(10001L), Value.get(10003L)),
                        CTX.andFilter(FILTER),
                        CTX.allChildren(),
                        CTX.allChildrenWithFilter(CHILD_FILTER)));
    }

    @Test
    void andFilterAfterSingleKeyIsAPathSegment() {
        assertOperation(emit(b -> b.onMapKey("k").andFilter(FILTER).collectValues()),
                CdtOperation.selectByPath(BIN, SelectFlags.VALUE,
                        CTX.mapKey(ROOT_KEY), CTX.mapKey(Value.get("k")), CTX.andFilter(FILTER)));
    }

    @Test
    void keyListSendsByteArraysAsBlobsAndAllowsMixedKeys() {
        byte[] blob = {1, 2};
        assertOperation(emit(b -> b.onMapKeyList(List.of(blob, "s", 3L)).onEachChild().collectValues()),
                CdtOperation.selectByPath(BIN, SelectFlags.VALUE, CTX.mapKey(ROOT_KEY),
                        CTX.mapKeysIn(Value.get(blob), Value.get("s"), Value.get(3L)), CTX.allChildren()));
    }

    @Test
    void keyListClassicTerminalIsUnchanged() {
        assertOperation(emit(b -> b.onMapKeyList(List.of("a", "b")).getValues()),
                MapOperation.getByKeyList(BIN, List.of(Value.get("a"), Value.get("b")), MapReturnType.VALUE,
                        CTX.mapKey(ROOT_KEY)));
    }

    @Test
    void andFilterAfterEachChildIsRejected() {
        assertThrows(IllegalStateException.class, () -> emit(b -> b.onEachChild().andFilter(FILTER)));
        assertThrows(IllegalStateException.class, () -> emit(b -> b.onEachChild(CHILD_FILTER).andFilter(FILTER)));
    }

    @Test
    void secondAndFilterAtTheSameLevelIsRejected() {
        assertThrows(IllegalStateException.class,
                () -> emit(b -> b.onMapKeyList(List.of("a")).andFilter(FILTER).andFilter(CHILD_FILTER)));
    }

    @Test
    void andFilterRejectsNull() {
        assertThrows(NullPointerException.class, () -> emit(b -> b.onMapKeyList(List.of("a")).andFilter(null)));
    }

    @Test
    void classicTerminalAfterAndFilterIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> emit(b -> b.onMapKeyList(List.of("a")).andFilter(FILTER).getValues()));
    }

    // ========================================
    // Operate path (CdtGetOrRemoveBuilder, BinBuilder)
    // ========================================

    @Test
    void operateKeyListAndFilterThenModifyBy() {
        Exp modify = Exp.val(1L);
        assertOperation(emitOperate(b -> b.onMapKeyList(List.of("a", "b")).andFilter(FILTER).modifyBy(modify)),
                CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, Exp.build(modify),
                        CTX.mapKey(ROOT_KEY), KEYS_AB, CTX.andFilter(FILTER)));
    }

    @Test
    void operateKeyListThenRemoveMatches() {
        assertOperation(emitOperate(b -> b.onMapKeyList(List.of("a", "b")).removeMatches()),
                CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, Exp.build(Exp.removeResult()),
                        CTX.mapKey(ROOT_KEY), KEYS_AB));
    }

    @Test
    void operateAndFilterAfterEachChildIsRejected() {
        assertThrows(IllegalStateException.class, () -> emitOperate(b -> b.onEachChild().andFilter(FILTER)));
    }

    @Test
    void binRootKeyListAndFilterStartsThePath() {
        assertOperation(emitBin(b -> b.onMapKeyList(List.of("a", "b")).andFilter(FILTER).onEachChild().collectTree()),
                CdtOperation.selectByPath(BIN, SelectFlags.MATCHING_TREE,
                        KEYS_AB, CTX.andFilter(FILTER), CTX.allChildren()));
    }
}
