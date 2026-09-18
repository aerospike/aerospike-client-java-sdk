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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapPolicy;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.vector.Vector;

/** Verifies VECTOR detection in CDT values and contexts. */
class CdtVectorFlagTest {
    private static final String BIN = "b";

    private static Value vectorValue() {
        return Value.get(Vector.ofFloat32(new float[] {1.0f, 2.0f}));
    }

    private static CTX vectorCtx() {
        return CTX.mapKey(Value.get(Vector.ofFloat32(new float[] {3.0f, 4.0f})));
    }

    @Test
    void listAppendValueVectorIsFlagged() {
        assertTrue(ListOperation.append(BIN, vectorValue()).value.hasVector());
    }

    @Test
    void listAppendItemsVectorIsFlagged() {
        assertTrue(ListOperation.appendItems(BIN, List.of(Value.get(1L), vectorValue())).value.hasVector());
    }

    @Test
    void listGetByValueVectorIsFlagged() {
        assertTrue(ListOperation.getByValue(BIN, vectorValue(), ListReturnType.VALUE).value.hasVector());
    }

    @Test
    void listIncrementVectorIsFlagged() {
        assertTrue(ListOperation.increment(BIN, 0, vectorValue()).value.hasVector());
    }

    @Test
    void listOperationVectorInContextIsFlagged() {
        // Plain scalar value, but the navigation context carries a vector map key.
        assertTrue(ListOperation.append(BIN, Value.get(1L), vectorCtx()).value.hasVector());
    }

    @Test
    void listPlainOperationIsNotFlagged() {
        assertFalse(ListOperation.append(BIN, Value.get(1L)).value.hasVector());
        assertFalse(ListOperation.getByIndex(BIN, 0, ListReturnType.VALUE).value.hasVector());
    }

    @Test
    void mapPutValueVectorIsFlagged() {
        assertTrue(MapOperation.put(MapPolicy.Default, BIN, Value.get("k"), vectorValue()).value.hasVector());
    }

    @Test
    void mapGetByKeyVectorIsFlagged() {
        assertTrue(MapOperation.getByKey(BIN, vectorValue(), MapReturnType.VALUE).value.hasVector());
    }

    @Test
    void mapOperationVectorInContextIsFlagged() {
        assertTrue(MapOperation.put(MapPolicy.Default, BIN, Value.get("k"), Value.get(1L), vectorCtx())
            .value.hasVector());
    }

    @Test
    void contextExpressionWithVectorIsFlagged() {
        CTX filtered = CTX.allChildrenWithFilter(Exp.vectorBin("embedding"));

        assertTrue(ListOperation.size(BIN, filtered).value.hasVector());
        assertTrue(MapOperation.size(BIN, filtered).value.hasVector());
        assertTrue(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, filtered).value.hasVector());
        assertTrue(CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT,
            Exp.build(Exp.vectorBin("replacement")), filtered).value.hasVector());
    }

    @Test
    void filterContextWithVectorIsFlagged() {
        assertTrue(Filter.equal("idx", 1L, vectorCtx()).hasVector());
    }

    @Test
    void filterExpressionWithVectorIsFlagged() {
        assertTrue(Filter.equal(Exp.build(Exp.vectorBin("embedding")), 1L).hasVector());
    }

    @Test
    void mapPutItemsContextExpressionWithVectorIsFlagged() {
        CTX filtered = CTX.allChildrenWithFilter(Exp.vectorBin("embedding"));
        assertTrue(MapOperation.putItems(MapPolicy.Default, BIN,
            Map.of(Value.get("k"), Value.get(1L)), filtered).value.hasVector());
    }

    @Test
    void mapPlainOperationIsNotFlagged() {
        assertFalse(MapOperation.put(MapPolicy.Default, BIN, Value.get("k"), Value.get(1L)).value.hasVector());
        assertFalse(MapOperation.getByIndex(BIN, 0, MapReturnType.VALUE).value.hasVector());
    }
}
