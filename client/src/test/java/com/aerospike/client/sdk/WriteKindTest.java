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

import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.policy.Behavior.OpKind;

/**
 * Which behavior kind each write resolves as. The kind decides whether
 * {@code writes().retryable()} or {@code writes().nonRetryable()} settings apply.
 */
public class WriteKindTest {
    private static final OpKind RETRYABLE = OpKind.WRITE_RETRYABLE;
    private static final OpKind NON_RETRYABLE = OpKind.WRITE_NON_RETRYABLE;

    private static final List<Operation> PUT = List.of(Operation.put(new Bin("v", 1)));

    private static OpKind kind(OpType opType, List<Operation> ops) {
        return AbstractOperationBuilder.writeKind(opType, 0, ops);
    }

    @Test
    public void putsAreRetryable() {
        assertEquals(RETRYABLE, kind(OpType.UPSERT, PUT));
        assertEquals(RETRYABLE, kind(OpType.UPDATE, PUT));
        assertEquals(RETRYABLE, kind(OpType.REPLACE, PUT));
        assertEquals(RETRYABLE, kind(OpType.REPLACE_IF_EXISTS, PUT));
        assertEquals(RETRYABLE, kind(OpType.UPSERT, null));
    }

    @Test
    public void putWithReadIsRetryable() {
        assertEquals(RETRYABLE, kind(OpType.UPSERT,
            List.of(Operation.put(new Bin("v", 1)), Operation.get("v"))));
    }

    @Test
    public void deleteAndTouchAreRetryable() {
        assertEquals(RETRYABLE, kind(OpType.DELETE, null));
        assertEquals(RETRYABLE, kind(OpType.TOUCH, null));
        // Operate deleteRecord() and touch() inside an upsert.
        assertEquals(RETRYABLE, kind(OpType.UPSERT, List.of(Operation.delete())));
        assertEquals(RETRYABLE, kind(OpType.UPSERT, List.of(Operation.touch())));
    }

    @Test
    public void insertIsNotRetryable() {
        // A retry after a successful first attempt fails with KEY_EXISTS_ERROR.
        assertEquals(NON_RETRYABLE, kind(OpType.INSERT, PUT));
        assertEquals(NON_RETRYABLE, kind(OpType.INSERT, null));
    }

    @Test
    public void generationCheckIsNotRetryable() {
        // A retry after a successful first attempt fails with GENERATION_ERROR.
        for (OpType opType : new OpType[] {OpType.UPSERT, OpType.UPDATE, OpType.REPLACE,
                OpType.DELETE, OpType.TOUCH}) {
            assertEquals(NON_RETRYABLE, AbstractOperationBuilder.writeKind(opType, 5, PUT),
                opType.toString());
        }
    }

    @Test
    public void udfIsNotRetryable() {
        assertEquals(NON_RETRYABLE, kind(OpType.UDF, null));
    }

    @Test
    public void nonIdempotentOperationsAreNotRetryable() {
        assertEquals(NON_RETRYABLE, kind(OpType.UPSERT, List.of(Operation.add(new Bin("v", 1)))));
        assertEquals(NON_RETRYABLE, kind(OpType.UPSERT, List.of(Operation.append(new Bin("v", "x")))));
        assertEquals(NON_RETRYABLE, kind(OpType.UPSERT, List.of(Operation.prepend(new Bin("v", "x")))));
        assertEquals(NON_RETRYABLE, kind(OpType.UPSERT, List.of(ListOperation.append("l", Value.get(1)))));
    }

    @Test
    public void oneNonIdempotentOperationMakesTheWriteNonRetryable() {
        assertEquals(NON_RETRYABLE, kind(OpType.UPDATE,
            List.of(Operation.put(new Bin("a", 1)), Operation.add(new Bin("b", 1)))));
    }
}
