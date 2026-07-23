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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.StringExp;
import com.aerospike.client.sdk.operation.StringOperation;

/**
 * Unit tests (no cluster) for {@link StringOperation} wire payloads and {@link StringExp} packing.
 * Integration coverage with {@link ChainableOperationBuilder#appendOperations} and
 * {@code selectFrom(StringExp...)} lives in {@link OperateStringTest}.
 */
public class StringApiPackagingTest {

    @Test
    public void stringOperationStrlenIsStringRead() {
        Operation op = StringOperation.strlen("msg");
        assertEquals(Operation.Type.STRING_READ, op.type);
        assertEquals("msg", op.binName);
    }

    @Test
    public void stringOperationSubstrOverloadShapes() {
        Operation suffix = StringOperation.substr("msg", 2);
        Operation slice = StringOperation.substr("msg", 1, 4);
        assertEquals(Operation.Type.STRING_READ, suffix.type);
        assertEquals(Operation.Type.STRING_READ, slice.type);
    }

    @Test
    public void stringExpStrlenCompilesToExpressionBytes() {
        Expression compiled = Exp.build(StringExp.strlen(Exp.stringBin("s")));
        assertTrue(compiled.getBytes().length > 0);
    }

    @Test
    public void stringExpSubstrOneBoundedFormCompiles() {
        Expression compiled = Exp.build(StringExp.substr(Exp.val(2), Exp.stringBin("s")));
        assertTrue(compiled.getBytes().length > 0);
    }

    @Test
    public void stringExpFindCompiles() {
        Expression compiled = Exp.build(StringExp.find(Exp.val("x"), Exp.stringBin("s")));
        assertTrue(compiled.getBytes().length > 0);
    }
}
