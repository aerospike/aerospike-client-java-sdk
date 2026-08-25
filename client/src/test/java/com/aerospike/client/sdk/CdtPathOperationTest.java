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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.LoopVarPart;

/**
 * Client-side tests for {@link CdtOperation} packing and validation (no server).
 */
class CdtPathOperationTest {

    private static final String BIN = "cdtPath";
    private static final Expression MODIFY_EXP =
        Exp.build(Exp.mul(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(2)));

    @ParameterizedTest
    @NullSource
    @EmptySource
    @MethodSource("overLengthBinNames")
    void selectByPathRejectsInvalidBinName(String binName) {
        assertThatThrownBy(() -> CdtOperation.selectByPath(binName, SelectFlags.VALUE))
            .isInstanceOf(AerospikeException.class)
            .extracting(ex -> ((AerospikeException) ex).getResultCode())
            .isEqualTo(ResultCode.PARAMETER_ERROR);
    }

    @ParameterizedTest
    @NullSource
    @EmptySource
    @MethodSource("overLengthBinNames")
    void modifyByPathRejectsInvalidBinName(String binName) {
        assertThatThrownBy(() -> CdtOperation.modifyByPath(binName, ModifyFlags.DEFAULT, MODIFY_EXP))
            .isInstanceOf(AerospikeException.class)
            .extracting(ex -> ((AerospikeException) ex).getResultCode())
            .isEqualTo(ResultCode.PARAMETER_ERROR);
    }

    static Stream<String> overLengthBinNames() {
        return Stream.of("x".repeat(Bin.MAX_BIN_NAME_LENGTH + 1));
    }

    @Test
    void selectByPathEmptyContextMatchesOmittedContextPacking() {
        Operation omitted = CdtOperation.selectByPath(BIN, SelectFlags.VALUE);
        Operation emptyCtx = CdtOperation.selectByPath(BIN, SelectFlags.VALUE, new CTX[0]);

        assertThat(omitted.type).isEqualTo(emptyCtx.type);
        assertThat(omitted.binName).isEqualTo(emptyCtx.binName);
        assertArrayEquals(cdtPackedBytes(omitted), cdtPackedBytes(emptyCtx));
    }

    @Test
    void modifyByPathEmptyContextMatchesOmittedContextPacking() {
        Expression modifyExp = Exp.build(Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(5)));

        Operation omitted = CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, modifyExp);
        Operation emptyCtx = CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, modifyExp, new CTX[0]);

        assertThat(omitted.type).isEqualTo(emptyCtx.type);
        assertThat(omitted.binName).isEqualTo(emptyCtx.binName);
        assertArrayEquals(cdtPackedBytes(omitted), cdtPackedBytes(emptyCtx));
    }

    private static byte[] cdtPackedBytes(Operation operation) {
        Value.BytesValue bytesValue = assertInstanceOf(Value.BytesValue.class, operation.value);
        return (byte[]) bytesValue.getObject();
    }
}
