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

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.StringExp;
import com.aerospike.client.sdk.operation.StringNumericType;
import com.aerospike.client.sdk.operation.StringOperation;
import com.aerospike.client.sdk.operation.StringRegexFlags;
import com.aerospike.client.sdk.operation.StringWriteFlags;

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

    @ParameterizedTest(name = "{0}")
    @MethodSource("readPackers")
    public void stringExpReadPackersCompile(String label, Supplier<Exp> packer) {
        assertCompiles(packer.get(), label);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("modifyPackers")
    public void stringExpModifyPackersCompile(String label, Supplier<Exp> packer) {
        assertCompiles(packer.get(), label);
    }

    private static Stream<Arguments> readPackers() {
        Exp s = Exp.stringBin("s");
        return Stream.of(
            Arguments.of("strlen", (Supplier<Exp>) () -> StringExp.strlen(s)),
            Arguments.of("substr(offset)", (Supplier<Exp>) () -> StringExp.substr(Exp.val(2), s)),
            Arguments.of("substr(offset, count)", (Supplier<Exp>) () -> StringExp.substr(Exp.val(0), Exp.val(4), s)),
            Arguments.of("charAt", (Supplier<Exp>) () -> StringExp.charAt(Exp.val(1), s)),
            Arguments.of("find(needle)", (Supplier<Exp>) () -> StringExp.find(Exp.val("x"), s)),
            Arguments.of("find(needle, offset)", (Supplier<Exp>) () -> StringExp.find(Exp.val("x"), Exp.val(-1), s)),
            Arguments.of("contains", (Supplier<Exp>) () -> StringExp.contains(Exp.val("x"), s)),
            Arguments.of("startsWith", (Supplier<Exp>) () -> StringExp.startsWith(Exp.val("x"), s)),
            Arguments.of("endsWith", (Supplier<Exp>) () -> StringExp.endsWith(Exp.val("x"), s)),
            Arguments.of("toInteger", (Supplier<Exp>) () -> StringExp.toInteger(s)),
            Arguments.of("toDouble", (Supplier<Exp>) () -> StringExp.toDouble(s)),
            Arguments.of("byteLength", (Supplier<Exp>) () -> StringExp.byteLength(s)),
            Arguments.of("isNumeric", (Supplier<Exp>) () -> StringExp.isNumeric(s)),
            Arguments.of("isNumeric(INT)", (Supplier<Exp>) () -> StringExp.isNumeric(StringNumericType.INT, s)),
            Arguments.of("isUpper", (Supplier<Exp>) () -> StringExp.isUpper(s)),
            Arguments.of("isLower", (Supplier<Exp>) () -> StringExp.isLower(s)),
            Arguments.of("toBlob", (Supplier<Exp>) () -> StringExp.toBlob(s)),
            Arguments.of("split", (Supplier<Exp>) () -> StringExp.split(s)),
            Arguments.of("split(delimiter)", (Supplier<Exp>) () -> StringExp.split(Exp.val(","), s)),
            Arguments.of("b64Decode", (Supplier<Exp>) () -> StringExp.b64Decode(s)),
            Arguments.of("regexCompare", (Supplier<Exp>) () -> StringExp.regexCompare(Exp.val(".*"), s)),
            Arguments.of("regexCompare(flags)",
                (Supplier<Exp>) () -> StringExp.regexCompare(Exp.val(".*"), StringRegexFlags.GLOBAL, s)),
            Arguments.of("toString", (Supplier<Exp>) () -> StringExp.toString(Exp.intBin("n"))));
    }

    private static Stream<Arguments> modifyPackers() {
        Exp s = Exp.stringBin("s");
        int flags = StringWriteFlags.DEFAULT;
        return Stream.of(
            Arguments.of("insert",
                (Supplier<Exp>) () -> StringExp.insert(flags, Exp.val(1), Exp.val("x"), s)),
            Arguments.of("overwrite",
                (Supplier<Exp>) () -> StringExp.overwrite(flags, Exp.val(1), Exp.val("x"), s)),
            Arguments.of("concat",
                (Supplier<Exp>) () -> StringExp.concat(flags, Exp.val(List.of("a", "b")), s)),
            Arguments.of("append", (Supplier<Exp>) () -> StringExp.append(flags, Exp.val("!"), s)),
            Arguments.of("prepend", (Supplier<Exp>) () -> StringExp.prepend(flags, Exp.val(">"), s)),
            Arguments.of("snip", (Supplier<Exp>) () -> StringExp.snip(flags, Exp.val(1), s)),
            Arguments.of("snip(count)",
                (Supplier<Exp>) () -> StringExp.snip(flags, Exp.val(1), Exp.val(3), s)),
            Arguments.of("replace",
                (Supplier<Exp>) () -> StringExp.replace(flags, Exp.val("a"), Exp.val("b"), s)),
            Arguments.of("replaceAll",
                (Supplier<Exp>) () -> StringExp.replaceAll(flags, Exp.val("a"), Exp.val("b"), s)),
            Arguments.of("upper", (Supplier<Exp>) () -> StringExp.upper(flags, s)),
            Arguments.of("lower", (Supplier<Exp>) () -> StringExp.lower(flags, s)),
            Arguments.of("caseFold", (Supplier<Exp>) () -> StringExp.caseFold(flags, s)),
            Arguments.of("normalizeNFC", (Supplier<Exp>) () -> StringExp.normalizeNFC(flags, s)),
            Arguments.of("trimStart", (Supplier<Exp>) () -> StringExp.trimStart(flags, s)),
            Arguments.of("trimEnd", (Supplier<Exp>) () -> StringExp.trimEnd(flags, s)),
            Arguments.of("trim", (Supplier<Exp>) () -> StringExp.trim(flags, s)),
            Arguments.of("padStart",
                (Supplier<Exp>) () -> StringExp.padStart(flags, Exp.val(8), Exp.val("0"), s)),
            Arguments.of("padEnd",
                (Supplier<Exp>) () -> StringExp.padEnd(flags, Exp.val(8), Exp.val("."), s)),
            Arguments.of("repeat", (Supplier<Exp>) () -> StringExp.repeat(flags, Exp.val(2), s)),
            Arguments.of("regexReplace",
                (Supplier<Exp>) () -> StringExp.regexReplace(
                    flags, Exp.val("[0-9]+"), Exp.val("X"), StringRegexFlags.GLOBAL, s)));
    }

    private static void assertCompiles(Exp exp, String label) {
        Expression compiled = Exp.build(exp);
        assertTrue(compiled.getBytes().length > 0, label);
    }
}
