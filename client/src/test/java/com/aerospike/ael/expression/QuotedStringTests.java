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
package com.aerospike.ael.expression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

/**
 * Documents current quoting behaviour: no C-style escapes in {@code QUOTED_STRING}
 * (see {@code ParsingUtils#unquote}).
 */
public class QuotedStringTests {

    @Test
    void quotedStringBackslashIsLiteralNotAnEscape() {
        // AEL source: $.s == "a\b"  → value is three characters: a, backslash, b
        Exp expected = Exp.eq(Exp.stringBin("s"), Exp.val("a\\b"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.s == \"a\\b\""), expected);
    }

    @Test
    void quotedStringBackslashNIsNotNewline() {
        Exp expected = Exp.eq(Exp.stringBin("s"), Exp.val("\\n"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.s == \"\\n\""), expected);
    }
}
