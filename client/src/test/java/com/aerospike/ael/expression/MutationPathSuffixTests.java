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
import com.aerospike.client.sdk.exp.Expression;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Grammar accepts no-arg mutation-style path suffixes ({@code remove()}, etc.), but the compiler
 * does not implement mutations: they fold into the same read ({@code get}) as omitting the suffix
 * (see {@code PathOperandUtils.processPathFunction} default). These tests pin that behavior.
 */
public class MutationPathSuffixTests {

    private static final String[] NO_ARG_MUTATION_STYLE_SUFFIXES = {
            "remove", "insert", "set", "append", "increment", "clear", "sort"
    };

    @Test
    void mapPathWithMutationStyleSuffixCompilesSameAsPlainPath() {
        Expression baseline = Exp.build(parseFilterExp(ExpressionContext.of("$.mapBin1.a == 200")));
        for (String name : NO_ARG_MUTATION_STYLE_SUFFIXES) {
            String withSuffix = "$.mapBin1.a.%s() == 200".formatted(name);
            Expression actual = Exp.build(parseFilterExp(ExpressionContext.of(withSuffix)));
            assertEquals(baseline, actual, "expected suffix %s() to be ignored for compilation".formatted(name));
        }
    }

    @Test
    void listPathWithMutationStyleSuffixCompilesSameAsPlainPath() {
        Expression baseline = Exp.build(parseFilterExp(ExpressionContext.of("$.listBin1.[0] == 100")));
        for (String name : NO_ARG_MUTATION_STYLE_SUFFIXES) {
            String withSuffix = "$.listBin1.[0].%s() == 100".formatted(name);
            Expression actual = Exp.build(parseFilterExp(ExpressionContext.of(withSuffix)));
            assertEquals(baseline, actual, "expected suffix %s() to be ignored for compilation".formatted(name));
        }
    }
}
