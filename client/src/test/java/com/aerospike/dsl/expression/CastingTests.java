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
package com.aerospike.dsl.expression;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void floatToIntComparison() {
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.intBin("floatBin1"));
        // Int is default
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > $.floatBin1.asInt()"), expectedExp);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > $.floatBin1.asInt()"), expectedExp);
    }

    @Test
    void intToFloatComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > $.intBin2.asFloat()"),
                Exp.gt(Exp.intBin("intBin1"), Exp.floatBin("intBin2")));
    }

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()")))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }
}
