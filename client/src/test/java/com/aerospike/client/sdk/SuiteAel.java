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

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.ael.ctx.CtxTests;
import com.aerospike.ael.expression.ArithmeticExpressionsTests;
import com.aerospike.ael.expression.BareGetFunctionTests;
import com.aerospike.ael.expression.BinExpressionsTests;
import com.aerospike.ael.expression.BlobTests;
import com.aerospike.ael.expression.CastingTests;
import com.aerospike.ael.expression.ControlStructuresTests;
import com.aerospike.ael.expression.ExplicitTypesTests;
import com.aerospike.ael.expression.ImplicitTypesTests;
import com.aerospike.ael.expression.InBinTests;
import com.aerospike.ael.expression.InCompositeTests;
import com.aerospike.ael.expression.InExplicitTypeTests;
import com.aerospike.ael.expression.InGrammarConflictTests;
import com.aerospike.ael.expression.InLiteralTests;
import com.aerospike.ael.expression.InNegativeTests;
import com.aerospike.ael.expression.InPlaceholderTests;
import com.aerospike.ael.expression.ListExpressionsTests;
import com.aerospike.ael.expression.LogicalExpressionsTests;
import com.aerospike.ael.expression.MapAndListExpressionsTests;
import com.aerospike.ael.expression.MapExpressionsTests;
import com.aerospike.ael.expression.RecordMetadataTests;
import com.aerospike.ael.expression.SyntaxErrorTests;
import com.aerospike.ael.filter.ArithmeticFiltersTests;
import com.aerospike.ael.filter.BinFiltersTests;
import com.aerospike.ael.filter.ExplicitTypesFiltersTests;
import com.aerospike.ael.filter.ImplicitTypesFiltersTests;
import com.aerospike.ael.index.IndexContextTests;
import com.aerospike.ael.index.IndexTests;
import com.aerospike.ael.parsedExpression.LogicalParsedExpressionTests;
import com.aerospike.ael.parsedExpression.PlaceholdersTests;
import com.aerospike.ael.parts.operand.OperandFactoryTests;

@Suite
@SelectClasses({
    // com.aerospike.ael.ctx
    CtxTests.class,
    // com.aerospike.ael.expression
    ArithmeticExpressionsTests.class,
    BareGetFunctionTests.class,
    BinExpressionsTests.class,
    BlobTests.class,
    CastingTests.class,
    ControlStructuresTests.class,
    ExplicitTypesTests.class,
    ImplicitTypesTests.class,
    InBinTests.class,
    InCompositeTests.class,
    InExplicitTypeTests.class,
    InGrammarConflictTests.class,
    InLiteralTests.class,
    InNegativeTests.class,
    InPlaceholderTests.class,
    ListExpressionsTests.class,
    LogicalExpressionsTests.class,
    MapAndListExpressionsTests.class,
    MapExpressionsTests.class,
    RecordMetadataTests.class,
    SyntaxErrorTests.class,
    // com.aerospike.ael.filter
    ArithmeticFiltersTests.class,
    BinFiltersTests.class,
    ExplicitTypesFiltersTests.class,
    ImplicitTypesFiltersTests.class,
    ListExpressionsTests.class,
    // com.aerospike.ael.parsedExpression
    LogicalParsedExpressionTests.class,
    PlaceholdersTests.class,
    // com.aerospike.ael.index
    IndexContextTests.class,
    IndexTests.class,
    // com.aerospike.ael.parts.operand
    OperandFactoryTests.class
})
public class SuiteAel {
}
