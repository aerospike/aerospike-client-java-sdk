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

import com.aerospike.dsl.expression.*;
import com.aerospike.dsl.index.IndexContextTests;
import com.aerospike.dsl.index.IndexTests;
import com.aerospike.dsl.parts.operand.OperandFactoryTests;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.dsl.ctx.CtxTests;
import com.aerospike.dsl.filter.ArithmeticFiltersTests;
import com.aerospike.dsl.filter.BinFiltersTests;
import com.aerospike.dsl.filter.ExplicitTypesFiltersTests;
import com.aerospike.dsl.filter.ImplicitTypesFiltersTests;
import com.aerospike.dsl.parsedExpression.LogicalParsedExpressionTests;
import com.aerospike.dsl.parsedExpression.PlaceholdersTests;

@Suite
@SelectClasses({
	// com.aerospike.dsl.ctx
	CtxTests.class,
	// com.aerospike.dsl.expression
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
	// com.aerospike.dsl.filter
	ArithmeticFiltersTests.class,
	BinFiltersTests.class,
	ExplicitTypesFiltersTests.class,
	ImplicitTypesFiltersTests.class,
	ListExpressionsTests.class,
	// com.aerospike.dsl.parsedExpression
	LogicalParsedExpressionTests.class,
	PlaceholdersTests.class,
    // com.aerospike.dsl.index
    IndexContextTests.class,
    IndexTests.class,
    // com.aerospike.dsl.parts.operand
    OperandFactoryTests.class
})
public class SuiteDSL {
}
