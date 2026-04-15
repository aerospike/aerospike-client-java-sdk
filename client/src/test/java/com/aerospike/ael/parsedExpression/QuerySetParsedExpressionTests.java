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
package com.aerospike.ael.parsedExpression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.NAMESPACE;
import static com.aerospike.ael.util.TestUtils.parseAelExpressionAndCompare;

/**
 * Parser-level tests for {@link IndexContext#withQuerySet} via {@link com.aerospike.ael.util.TestUtils#parseAelExpressionAndCompare}
 * (same pattern as {@link InExprTests}).
 */
class QuerySetParsedExpressionTests {

    private static final String TEST_SCAN = "testScan";

    @Test
    void inAndEq_withQuerySet_eqBinIndexedOnlyOnOtherSet_noSiFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).setName("customers").bin("intBin1")
                        .indexType(IndexType.INTEGER).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).setName("orders").bin("intBin2")
                        .indexType(IndexType.INTEGER).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withQuerySet(NAMESPACE, "customers", indexes));
    }

    @Test
    void inAndEq_withQuerySet_indexesOnQuerySet_sameAsInExpr() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin1")
                        .indexType(IndexType.INTEGER).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin2")
                        .indexType(IndexType.INTEGER).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withQuerySet(NAMESPACE, TEST_SCAN, indexes));
    }

    @Test
    void tripleGt_withoutQuerySet_otherSetWinsCardinality_withQuerySet_usesQuerySetIndexes() {
        List<Index> catalog = List.of(
                Index.builder().namespace(NAMESPACE).setName("other").bin("intBin1").name("idx_bin1")
                        .indexType(IndexType.INTEGER).binValuesRatio(200).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin2").name("idx_bin2")
                        .indexType(IndexType.INTEGER).binValuesRatio(100).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin3").name("idx_bin3")
                        .indexType(IndexType.INTEGER).binValuesRatio(100).build()
        );
        String ael = "$.intBin1 > 50 and $.intBin2 > 50 and $.intBin3 > 50";
        Exp expWhenSiOnIntBin1 = Exp.and(
                Exp.gt(Exp.intBin("intBin2"), Exp.val(50)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(50)));
        Exp expWhenSiOnIntBin2 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(50)),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(50)));

        parseAelExpressionAndCompare(
                ExpressionContext.of(ael),
                Filter.range("intBin1", 51, Long.MAX_VALUE),
                expWhenSiOnIntBin1,
                IndexContext.of(NAMESPACE, catalog));

        parseAelExpressionAndCompare(
                ExpressionContext.of(ael),
                Filter.range("intBin2", 51, Long.MAX_VALUE),
                expWhenSiOnIntBin2,
                IndexContext.withQuerySet(NAMESPACE, TEST_SCAN, catalog));
    }

    @Test
    void inAndGt_withQuerySet_onMatchingSets() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin1")
                        .indexType(IndexType.INTEGER).binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin2")
                        .indexType(IndexType.INTEGER).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(10, 20)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [10, 20] and $.intBin2 > 100"),
                filter, exp, IndexContext.withQuerySet(NAMESPACE, TEST_SCAN, indexes));
    }

    @Test
    void withQuerySet_andIndexNameHint_selectsBinForNamedIndexOnThatSet() {
        List<Index> catalog = List.of(
                Index.builder().namespace(NAMESPACE).setName("set").bin("age").name("ageidx")
                        .indexType(IndexType.INTEGER).binValuesRatio(50).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("age").name("age_idx")
                        .indexType(IndexType.INTEGER).binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("score").name("score_idx")
                        .indexType(IndexType.INTEGER).binValuesRatio(5).build()
        );
        String ael = "$.age > 18 and $.score > 0 and $.flag == 1";
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("score"), Exp.val(0)),
                Exp.eq(Exp.intBin("flag"), Exp.val(1)));

        parseAelExpressionAndCompare(
                ExpressionContext.of(ael),
                Filter.range("age", 19, Long.MAX_VALUE),
                exp,
                IndexContext.withQuerySet(NAMESPACE, TEST_SCAN, catalog, "age_idx"));
    }

    @Test
    void withBinHint_withQuerySet_prefersBinWhenPresentOnQuerySet() {
        List<Index> catalog = List.of(
                Index.builder().namespace(NAMESPACE).setName("other").bin("intBin1")
                        .indexType(IndexType.INTEGER).binValuesRatio(200).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin2")
                        .indexType(IndexType.INTEGER).binValuesRatio(100).build(),
                Index.builder().namespace(NAMESPACE).setName(TEST_SCAN).bin("intBin3")
                        .indexType(IndexType.INTEGER).binValuesRatio(100).build()
        );
        String ael = "$.intBin1 > 50 and $.intBin2 > 50 and $.intBin3 > 50";
        Exp exp = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(50)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(50)));

        parseAelExpressionAndCompare(
                ExpressionContext.of(ael),
                Filter.range("intBin3", 51, Long.MAX_VALUE),
                exp,
                IndexContext.withBinHint(NAMESPACE, catalog, "intBin3", TEST_SCAN));
    }
}
