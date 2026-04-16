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
package com.aerospike.ael.filter;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.IndexType;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.client.sdk.query.IndexCollectionType.LIST;

import java.util.List;

class ListExpressionsTests {

    String NAMESPACE = "test1";
    List<Index> INDEXES = List.of(
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.INTEGER).binValuesRatio(0).build(),
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.STRING)
                    .binValuesRatio(0).indexCollectionType(LIST).build(),
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.STRING)
                    .binValuesRatio(0).indexCollectionType(LIST).ctx(new CTX[]{CTX.listIndex(5)}).build(),
            Index.builder().namespace(NAMESPACE).bin("listBin1").indexType(IndexType.STRING)
                    .binValuesRatio(0).indexCollectionType(LIST).ctx(new CTX[]{CTX.listValue(Value.get(5))}).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void listExpression() {
        Filter expected = Filter.equal("listBin1", "stringVal");
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1 == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.get(type: STRING) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.listBin1.get(type: STRING, return: VALUE) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected
        );
    }

    @Test
    void listExpressionNested_oneLevel() {
        Filter expected = Filter.equal("listBin1", "stringVal", CTX.listIndex(5));
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5] == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5].get(type: STRING) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.listBin1.[5].get(type: STRING, return: VALUE) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected
        );
    }

    @Test
    void listExpressionNested_twoLevels() {
        Filter expected = Filter.equal("listBin1", "stringVal", CTX.listIndex(5), CTX.listIndex(1));
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5].[1] == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[5].[1].get(type: STRING) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected);
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.listBin1.[5].[1].get(type: STRING, return: VALUE) == \"stringVal\""),
                INDEX_FILTER_INPUT, expected
        );

        Filter expected2 = Filter.equal("listBin1", "stringVal", CTX.listValue(Value.get(5)), CTX.listRank(10));
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.listBin1.[=5].[#10] == \"stringVal\""),
                INDEX_FILTER_INPUT, expected2);
    }
}
