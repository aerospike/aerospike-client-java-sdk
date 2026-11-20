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
package com.aerospike.client.sdk.suite;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.client.sdk.BackgroundIndexFilterWireTest;
import com.aerospike.client.sdk.BackgroundOperationBuilderValidationTest;
import com.aerospike.client.sdk.BinBuilderOptionsTest;
import com.aerospike.client.sdk.BinBuilderValueTest;
import com.aerospike.client.sdk.ByteBufferValueTest;
import com.aerospike.client.sdk.CdtGetOrRemoveBuilderDispatchTest;
import com.aerospike.client.sdk.CdtGetOrRemoveBuilderPathTest;
import com.aerospike.client.sdk.CdtGetOrRemoveBuilderWriteTest;
import com.aerospike.client.sdk.CdtPathExpressionFluentTest;
import com.aerospike.client.sdk.CdtPathOperationTest;
import com.aerospike.client.sdk.CdtReadOnlyBuilderPathTest;
import com.aerospike.client.sdk.CdtReadOnlyBuilderSelectorTest;
import com.aerospike.client.sdk.CdtReadOnlyBuilderTerminalTest;
import com.aerospike.client.sdk.CdtSelectorParityTest;
import com.aerospike.client.sdk.CdtVectorFlagTest;
import com.aerospike.client.sdk.ContainerToStringTest;
import com.aerospike.client.sdk.CtxSerdeTest;
import com.aerospike.client.sdk.QueryPlanExecuteWireTest;
import com.aerospike.client.sdk.QueryTopKWireTest;
import com.aerospike.client.sdk.StringApiPackagingTest;
import com.aerospike.client.sdk.VectorWriteGuardWireTest;
import com.aerospike.client.sdk.WriteKindTest;
import com.aerospike.client.sdk.metrics.HistogramTest;
import com.aerospike.client.sdk.query.AelPlaceholderBinderTest;
import com.aerospike.client.sdk.query.FilterOverrideTest;
import com.aerospike.client.sdk.query.FilterWireRangeTest;
import com.aerospike.client.sdk.query.IndexProbePlannerRoutingTest;
import com.aerospike.client.sdk.query.IndexProbePlannerWhereFlagsTest;
import com.aerospike.client.sdk.query.QueryIndexFilterWireTest;
import com.aerospike.client.sdk.query.QueryHintTest;
import com.aerospike.client.sdk.query.QueryTopKValidationTest;
import com.aerospike.client.sdk.query.SortPropertiesValidationTest;
import com.aerospike.client.sdk.query.TopKMergeTest;
import com.aerospike.client.sdk.query.TopKMergingRecordStreamTest;
import com.aerospike.client.sdk.tend.PartitionParserFullOwnerTest;
import com.aerospike.client.sdk.vector.VectorTest;
import com.aerospike.client.sdk.query.plan.IndexRangeWireTest;
import com.aerospike.client.sdk.query.plan.QueryPlanTest;
import com.aerospike.client.sdk.query.plan.QueryWhereWireTest;
import com.aerospike.client.sdk.util.PackerTest;

@Suite
@SelectClasses({
    AelPlaceholderBinderTest.class,
    BinBuilderOptionsTest.class,
    BinBuilderValueTest.class,
    BackgroundIndexFilterWireTest.class,
    BackgroundOperationBuilderValidationTest.class,
    ByteBufferValueTest.class,
    CdtGetOrRemoveBuilderDispatchTest.class,
    CdtGetOrRemoveBuilderPathTest.class,
    CdtGetOrRemoveBuilderWriteTest.class,
    CdtReadOnlyBuilderPathTest.class,
    CdtReadOnlyBuilderSelectorTest.class,
    CdtReadOnlyBuilderTerminalTest.class,
    CdtPathExpressionFluentTest.class,
    CdtPathOperationTest.class,
    CdtSelectorParityTest.class,
    CdtVectorFlagTest.class,
    ContainerToStringTest.class,
    CtxSerdeTest.class,
    FilterOverrideTest.class,
    FilterWireRangeTest.class,
    HistogramTest.class,
    IndexProbePlannerRoutingTest.class,
    IndexProbePlannerWhereFlagsTest.class,
    IndexRangeWireTest.class,
    PackerTest.class,
    PartitionParserFullOwnerTest.class,
    QueryIndexFilterWireTest.class,
    QueryHintTest.class,
    QueryPlanExecuteWireTest.class,
    QueryPlanTest.class,
    QueryTopKValidationTest.class,
    QueryTopKWireTest.class,
    QueryWhereWireTest.class,
    SortPropertiesValidationTest.class,
    StringApiPackagingTest.class,
    TopKMergeTest.class,
    TopKMergingRecordStreamTest.class,
    WriteKindTest.class,
    VectorTest.class,
    VectorWriteGuardWireTest.class,
})
public class SuiteCore {
}
