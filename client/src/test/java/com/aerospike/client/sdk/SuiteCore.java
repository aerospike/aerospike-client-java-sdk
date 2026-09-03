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

import com.aerospike.client.sdk.command.IndexProbeCommandTest;
import com.aerospike.client.sdk.policy.AsyncRecordStreamTest;
import com.aerospike.client.sdk.policy.BehaviorFileMonitorRestartTest;
import com.aerospike.client.sdk.policy.BehaviorTest;
import com.aerospike.client.sdk.policy.BehaviorYamlTest;
import com.aerospike.client.sdk.policy.QueryProducerCancellationTest;
import com.aerospike.client.sdk.policy.SystemBehaviorTest;
import com.aerospike.client.sdk.query.AelPlaceholderBinderTest;
import com.aerospike.client.sdk.query.FilterOverrideTest;
import com.aerospike.client.sdk.query.FilterWireRangeTest;
import com.aerospike.client.sdk.query.IndexProbePlannerRoutingTest;
import com.aerospike.client.sdk.query.QueryHintTest;
import com.aerospike.client.sdk.query.SortPropertiesValidationTest;
import com.aerospike.client.sdk.query.plan.IndexRangeWireTest;
import com.aerospike.client.sdk.query.plan.QueryPlanTest;
import com.aerospike.client.sdk.query.plan.QueryWhereWireTest;

@Suite
@SelectClasses({
    AelPlaceholderBinderTest.class,
    AsyncRecordStreamTest.class,
    BehaviorFileMonitorRestartTest.class,
    BehaviorTest.class,
    BehaviorYamlTest.class,
    CdtPathExpressionFluentTest.class,
    CdtPathOperationTest.class,
    BinBuilderOptionsTest.class,
    BinBuilderValueTest.class,
    CdtGetOrRemoveBuilderDispatchTest.class,
    CdtGetOrRemoveBuilderPathTest.class,
    CdtGetOrRemoveBuilderWriteTest.class,
    CdtReadOnlyBuilderPathTest.class,
    CdtReadOnlyBuilderSelectorTest.class,
    CdtReadOnlyBuilderTerminalTest.class,
    CdtSelectorParityTest.class,
    CtxSerdeTest.class,
    FilterOverrideTest.class,
    FilterWireRangeTest.class,
    IndexProbeCommandTest.class,
    IndexProbePlannerRoutingTest.class,
    IndexRangeWireTest.class,
    QueryHintTest.class,
    QueryPlanExecuteWireTest.class,
    QueryPlanTest.class,
    QueryProducerCancellationTest.class,
    QueryWhereWireTest.class,
    SortPropertiesValidationTest.class,
    StringApiPackagingTest.class,
    SystemBehaviorTest.class,
})
public class SuiteCore {
}
