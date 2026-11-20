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

import java.io.File;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.client.sdk.AddTest;
import com.aerospike.client.sdk.AelErrorDetailVerbosityTest;
import com.aerospike.client.sdk.AelMaterializerWhereTest;
import com.aerospike.client.sdk.AppendTest;
import com.aerospike.client.sdk.Args;
import com.aerospike.client.sdk.BackgroundIndexFilterTest;
import com.aerospike.client.sdk.BackgroundTaskTest;
import com.aerospike.client.sdk.BatchTest;
import com.aerospike.client.sdk.BitExpTest;
import com.aerospike.client.sdk.CdtExpTest;
import com.aerospike.client.sdk.CdtMapKeyValueReadOrderTest;
import com.aerospike.client.sdk.CdtOperateComplexTest;
import com.aerospike.client.sdk.CdtOperateTest;
import com.aerospike.client.sdk.CdtPathIntegrationTest;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.ConnectionPoolSettingsIntegrationTest;
import com.aerospike.client.sdk.DeleteBinTest;
import com.aerospike.client.sdk.DurableDeleteTests;
import com.aerospike.client.sdk.ErrorDetailVerbosityTest;
import com.aerospike.client.sdk.ErrorHandlingSpecTest;
import com.aerospike.client.sdk.ExecuteCompletableFutureTest;
import com.aerospike.client.sdk.ExpOperationTest;
import com.aerospike.client.sdk.ExpireTest;
import com.aerospike.client.sdk.FilterExpTest;
import com.aerospike.client.sdk.GenerationTest;
import com.aerospike.client.sdk.HLLExpTest;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.KeyBusyIntegrationTest;
import com.aerospike.client.sdk.ListExpTest;
import com.aerospike.client.sdk.ListMapTest;
import com.aerospike.client.sdk.MapExpTest;
import com.aerospike.client.sdk.NavigatableRecordStreamSortTest;
import com.aerospike.client.sdk.OpTypeTest;
import com.aerospike.client.sdk.OperateBitTest;
import com.aerospike.client.sdk.OperateHllTest;
import com.aerospike.client.sdk.OperateListTest;
import com.aerospike.client.sdk.OperateMapTest;
import com.aerospike.client.sdk.OperateStringTest;
import com.aerospike.client.sdk.OperateTest;
import com.aerospike.client.sdk.PutGetTest;
import com.aerospike.client.sdk.QueryWithPartitionPaginationTest;
import com.aerospike.client.sdk.ReadOperationsTest;
import com.aerospike.client.sdk.RecordStreamAdapterTest;
import com.aerospike.client.sdk.ReplaceTest;
import com.aerospike.client.sdk.ServerInfoTest;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.SessionExtensionTest;
import com.aerospike.client.sdk.SingleNodeBatchReadIntegrationTest;
import com.aerospike.client.sdk.SystemSettings;
import com.aerospike.client.sdk.TopKIntegrationTest;
import com.aerospike.client.sdk.TouchTest;
import com.aerospike.client.sdk.TxnCommitStatusTest;
import com.aerospike.client.sdk.TxnTest;
import com.aerospike.client.sdk.TypedQueryMappingTest;
import com.aerospike.client.sdk.UdfTest;
import com.aerospike.client.sdk.VectorExpIntegrationTest;
import com.aerospike.client.sdk.VectorIntegrationTest;
import com.aerospike.client.sdk.VectorLimitsProbeTest;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.query.AelBitHllTest;
import com.aerospike.client.sdk.query.AelLiteralTest;
import com.aerospike.client.sdk.query.AelMetadataTest;
import com.aerospike.client.sdk.query.AelPathReadTest;
import com.aerospike.client.sdk.query.AelPathSelectorTest;
import com.aerospike.client.sdk.query.AelPathSubExprTest;
import com.aerospike.client.sdk.query.AelPathWriteTest;
import com.aerospike.client.sdk.query.AelStringTest;
import com.aerospike.client.sdk.query.AelWildcardTest;
import com.aerospike.client.sdk.query.ExpSecondaryIndexTest;
import com.aerospike.client.sdk.query.FilterIndexRangeIntegrationTest;
import com.aerospike.client.sdk.query.QueryBlobTest;
import com.aerospike.client.sdk.query.QueryBuilderBinBuilderTest;
import com.aerospike.client.sdk.query.QueryBuilderExecutePathTest;
import com.aerospike.client.sdk.query.QueryBuilderValidationTest;
import com.aerospike.client.sdk.query.QueryCollectionTest;
import com.aerospike.client.sdk.query.QueryContextTest;
import com.aerospike.client.sdk.query.QueryExecuteTest;
import com.aerospike.client.sdk.query.QueryFilterExpTest;
import com.aerospike.client.sdk.query.QueryFilterSetTest;
import com.aerospike.client.sdk.query.QueryGeoTest;
import com.aerospike.client.sdk.query.QueryHintBuilderTest;
import com.aerospike.client.sdk.query.QueryExpressionCollectionIndexFilterTest;
import com.aerospike.client.sdk.query.QueryInTransactionVisibilityTest;
import com.aerospike.client.sdk.query.QueryIndexTest;
import com.aerospike.client.sdk.query.QueryIntegerTest;
import com.aerospike.client.sdk.query.QueryKeyTest;
import com.aerospike.client.sdk.query.QueryOperationsTest;
import com.aerospike.client.sdk.query.QueryPlannerCollectionCdtTest;
import com.aerospike.client.sdk.query.QueryRPSTest;
import com.aerospike.client.sdk.query.QuerySelectionErrorDetailTest;
import com.aerospike.client.sdk.query.QuerySelectionExplainScopeTest;
import com.aerospike.client.sdk.query.QuerySelectionHintExecuteTest;
import com.aerospike.client.sdk.query.QuerySelectionHintFlagsTest;
import com.aerospike.client.sdk.query.QuerySelectionIntegrationTest;
import com.aerospike.client.sdk.query.QuerySelectionLifecycleTest;
import com.aerospike.client.sdk.query.QuerySelectionOperationalIntegrationTest;
import com.aerospike.client.sdk.query.QueryStringTest;
import com.aerospike.client.sdk.query.QueryTopKValidationTest;
import com.aerospike.client.sdk.query.QueryUpsertFromChainedTest;

@Suite
@SelectClasses({
    // Base
    AddTest.class,
    AppendTest.class,
    BackgroundIndexFilterTest.class,
    BackgroundTaskTest.class,
    BatchTest.class,
    BitExpTest.class,
    CdtPathIntegrationTest.class,
    CdtExpTest.class,
    CdtMapKeyValueReadOrderTest.class,
    CdtOperateComplexTest.class,
    CdtOperateTest.class,
    ConnectionPoolSettingsIntegrationTest.class,
    DeleteBinTest.class,
    DurableDeleteTests.class,
    ErrorDetailVerbosityTest.class,
    ExecuteCompletableFutureTest.class,
    AelBitHllTest.class,
    AelErrorDetailVerbosityTest.class,
    AelLiteralTest.class,
    AelMaterializerWhereTest.class,
    AelMetadataTest.class,
    AelPathReadTest.class,
    AelPathSelectorTest.class,
    AelPathSubExprTest.class,
    AelPathWriteTest.class,
    AelStringTest.class,
    AelWildcardTest.class,
    ErrorHandlingSpecTest.class,
    ExpireTest.class,
    ExpOperationTest.class,
    FilterExpTest.class,
    GenerationTest.class,
    HLLExpTest.class,
    KeyBusyIntegrationTest.class,
    ListExpTest.class,
    ListMapTest.class,
    MapExpTest.class,
    NavigatableRecordStreamSortTest.class,
    OperateBitTest.class,
    OperateHllTest.class,
    OperateListTest.class,
    OperateMapTest.class,
    OperateStringTest.class,
    OperateTest.class,
    OpTypeTest.class,
    PutGetTest.class,
    ReadOperationsTest.class,
    RecordStreamAdapterTest.class,
    ReplaceTest.class,
    ServerInfoTest.class,
    SessionExtensionTest.class,
    SingleNodeBatchReadIntegrationTest.class,
    TouchTest.class,
    TypedQueryMappingTest.class,
    TxnCommitStatusTest.class,
    TxnTest.class,
    UdfTest.class,
    VectorIntegrationTest.class,
    VectorExpIntegrationTest.class,
    VectorLimitsProbeTest.class,
    TopKIntegrationTest.class,
    // Query
    ExpSecondaryIndexTest.class,
    FilterIndexRangeIntegrationTest.class,
    QueryBlobTest.class,
    QueryCollectionTest.class,
    QueryContextTest.class,
    QueryExecuteTest.class,
    QueryExpressionCollectionIndexFilterTest.class,
    QueryFilterExpTest.class,
    QueryFilterSetTest.class,
    QueryGeoTest.class,
    QueryHintBuilderTest.class,
    QueryInTransactionVisibilityTest.class,
    QueryIndexTest.class,
    QueryIntegerTest.class,
    QueryKeyTest.class,
    QueryOperationsTest.class,
    QueryBuilderBinBuilderTest.class,
    QueryBuilderExecutePathTest.class,
    QueryBuilderValidationTest.class,
    QueryPlannerCollectionCdtTest.class,
    QueryRPSTest.class,
    QuerySelectionErrorDetailTest.class,
    QuerySelectionExplainScopeTest.class,
    QuerySelectionHintExecuteTest.class,
    QuerySelectionHintFlagsTest.class,
    QuerySelectionIntegrationTest.class,
    QuerySelectionLifecycleTest.class,
    QuerySelectionOperationalIntegrationTest.class,
    QueryStringTest.class,
    QueryTopKValidationTest.class,
    QueryUpsertFromChainedTest.class,
    QueryWithPartitionPaginationTest.class
})
public class SuiteCluster {
    @BeforeSuite
    public static void beforeSuite() {
        System.out.println("Begin AerospikeClient");

        Args args = Args.Instance;

        Host[] hosts = Host.parseHosts(args.host, args.port);

        ClusterDefinition def = new ClusterDefinition(hosts)
            .clusterName(args.clusterName)
            .withSystemSettings(SystemSettings.builder()
                    .connections(ops -> ops.maximumConnectionsPerNode(200)).build()
                    .mergeWith(SystemSettings.DEFAULT));

        // Handle authenticated requests if provided
        if (args.user != null && args.password != null) {
            switch (args.authMode) {
                case INTERNAL:
                    def.withNativeCredentials(args.user, args.password);
                    break;
                case EXTERNAL:
                    def.withExternalCredentials(args.user, args.password);
                    break;
                case EXTERNAL_INSECURE:
                    def.withExternalInsecureCredentials(args.user, args.password);
                    break;
                default:
                    break;
            }
        }

        if (args.tlsName != null) {
            String certHome = System.getenv("CERT_HOME");

            if (certHome == null) {
                certHome = "";
            }

            String caFile = resolvePath(certHome, args.caFile);
            String clientCertFile = resolvePath(certHome, args.clientCertFile);
            String clientKeyFile = resolvePath(certHome, args.clientKeyFile);

            def.withTlsConfig(tls -> tls
                .tlsName(args.tlsName)
                .caFile(caFile)
                .clientCertFile(clientCertFile)
                .clientKeyFile(clientKeyFile)
            );
        }

        Cluster cluster = def.connect();
        Session session, sessionWithSendKey;

        try {
            session = cluster.createSession(Behavior.DEFAULT);
            sessionWithSendKey = cluster.createSession(Behavior.DEFAULT.deriveWithChanges(
                    "sendKey",
                    opt -> opt.on(Selectors.all(), s -> s.sendKey(true)))
            );
            args.setServerSpecific(cluster);
        }
        catch (RuntimeException re) {
            cluster.close();
            throw re;
        }

        ClusterTest.cluster = cluster;
        ClusterTest.session = session;
        ClusterTest.sessionWithSendKey = sessionWithSendKey;
        ClusterTest.initializedBySuite = true;
        ClusterTest.ensurePartitionMapReady();
    }

    private static String resolvePath(String dir, String path) {
        File file = new File(path);

        if (file.isAbsolute()) {
            return path;
        }

        file = new File(dir, path);
        return file.getAbsolutePath();
    }

    @AfterSuite
    public static void afterSuite() {
        System.out.println("End AerospikeClient");
        if (ClusterTest.cluster != null) {
            ClusterTest.cluster.close();
            ClusterTest.cluster = null;
            ClusterTest.session = null;
            ClusterTest.sessionWithSendKey = null;
        }
        ClusterTest.initializedBySuite = false;
    }
}
