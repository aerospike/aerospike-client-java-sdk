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

import java.io.File;

import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.query.AelMetadataTest;
import com.aerospike.client.sdk.query.AelPathReadTest;
import com.aerospike.client.sdk.query.AelPathSubExprTest;
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
import com.aerospike.client.sdk.query.QueryUpsertFromChainedTest;

@Suite
@SelectClasses({
    // Base
    AddTest.class,
    AppendTest.class,
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
    AelErrorDetailVerbosityTest.class,
    AelMaterializerWhereTest.class,
    AelMetadataTest.class,
    AelPathReadTest.class,
    AelPathSubExprTest.class,
    AelWildcardTest.class,
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
    TouchTest.class,
    TypedQueryMappingTest.class,
    TxnTest.class,
    UdfTest.class,
    // Query
    ExpSecondaryIndexTest.class,
    FilterIndexRangeIntegrationTest.class,
    QueryBlobTest.class,
    QueryCollectionTest.class,
    QueryContextTest.class,
    QueryExecuteTest.class,
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
